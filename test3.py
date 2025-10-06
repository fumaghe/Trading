#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crypto Screener (v2.3) — 3 criteri + funnel, cache-aware & robust, PERPS-FIRST

Criteri attivi:
  1) CEX spot principale per volume = Bitget (tickers CoinGecko)
  2) Perpetual su ENTRAMBI: Binance Futures (USDT-M) e Bybit (linear)
  3) Ha contratto su BSC e il main LP (maggiore liquidità) è su PancakeSwap
     (opzione --require-v3 per limitare solo a Pancake v3)

Novità v2.3:
- Seed “perps-first”: candidati = intersezione dei base asset di Binance&Bybit.
- Mappatura symbol→CoinGecko id con /coins/list?include_platform=true (cache).
- Log migliorati (Rich) con riepiloghi, tabelle e progressi.
- Filtri qualità tickers CG (stale/anomaly e trust≠green/yellow).
- Stato persistente thread-safe (lock) per evitare write races.

Requisiti: requests, tenacity, rich
  pip install requests tenacity rich

python test3.py --seed-from perps --workers 12 --min-liq 200000 --max-tickers-scan 40 --dominance 0.30 --skip-unchanged-days 0 --rps-cg 0.5 --rps-ds 2.0 --funnel-show 100

"""
from __future__ import annotations
import argparse
import json
import os
import re
import sys
import time
import threading
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
except Exception:  # pragma: no cover
    def retry(*args, **kwargs):
        def deco(fn): return fn
        return deco
    def stop_after_attempt(n): return None
    def wait_exponential(**kwargs): return None
    def retry_if_exception_type(*args, **kwargs): return None

try:
    from rich.console import Console
    from rich.table import Table
    from rich.progress import Progress, BarColumn, TimeElapsedColumn
    from rich.panel import Panel
    from rich.rule import Rule
    from rich import box
    RICH = True
    console = Console()
except Exception:
    RICH = False
    console = None

# --------------------------- Endpoints --------------------------- #

COINGECKO = "https://api.coingecko.com/api/v3"
DEXSCREENER_TOKEN = "https://api.dexscreener.com/latest/dex/tokens/{address}"
BINANCE_FUTURES_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BYBIT_LINEAR = "https://api.bybit.com/v5/market/instruments-info"

# --------------------------- Defaults --------------------------- #

DEFAULT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    ),
}

# --------------------------- Rate limiter --------------------------- #

class RateLimiter:
    """Semplice rate limiter per-sorgente con piccolo jitter per ridurre burst/429."""
    def __init__(self, rps: float):
        self.min_interval = 1.0 / max(0.0001, rps)
        self.lock = threading.Lock()
        self.last = 0.0

    def wait(self):
        with self.lock:
            now = time.time()
            delta = now - self.last
            if delta < self.min_interval:
                time.sleep(self.min_interval - delta)
            self.last = time.time()
        time.sleep(random.uniform(0.05, 0.20))  # desincronizza leggermente

# --------------------------- Settings --------------------------- #

@dataclass
class Settings:
    pages: int = 1
    per_page: int = 250
    ids: Optional[List[str]] = None
    workers: int = 6

    # Stage thresholds
    min_pair_liquidity_usd: float = 150_000.0
    require_v3: bool = False  # opzionale: solo Pancake v3

    # Heuristica Bitget
    max_tickers_scan: int = 15
    bitget_dominance: float = 0.60  # 0..1

    # TTLs
    ttl_cg_coin_sec: int = 48 * 3600
    ttl_perps_sec: int = 7 * 24 * 3600
    ttl_dexscreener_sec: int = 24 * 3600

    # Skip coins stabili per N giorni
    skip_unchanged_days: int = 3

    # Paths
    cache_root: str = ".cache"
    state_root: str = "state"

    # Verbose & safety
    verbose: bool = True
    quiet: bool = False

    # Rate limits (rps)
    rps_cg: float = 1.0      # CoinGecko
    rps_ds: float = 2.0      # DexScreener

    # Funnel printing
    funnel_show: int = 30

    # Seed mode: "perps" (default) o "cg"
    seed_from: str = "perps"

# --------------------------- Utilities & logging --------------------------- #

def now_ts() -> int:
    return int(time.time())

def ensure_dir(p: str):
    Path(p).mkdir(parents=True, exist_ok=True)

def joinp(*a) -> str:
    return os.path.join(*a)

def log_info(msg: str):
    if settings.quiet:
        return
    if RICH:
        console.log(msg)
    else:
        print(msg)

def read_json(path: str) -> Optional[dict]:
    p = Path(path)
    if not p.exists(): return None
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def write_json(path: str, data: dict):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)

def cache_get(path: str, ttl_sec: int) -> Optional[dict]:
    p = Path(path)
    if not p.exists(): return None
    try:
        age = time.time() - p.stat().st_mtime
        if age > ttl_sec: return None
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

# --------------------------- HTTP --------------------------- #

class Http:
    sess = requests.Session()
    sess.headers.update(DEFAULT_HEADERS)

    @staticmethod
    @retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=1, max=8),
           retry=retry_if_exception_type((requests.RequestException,)))
    def get(url: str, **kwargs) -> requests.Response:
        headers = kwargs.pop("headers", {})
        all_headers = {**DEFAULT_HEADERS, **headers}
        r = Http.sess.get(url, timeout=25, headers=all_headers, **kwargs)
        if r.status_code in (429, 418):
            wait_s = int(r.headers.get("Retry-After", "5"))
            time.sleep(max(wait_s, 5))
            raise requests.RequestException(f"{r.status_code} Too Many Requests")
        if r.status_code in (403, 401):
            time.sleep(5)
            raise requests.RequestException(f"{r.status_code} Forbidden/Unauthorized")
        if r.status_code >= 400:
            raise requests.RequestException(f"GET {url} -> {r.status_code} {r.text[:200]}")
        return r

# --------------------------- Providers: Perps --------------------------- #

def load_binance_perp_bases_cached() -> Dict[str, Any]:
    cache_path = joinp(settings.cache_root, "perps_binance.json")
    j = cache_get(cache_path, settings.ttl_perps_sec)
    if j is not None:
        return j
    data = Http.get(BINANCE_FUTURES_INFO).json()
    res = {}
    for s in data.get("symbols", []):
        if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING":
            res[s["baseAsset"].upper()] = s
    write_json(cache_path, res)
    return res

def load_bybit_perp_bases_cached() -> Dict[str, Any]:
    cache_path = joinp(settings.cache_root, "perps_bybit.json")
    j = cache_get(cache_path, settings.ttl_perps_sec)
    if j is not None:
        return j
    res: Dict[str, Any] = {}
    cursor = None
    while True:
        params = {"category": "linear", "limit": 1000}
        if cursor: params["cursor"] = cursor
        data = Http.get(BYBIT_LINEAR, params=params).json()
        lst = (data.get("result") or {}).get("list") or []
        for s in lst:
            if s.get("status") == "Trading" and s.get("contractType") in ("LinearPerpetual", "LinearFutures"):
                symbol = s.get("symbol", "")
                base = re.sub(r"(USDT|USDC)$", "", symbol).upper()
                if base: res[base] = s
        cursor = (data.get("result") or {}).get("nextPageCursor")
        if not cursor: break
    write_json(cache_path, res)
    return res

def base_symbol_candidates(base: str) -> List[str]:
    base = base.upper()
    cands = {base}
    for k in ("10", "100", "1000", "10000"):
        cands.add(f"{k}{base}")
    m = re.match(r"^(10|100|1000|10000)([A-Z0-9]+)$", base)
    if m: cands.add(m.group(2))
    return list(cands)

def has_perps_on_both(binance_bases: Dict[str, Any], bybit_bases: Dict[str, Any], symbol: str) -> bool:
    cands = base_symbol_candidates(symbol)
    return any(c in binance_bases for c in cands) and any(c in bybit_bases for c in cands)

# --------------------------- CoinGecko --------------------------- #

def cg_coin_cache_path(cid: str) -> str:
    return joinp(settings.cache_root, "coingecko", f"{cid}.json")

def fetch_coin_full(cid: str, cg_rl: RateLimiter) -> Dict[str, Any]:
    cache_path = cg_coin_cache_path(cid)
    j = cache_get(cache_path, settings.ttl_cg_coin_sec)
    if j is not None: return j
    url = f"{COINGECKO}/coins/{cid}?localization=false&tickers=true&market_data=false&community_data=false&developer_data=false&sparkline=false"
    cg_rl.wait()
    data = Http.get(url).json()
    write_json(cache_path, data)
    return data

def bsc_contract_address(coin: Dict[str, Any]) -> Optional[str]:
    platforms = coin.get("platforms") or {}
    for key in platforms.keys():
        if key and key.lower() in ("binance-smart-chain", "bsc", "bnb-smart-chain", "bnb"):
            addr = platforms[key]
            if addr: return addr.lower()
    return None

def top_spot_cex_bitget_ok(coin: Dict[str, Any], dominance: float, max_scan: int) -> Tuple[bool, Optional[str]]:
    """Ritorna (is_bitget_top, top_name) con short-circuit euristico + filtri qualità."""
    tickers = coin.get("tickers", []) or []
    best_name, best_vol = None, -1.0
    seen = 0
    sum_vol = 0.0
    bitget_vol = 0.0
    dex_like = {"uniswap", "pancakeswap", "sushiswap", "curve", "quickswap", "raydium", "balancer"}
    for t in tickers:
        if t.get("is_anomaly") or t.get("is_stale"):
            continue
        trust = (t.get("trust_score") or "").lower()
        if trust and trust not in ("green", "yellow"):
            continue

        market = (t.get("market") or {})
        name = (market.get("name") or "").strip()
        ident = (market.get("identifier") or "").lower()
        if any(d in ident for d in dex_like):
            continue

        conv = t.get("converted_volume") or {}
        vol = None
        for k in ("usd", "eur", "btc"):
            if conv.get(k) is not None:
                vol = float(conv[k]); break
        if vol is None: vol = 0.0
        seen += 1
        sum_vol += vol
        if vol > best_vol:
            best_vol, best_name = vol, name
        if name.lower().startswith("bitget"):
            bitget_vol += vol

        if seen >= max_scan and sum_vol > 0:
            if (bitget_vol / sum_vol) >= dominance:
                return True, "Bitget"
            break
    return (best_name or "").lower().startswith("bitget"), best_name

# /coins/list?include_platform=true  --> per mappare SYMBOL -> id preferendo chi ha BSC
def load_cg_coins_list_cached(cg_rl: RateLimiter) -> List[Dict[str, Any]]:
    cache_path = joinp(settings.cache_root, "coingecko", "coins_list_include_platform.json")
    j = cache_get(cache_path, settings.ttl_cg_coin_sec)
    if j is not None:
        return j
    url = f"{COINGECKO}/coins/list?include_platform=true"
    cg_rl.wait()
    data = Http.get(url).json()
    write_json(cache_path, data)
    return data

def choose_best_cg_id_for_symbol(symbol: str, cg_list: List[Dict[str, Any]]) -> Optional[Tuple[str, str]]:
    """
    Ritorna (id, name) per il symbol dato.
    Heuristica:
      1) match case-insensitive su symbol; se più entry:
         - preferisci quelle con 'binance-smart-chain' nei platforms
         - altrimenti prendi la prima occorrenza stabile
    """
    sym_up = symbol.upper()
    candidates = []
    for row in cg_list:
        sym = (row.get("symbol") or "").upper()
        if sym != sym_up: 
            continue
        name = row.get("name") or ""
        platforms = (row.get("platforms") or {})  # se include_platform=true
        has_bsc = any(k.lower() in ("binance-smart-chain", "bsc", "bnb", "bnb-smart-chain") for k in platforms.keys())
        candidates.append((row.get("id"), name, has_bsc))
    if not candidates:
        return None
    # preferisci chi ha BSC
    for cid, name, has_bsc in candidates:
        if has_bsc:
            return cid, name
    # fallback: prima entry
    cid, name, _ = candidates[0]
    return cid, name

# --------------------------- DexScreener --------------------------- #

def ds_cache_path(addr: str) -> str:
    return joinp(settings.cache_root, "dexscreener", f"{addr.lower()}.json")

def main_bsc_pair_on_pancake(addr: str, ds_rl: RateLimiter) -> Optional[Dict[str, Any]]:
    path = ds_cache_path(addr)
    j = cache_get(path, settings.ttl_dexscreener_sec)
    if j is not None: data = j
    else:
        ds_rl.wait()
        url = DEXSCREENER_TOKEN.format(address=addr)
        data = Http.get(url).json()
        write_json(path, data)
    pairs = data.get("pairs") or []
    best, best_liq = None, -1.0
    for p in pairs:
        if (p.get("chainId") or "").lower() != "bsc":
            continue
        liq = float(((p.get("liquidity") or {}).get("usd") or 0))
        if liq < settings.min_pair_liquidity_usd:
            continue
        if liq > best_liq:
            best_liq, best = liq, p
    if not best: return None
    dexid = (best.get("dexId") or "").lower()
    if "pancake" not in dexid:
        return None
    return best

# --------------------------- State index --------------------------- #

def state_path() -> str:
    return joinp(settings.state_root, "index.json")

def load_state() -> Dict[str, Any]:
    j = read_json(state_path()) or {}
    return j

def save_state(state: Dict[str, Any]):
    write_json(state_path(), state)

def stale_by_days(ts: Optional[int], days: int) -> bool:
    if not ts: return True
    return (now_ts() - ts) > days * 86400

# --------------------------- Screening --------------------------- #

@dataclass
class CheckResult:
    id: str
    symbol: str
    name: str
    reasons_ok: List[str]
    reasons_ko: List[str]
    extra: Dict[str, Any]
    def ok(self) -> bool: return len(self.reasons_ko) == 0
    def score(self) -> int: return len(self.reasons_ok)

@dataclass
class FunnelRow:
    id: str
    symbol: str
    name: str
    skipped: bool
    reason: Optional[str]
    s1_perps: bool
    s2_bitget: bool
    s3_bsc_pancake: bool
    top_spot_cex: Optional[str] = None
    lp_pair: Optional[Dict[str, Any]] = None

# lock globale per lo stato (thread-safe)
state_lock = threading.Lock()

def screen_coin(cid: str, sym_hint: str,
                binance_bases: Dict[str, Any],
                bybit_bases: Dict[str, Any],
                cg_rl: RateLimiter, ds_rl: RateLimiter,
                state: Dict[str, Any]) -> Tuple[FunnelRow, Optional[CheckResult]]:
    # ---- Pre-skip da indice locale ----
    with state_lock:
        st = dict(state.get(cid, {}))
    last_checked = st.get("last_checked")
    if not stale_by_days(last_checked, settings.skip_unchanged_days):
        fr = FunnelRow(id=cid, symbol=(sym_hint or "").upper(), name=cid,
                       skipped=True, reason=f"recent (<= {settings.skip_unchanged_days}d)",
                       s1_perps=False, s2_bitget=False, s3_bsc_pancake=False)
        return fr, None

    fr = FunnelRow(id=cid, symbol=(sym_hint or "").upper(), name=cid,
                   skipped=False, reason=None,
                   s1_perps=False, s2_bitget=False, s3_bsc_pancake=False)

    # ---- Stage 1: Perps su entrambi (by construction per seed=perps, ma ricontrollo) ----
    if not has_perps_on_both(binance_bases, bybit_bases, fr.symbol):
        with state_lock:
            state[cid] = {**st, "last_checked": now_ts(), "status": "no_perps"}
        return fr, None
    fr.s1_perps = True

    # ---- CoinGecko ----
    try:
        coin = fetch_coin_full(cid, cg_rl)
        fr.name = coin.get("name") or cid
    except Exception as e:
        if settings.verbose: log_info(f"[WARN] CoinGecko error for {cid}: {e}")
        with state_lock:
            state[cid] = {**st, "last_checked": now_ts(), "status": "cg_error"}
        return fr, None

    reasons_ok: List[str] = []
    reasons_ko: List[str] = []
    extra: Dict[str, Any] = {}

    # ---- Stage 2: Top spot CEX = Bitget ----
    bitget_ok, top_name = top_spot_cex_bitget_ok(coin, settings.bitget_dominance, settings.max_tickers_scan)
    extra["top_spot_cex"] = top_name
    fr.top_spot_cex = top_name
    if not bitget_ok:
        with state_lock:
            state[cid] = {**st, "last_checked": now_ts(), "status": "top_cex_not_bitget", "top": top_name}
        return fr, None
    fr.s2_bitget = True
    reasons_ok.append("Top spot CEX = Bitget")

    # ---- Stage 3: BSC + main LP Pancake ----
    bsc_addr = bsc_contract_address(coin)
    extra["bsc_address"] = bsc_addr
    if not bsc_addr:
        with state_lock:
            state[cid] = {**st, "last_checked": now_ts(), "status": "no_bsc"}
        return fr, None
    try:
        pair = main_bsc_pair_on_pancake(bsc_addr, ds_rl)
    except Exception as e:
        pair = None
        if settings.verbose: log_info(f"[WARN] Dexscreener error for {cid}: {e}")
    if not pair:
        with state_lock:
            state[cid] = {**st, "last_checked": now_ts(), "status": "no_pancake_or_low_liq"}
        return fr, None

    dexid = (pair.get("dexId") or "")
    if settings.require_v3 and "v3" not in dexid.lower():
        with state_lock:
            state[cid] = {**st, "last_checked": now_ts(), "status": "pancake_not_v3"}
        return fr, None

    lp_liq = float(((pair.get("liquidity") or {}).get("usd") or 0))
    pool_addr = (pair.get("pairAddress") or "").lower()
    fr.s3_bsc_pancake = True
    fr.lp_pair = {
        "dexId": dexid, "pairAddress": pool_addr,
        "liquidityUsd": lp_liq, "priceUsd": pair.get("priceUsd"),
    }
    extra["lp_pair"] = fr.lp_pair
    reasons_ok.append("LP principale Pancake" + (" V3" if "v3" in dexid.lower() else ""))

    with state_lock:
        state[cid] = {
            "last_checked": now_ts(),
            "status": "done",
            "top": extra.get("top_spot_cex"),
            "lp_pair": fr.lp_pair,
        }

    res = CheckResult(cid, fr.symbol, fr.name, reasons_ok, reasons_ko, extra)
    return fr, res

# --------------------------- Seeds --------------------------- #

def list_market_id_symbols(pages: int, per_page: int, cg_rl: RateLimiter) -> List[tuple]:
    rows: List[tuple] = []
    for p in range(1, pages + 1):
        url = f"{COINGECKO}/coins/markets?vs_currency=usd&order=market_cap_desc&per_page={per_page}&page={p}&sparkline=false&locale=en"
        cg_rl.wait()
        data = Http.get(url).json()
        for x in data:
            cid = x.get("id")
            sym = (x.get("symbol") or "").upper()
            if cid: rows.append((cid, sym))
    return rows

def build_seed_from_perps(cg_rl: RateLimiter,
                          binance_bases: Dict[str, Any],
                          bybit_bases: Dict[str, Any]) -> List[Tuple[str, str]]:
    """Intersezione Binance∩Bybit (base symbol) -> mappatura a CG id via /coins/list?include_platform=true"""
    inter_syms = sorted(set(binance_bases.keys()) & set(bybit_bases.keys()))
    if RICH:
        console.print(Panel.fit(f"[bold]Seed from PERPS[/bold]\nBinance bases: {len(binance_bases)} | Bybit bases: {len(bybit_bases)}\nIntersezione: [bold]{len(inter_syms)}[/bold]",
                                title="Seed", border_style="cyan"))
    else:
        log_info(f"Seed from PERPS — intersezione: {len(inter_syms)}")

    cg_list = load_cg_coins_list_cached(cg_rl)

    mapped: List[Tuple[str, str]] = []
    unmapped: List[str] = []
    for sym in inter_syms:
        picked = choose_best_cg_id_for_symbol(sym, cg_list)
        if picked is None:
            unmapped.append(sym)
        else:
            cid, _name = picked
            mapped.append((cid, sym))

    if RICH:
        t = Table(title="Mappatura Symbol → CoinGecko ID", box=box.SIMPLE_HEAVY)
        t.add_column("Tot Intersezione", justify="right")
        t.add_column("Mappati", justify="right")
        t.add_column("Non mappati", justify="right")
        t.add_row(str(len(inter_syms)), str(len(mapped)), str(len(unmapped)))
        console.print(t)
        if unmapped[:20]:
            console.print(Panel.fit(", ".join(unmapped[:20]) + (f" ... (+{len(unmapped)-20})" if len(unmapped) > 20 else ""),
                                    title="Esempi non mappati", border_style="yellow"))
    else:
        log_info(f"Intersezione: {len(inter_syms)} | Mappati: {len(mapped)} | Non mappati: {len(unmapped)}")

    return mapped  # [(cg_id, symbol)]

# --------------------------- Rendering --------------------------- #

def _print_stage_list(title: str, rows: List[FunnelRow], show: int, extra_cols: Optional[List[Tuple[str, str]]] = None):
    if not rows:
        if RICH: console.print(f"[bold yellow]{title}: 0[/bold yellow]")
        else: print(f"{title}: 0")
        return
    if RICH:
        t = Table(title=f"{title} — {len(rows)}", box=box.SIMPLE_HEAVY)
        t.add_column("Symbol"); t.add_column("Name")
        if extra_cols:
            for h, _ in extra_cols:
                t.add_column(h)
        for r in rows[:show]:
            cells = [r.symbol, r.name]
            if extra_cols:
                for _, attr in extra_cols:
                    val = getattr(r, attr, None)
                    if isinstance(val, dict):
                        cells.append(str(val.get("dexId") or val.get("liquidityUsd") or val))
                    else:
                        cells.append("" if val is None else str(val))
            t.add_row(*cells)
        console.print(t)
        if len(rows) > show:
            console.print(f"[dim]...e altre {len(rows)-show}[/dim]")
    else:
        print(f"{title}: {len(rows)}")
        for r in rows[:show]:
            print(f" - {r.symbol:6} {r.name}")
        if len(rows) > show:
            print(f"...e altre {len(rows)-show}")

def print_funnel(funnel_rows: List[FunnelRow], show: int, total_candidates: int):
    total_skipped = sum(1 for r in funnel_rows if r.skipped)
    processed = [r for r in funnel_rows if not r.skipped]

    s1 = [r for r in processed if r.s1_perps]
    s2 = [r for r in processed if r.s1_perps and r.s2_bitget]
    s3 = [r for r in processed if r.s1_perps and r.s2_bitget and r.s3_bsc_pancake]

    if RICH:
        console.print(Rule(title="[bold]Funnel (run corrente)[/bold]"))
        console.print(f"Totale candidati: [bold]{total_candidates}[/bold] | "
                      f"Processati: [bold]{len(processed)}[/bold] | Skippati: [bold]{total_skipped}[/bold] (recenti)")
    else:
        print("==== Funnel ====")
        print(f"Totale candidati: {total_candidates} | Processati: {len(processed)} | Skippati: {total_skipped}")

    _print_stage_list("Stage 1 — Perps Binance & Bybit", s1, show)
    _print_stage_list("Stage 1+2 — + Top spot = Bitget", s2, show, extra_cols=[("Top CEX", "top_spot_cex")])
    _print_stage_list("Stage 1+2+3 — + BSC Pancake (main LP)", s3, show, extra_cols=[("LP", "lp_pair")])

def print_results(results: List[CheckResult], funnel_rows: List[FunnelRow], total_candidates: int):
    print_funnel(funnel_rows, settings.funnel_show, total_candidates)

    all3 = [r for r in results if r.score() >= 2 and r.ok()]  # 2 OK: Bitget + Pancake (BSC)
    at_least2 = []
    for r in funnel_rows:
        if not r.skipped and r.s1_perps and r.s2_bitget:
            at_least2.append(r)

    if RICH:
        if all3:
            t = Table(title="Coin che rispettano TUTTI e 3 i parametri", box=box.SIMPLE_HEAVY)
            t.add_column("Symbol"); t.add_column("Name"); t.add_column("Top Spot CEX"); t.add_column("LP (Dex)"); t.add_column("LP Liquidity $", justify="right")
            for r in all3:
                lp = r.extra.get("lp_pair", {}) if r.extra else {}
                t.add_row(r.symbol, r.name, str(r.extra.get("top_spot_cex")), str(lp.get("dexId")), f"{lp.get('liquidityUsd')}")
            console.print(t)
        else:
            console.print("[bold yellow]Nessuna coin che rispetta tutti e 3 i vincoli[/bold yellow]")

        if at_least2:
            t = Table(title="Coin che rispettano ALMENO 2 parametri (S1+S2)", box=box.SIMPLE_HEAVY)
            t.add_column("Symbol"); t.add_column("Name"); t.add_column("Top CEX")
            for r in at_least2:
                t.add_row(r.symbol, r.name, str(r.top_spot_cex))
            console.print(t)
        else:
            console.print("[bold yellow]Nessuna coin con almeno 2 parametri[/bold yellow]")
    else:
        print("== Coin che rispettano TUTTI e 3 i parametri ==")
        for r in all3:
            lp = r.extra.get("lp_pair", {})
            print(f"{r.symbol:<8} | {r.name:<24} | topCEX={r.extra.get('top_spot_cex')} | LP={lp.get('dexId')} ${lp.get('liquidityUsd')}")
        print("== Coin che rispettano ALMENO 2 parametri (S1+S2) ==")
        for r in at_least2:
            print(f"{r.symbol:6} | {r.name:24} | Top CEX: {r.top_spot_cex}")

# --------------------------- CLI --------------------------- #

def parse_args() -> Settings:
    ap = argparse.ArgumentParser(description="Crypto screener per CEX/DEX (3 criteri + funnel, cache-aware & robust, perps-first)")
    ap.add_argument("--pages", type=int, default=1)
    ap.add_argument("--per-page", type=int, default=250)
    ap.add_argument("--ids", type=str, default=None, help="Lista di ids CoinGecko separati da virgola (bypassa seed)")
    ap.add_argument("--workers", type=int, default=6, help="Numero di worker concorrenti")

    ap.add_argument("--min-liq", type=float, default=150000.0)
    ap.add_argument("--require-v3", action="store_true", help="Accetta solo LP Pancake v3")

    # (compat flags non usati in questa versione, mantenuti per retro-compat)
    ap.add_argument("--price-mult", type=float, default=3.0, help=argparse.SUPPRESS)
    ap.add_argument("--max-ticks-above", type=int, default=0, help=argparse.SUPPRESS)
    ap.add_argument("--ttl-sg", type=int, default=24*3600, help=argparse.SUPPRESS)
    ap.add_argument("--rps-sg", type=float, default=1.0, help=argparse.SUPPRESS)
    ap.add_argument("--dex-liq-delta", type=float, default=0.4, help=argparse.SUPPRESS)

    ap.add_argument("--dominance", type=float, default=0.60, help="Dominanza Bitget per short-circuit (0-1)")
    ap.add_argument("--max-tickers-scan", type=int, default=15)

    ap.add_argument("--ttl-cg", type=int, default=48*3600)
    ap.add_argument("--ttl-perps", type=int, default=7*24*3600)
    ap.add_argument("--ttl-ds", type=int, default=24*3600)

    ap.add_argument("--skip-unchanged-days", type=int, default=3)

    ap.add_argument("--cache-root", type=str, default=".cache")
    ap.add_argument("--state-root", type=str, default="state")

    ap.add_argument("--rps-cg", type=float, default=1.0)
    ap.add_argument("--rps-ds", type=float, default=2.0)

    ap.add_argument("--funnel-show", type=int, default=30, help="Max righe da mostrare per stage")
    ap.add_argument("--seed-from", type=str, choices=["perps","cg"], default="perps",
                    help="Origine seed: 'perps' (intersezione Binance∩Bybit) o 'cg' (coins/markets)")

    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--no-verbose", dest="verbose", action="store_false")
    ap.set_defaults(verbose=True)
    args = ap.parse_args()

    s = Settings(
        pages=args.pages, per_page=args.per_page,
        ids=[x.strip() for x in args.ids.split(",")] if args.ids else None,
        workers=max(1, args.workers),

        min_pair_liquidity_usd=args.min_liq, require_v3=args.require_v3,

        max_tickers_scan=args.max_tickers_scan, bitget_dominance=args.dominance,

        ttl_cg_coin_sec=args.ttl_cg, ttl_perps_sec=args.ttl_perps,
        ttl_dexscreener_sec=args.ttl_ds,

        skip_unchanged_days=args.skip_unchanged_days,

        cache_root=args.cache_root, state_root=args.state_root,
        verbose=args.verbose, quiet=args.quiet,

        rps_cg=max(0.2, args.rps_cg), rps_ds=max(0.5, args.rps_ds),

        funnel_show=max(1, args.funnel_show),

        seed_from=args.seed_from,
    )
    return s

# --------------------------- Main --------------------------- #

def main():
    global settings
    settings = parse_args()
    ensure_dir(settings.cache_root); ensure_dir(settings.state_root)

    # Rate limiters
    cg_rl = RateLimiter(settings.rps_cg)
    ds_rl = RateLimiter(settings.rps_ds)

    # Perps (cache 7g)
    log_info("Scarico lista perps (cache-aware)...")
    binance_bases = load_binance_perp_bases_cached()
    bybit_bases   = load_bybit_perp_bases_cached()
    if RICH:
        console.print(Panel.fit(f"Binance perps: [bold]{len(binance_bases)}[/bold] | "
                                f"Bybit perps: [bold]{len(bybit_bases)}[/bold]", border_style="green"))
    else:
        log_info(f"Binance perps: {len(binance_bases)} | Bybit perps: {len(bybit_bases)}")

    # Seeds
    if settings.ids:
        id_syms = [(cid, "") for cid in settings.ids]
        log_info(f"Seed da --ids: {len(id_syms)}")
    else:
        if settings.seed_from == "perps":
            id_syms = build_seed_from_perps(cg_rl, binance_bases, bybit_bases)  # [(cg_id, symbol)]
        else:
            log_info("Scarico lista CoinGecko markets (seed cg)...")
            id_syms = list_market_id_symbols(settings.pages, settings.per_page, cg_rl)
            if RICH:
                console.print(Panel.fit(f"Totale markets scaricati: [bold]{len(id_syms)}[/bold]", border_style="cyan"))
            # Pre-filtra per simbolo: solo quelli con perps su entrambi
            inter = set(binance_bases.keys()) & set(bybit_bases.keys())
            before = len(id_syms)
            id_syms = [row for row in id_syms if (row[1] or "").upper() in inter]
            if RICH:
                console.print(Panel.fit(f"Candidati dopo filtro perps Binance&Bybit: [bold]{len(id_syms)}[/bold] (da {before})",
                                        border_style="cyan"))
            else:
                log_info(f"Candidati dopo filtro perps Binance&Bybit: {len(id_syms)} (da {before})")

    # Stato persistente
    state = load_state()

    # Concorrenza controllata
    results: List[CheckResult] = []
    funnel_rows: List[FunnelRow] = []

    if RICH:
        prog = Progress("[progress.description]{task.description}",
                        BarColumn(), "[progress.percentage]{task.percentage:>3.0f}%",
                        TimeElapsedColumn())
        with prog:
            task_id = prog.add_task("Analisi coin", total=len(id_syms))
            with ThreadPoolExecutor(max_workers=settings.workers) as ex:
                futures = {
                    ex.submit(screen_coin, cid, sym, binance_bases, bybit_bases, cg_rl, ds_rl, state): cid
                    for (cid, sym) in id_syms
                }
                for fut in as_completed(futures):
                    fr, res = fut.result()
                    funnel_rows.append(fr)
                    if res: results.append(res)
                    prog.advance(task_id)
    else:
        with ThreadPoolExecutor(max_workers=settings.workers) as ex:
            futures = {
                ex.submit(screen_coin, cid, sym, binance_bases, bybit_bases, cg_rl, ds_rl, state): cid
                for (cid, sym) in id_syms
            }
            for fut in as_completed(futures):
                fr, res = fut.result()
                funnel_rows.append(fr)
                if res: results.append(res)

    # Salva stato e stampa
    save_state(state)
    print_results(results, funnel_rows, total_candidates=len(id_syms))

if __name__ == "__main__":
    main()
