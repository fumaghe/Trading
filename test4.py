#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crypto Screener (v2.3.5) — Output TOP-only con Pool address

Fix principali:
- Seed robustness:
  - Normalizzazione simboli per futures multiplier (es. 1000PEPE -> PEPE)
  - Seed costruito su unione Binance∪Bybit + filtro has_perps_on_both()
  - Mapping CoinGecko su symbol normalizzato
- Fix cache freshness:
  - TTL separato per CoinGecko coins list (seed mapping) vs coin detail
  - Retry: se alcuni symbol non mappano, refresh list UNA volta e riprova
- Diagnostica forte:
  - Se TOP=0 stampa su STDERR: sizes perps, counts seed/mapped, e (se fallisce fetch perps senza cache) l’errore reale.
- Persistenza state (finalmente):
  - state/test4_state.json

Nota:
- stdout: SOLO sezione TOP (per parsing in run_pipeline)
- stderr: diagnostica
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
from collections import Counter

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
    RICH = True
    console = Console()
except Exception:
    RICH = False
    console = None

# --------------------------- Endpoints --------------------------- #

COINGECKO = "https://api.coingecko.com/api/v3"
DEXSCREENER_TOKEN = "https://api.dexscreener.com/latest/dex/tokens/{address}"

# Binance: spesso i runner GitHub hanno problemi con fapi.binance.com → aggiungo fallback
BINANCE_FUTURES_INFO_CANDIDATES = [
    "https://fapi.binance.com/fapi/v1/exchangeInfo",
    "https://data-api.binance.vision/fapi/v1/exchangeInfo",
    "https://www.binance.com/fapi/v1/exchangeInfo",
]

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
        time.sleep(random.uniform(0.05, 0.20))

# --------------------------- Settings --------------------------- #

@dataclass
class Settings:
    pages: int = 1
    per_page: int = 250
    ids: Optional[List[str]] = None
    workers: int = 6

    # Stage thresholds
    min_pair_liquidity_usd: float = 150_000.0
    require_v3: bool = False

    # Heuristica Bitget
    max_tickers_scan: int = 15
    bitget_dominance: float = 0.60  # 0..1

    # TTLs
    ttl_cg_coin_sec: int = 12 * 3600     # coin detail
    ttl_cg_list_sec: int = 2 * 3600      # coins list include_platform
    ttl_perps_sec: int = 6 * 3600
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
    rps_cg: float = 1.0
    rps_ds: float = 2.0

    # Funnel printing
    funnel_show: int = 30

    # Seed mode: "perps" (default) o "cg"
    seed_from: str = "perps"

    # force refresh flags
    refresh_perps: bool = False
    refresh_cg: bool = False

# --------------------------- Utilities & logging --------------------------- #

def now_ts() -> int:
    return int(time.time())

def ensure_dir(p: str):
    Path(p).mkdir(parents=True, exist_ok=True)

def joinp(*a) -> str:
    return os.path.join(*a)

def read_json(path: str) -> Optional[dict]:
    p = Path(path)
    if not p.exists():
        return None
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def write_json_atomic(path: str, data: dict):
    ensure_dir(os.path.dirname(path))
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, path)

def cache_get(path: str, ttl_sec: int) -> Optional[dict]:
    if ttl_sec <= 0:
        return None
    p = Path(path)
    if not p.exists():
        return None
    try:
        age = time.time() - p.stat().st_mtime
        if age > ttl_sec:
            return None
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def normalize_symbol(sym: str) -> str:
    """
    Normalizza futures multiplier:
      10XYZ / 100XYZ / 1000XYZ / 10000XYZ -> XYZ
    """
    sym = (sym or "").upper().strip()
    m = re.match(r"^(10|100|1000|10000)([A-Z0-9]+)$", sym)
    return m.group(2) if m else sym

# --------------------------- HTTP --------------------------- #

class Http:
    sess = requests.Session()
    sess.headers.update(DEFAULT_HEADERS)

    @staticmethod
    @retry(stop=stop_after_attempt(4),
           wait=wait_exponential(multiplier=1, min=1, max=8),
           retry=retry_if_exception_type((requests.RequestException,)))
    def get(url: str, **kwargs) -> requests.Response:
        headers = kwargs.pop("headers", {})
        all_headers = {**DEFAULT_HEADERS, **headers}
        r = Http.sess.get(url, timeout=25, headers=all_headers, **kwargs)
        if r.status_code in (429, 418):
            wait_s = int(r.headers.get("Retry-After", "5"))
            time.sleep(max(wait_s, 5))
            raise requests.RequestException(f"{r.status_code} Too Many Requests")
        if r.status_code in (403, 401, 451):
            time.sleep(3)
            raise requests.RequestException(f"{r.status_code} Forbidden/Unauthorized/Unavailable")
        if r.status_code >= 400:
            raise requests.RequestException(f"GET {url} -> {r.status_code} {r.text[:200]}")
        return r

# --------------------------- Providers: Perps --------------------------- #

def load_binance_perp_bases_cached(force_refresh: bool = False) -> Dict[str, Any]:
    cache_path = joinp(settings.cache_root, "perps_binance.json")
    if not force_refresh:
        j = cache_get(cache_path, settings.ttl_perps_sec)
        if j is not None:
            return j

    last_exc = None
    for url in BINANCE_FUTURES_INFO_CANDIDATES:
        try:
            data = Http.get(url).json()
            res = {}
            for s in data.get("symbols", []):
                if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING":
                    res[s["baseAsset"].upper()] = s
            write_json_atomic(cache_path, res)
            return res
        except Exception as e:
            last_exc = e

    # fallback a cache anche se vecchia (meglio di zero)
    j2 = read_json(cache_path)
    if not j2:
        sys.stderr.write(f"[test4 diag] Binance perps fetch FAILED and no cache: {repr(last_exc)}\n")
    return j2 or {}

def load_bybit_perp_bases_cached(force_refresh: bool = False) -> Dict[str, Any]:
    cache_path = joinp(settings.cache_root, "perps_bybit.json")
    if not force_refresh:
        j = cache_get(cache_path, settings.ttl_perps_sec)
        if j is not None:
            return j

    try:
        res: Dict[str, Any] = {}
        cursor = None
        while True:
            params = {"category": "linear", "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            data = Http.get(BYBIT_LINEAR, params=params).json()
            lst = (data.get("result") or {}).get("list") or []
            for s in lst:
                if s.get("status") == "Trading" and s.get("contractType") in ("LinearPerpetual", "LinearFutures"):
                    symbol = s.get("symbol", "")
                    base = re.sub(r"(USDT|USDC)$", "", symbol).upper()
                    if base:
                        res[base] = s
            cursor = (data.get("result") or {}).get("nextPageCursor")
            if not cursor:
                break
        write_json_atomic(cache_path, res)
        return res
    except Exception as e:
        j2 = read_json(cache_path)
        if not j2:
            sys.stderr.write(f"[test4 diag] Bybit perps fetch FAILED and no cache: {repr(e)}\n")
        return j2 or {}

def base_symbol_candidates(base: str) -> List[str]:
    base = (base or "").upper()
    cands = {base}
    for k in ("10", "100", "1000", "10000"):
        cands.add(f"{k}{base}")
    m = re.match(r"^(10|100|1000|10000)([A-Z0-9]+)$", base)
    if m:
        cands.add(m.group(2))
    return list(cands)

def has_perps_on_both(binance_bases: Dict[str, Any], bybit_bases: Dict[str, Any], symbol: str) -> bool:
    cands = base_symbol_candidates(symbol)
    return any(c in binance_bases for c in cands) and any(c in bybit_bases for c in cands)

# --------------------------- CoinGecko --------------------------- #

def cg_coin_cache_path(cid: str) -> str:
    return joinp(settings.cache_root, "coingecko", f"{cid}.json")

def fetch_coin_full(cid: str, cg_rl: RateLimiter, force_refresh: bool = False) -> Dict[str, Any]:
    cache_path = cg_coin_cache_path(cid)
    if not force_refresh:
        j = cache_get(cache_path, settings.ttl_cg_coin_sec)
        if j is not None:
            return j
    url = f"{COINGECKO}/coins/{cid}?localization=false&tickers=true&market_data=false&community_data=false&developer_data=false&sparkline=false"
    cg_rl.wait()
    data = Http.get(url).json()
    write_json_atomic(cache_path, data)
    return data

def bsc_contract_address(coin: Dict[str, Any]) -> Optional[str]:
    platforms = coin.get("platforms") or {}
    for key in platforms.keys():
        if key and key.lower() in ("binance-smart-chain", "bsc", "bnb-smart-chain", "bnb"):
            addr = platforms[key]
            if addr:
                return addr.lower()
    return None

def top_spot_cex_bitget_ok(coin: Dict[str, Any], dominance: float, max_scan: int) -> Tuple[bool, Optional[str]]:
    """
    Ritorna (is_bitget_top, top_name).
    Più permissivo: considera Bitget se 'bitget' appare in name o identifier.
    """
    tickers = coin.get("tickers", []) or []
    best_name, best_vol = None, -1.0
    best_is_bitget = False

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

        is_bitget = ("bitget" in (name or "").lower()) or ("bitget" in ident)

        conv = t.get("converted_volume") or {}
        vol = None
        for k in ("usd", "eur", "btc"):
            if conv.get(k) is not None:
                vol = float(conv[k])
                break
        if vol is None:
            vol = 0.0

        seen += 1
        sum_vol += vol

        if vol > best_vol:
            best_vol = vol
            best_name = name
            best_is_bitget = is_bitget

        if is_bitget:
            bitget_vol += vol

        if seen >= max_scan and sum_vol > 0:
            if (bitget_vol / sum_vol) >= dominance:
                return True, "Bitget"
            break

    return best_is_bitget, best_name

def load_cg_coins_list_cached(cg_rl: RateLimiter, force_refresh: bool = False) -> List[Dict[str, Any]]:
    cache_path = joinp(settings.cache_root, "coingecko", "coins_list_include_platform.json")
    if not force_refresh:
        j = cache_get(cache_path, settings.ttl_cg_list_sec)
        if j is not None:
            return j
    url = f"{COINGECKO}/coins/list?include_platform=true"
    cg_rl.wait()
    data = Http.get(url).json()
    write_json_atomic(cache_path, data)
    return data

def choose_best_cg_id_for_symbol(symbol: str, cg_list: List[Dict[str, Any]]) -> Optional[Tuple[str, str]]:
    sym_up = (symbol or "").upper()
    candidates = []
    for row in cg_list:
        sym = (row.get("symbol") or "").upper()
        if sym != sym_up:
            continue
        name = row.get("name") or ""
        platforms = (row.get("platforms") or {})
        has_bsc = any(k.lower() in ("binance-smart-chain", "bsc", "bnb", "bnb-smart-chain") for k in platforms.keys())
        candidates.append((row.get("id"), name, has_bsc))
    if not candidates:
        return None
    for cid, name, has_bsc in candidates:
        if has_bsc:
            return cid, name
    cid, name, _ = candidates[0]
    return cid, name

# --------------------------- DexScreener --------------------------- #

def main_bsc_pair_on_pancake(addr: str, ds_rl: RateLimiter) -> Optional[Dict[str, Any]]:
    ds_rl.wait()
    url = DEXSCREENER_TOKEN.format(address=addr)
    data = Http.get(url).json()

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

    if not best:
        return None

    dexid = (best.get("dexId") or "").lower()
    if "pancake" not in dexid:
        return None

    return best

# --------------------------- State index --------------------------- #

def state_file_path() -> str:
    return joinp(settings.state_root, "test4_state.json")

def load_state() -> Dict[str, Any]:
    ensure_dir(settings.state_root)
    p = Path(state_file_path())
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}

def save_state(state: Dict[str, Any]):
    ensure_dir(settings.state_root)
    write_json_atomic(state_file_path(), state)

def stale_by_days(ts: Optional[int], days: int) -> bool:
    if days <= 0:
        return True
    if not ts:
        return True
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

state_lock = threading.Lock()

def screen_coin(cid: str, sym_hint: str,
                binance_bases: Dict[str, Any],
                bybit_bases: Dict[str, Any],
                cg_rl: RateLimiter, ds_rl: RateLimiter,
                state: Dict[str, Any]) -> Tuple[FunnelRow, Optional[CheckResult]]:
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

    # ---- Stage 1: Perps su entrambi ----
    if not has_perps_on_both(binance_bases, bybit_bases, fr.symbol):
        with state_lock:
            state[cid] = {**st, "last_checked": now_ts(), "status": "no_perps"}
        return fr, None
    fr.s1_perps = True

    # ---- CoinGecko ----
    try:
        coin = fetch_coin_full(cid, cg_rl, force_refresh=settings.refresh_cg)
        fr.name = coin.get("name") or cid
    except Exception:
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
    except Exception:
        pair = None

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

def build_seed_from_perps(cg_rl: RateLimiter,
                          binance_bases: Dict[str, Any],
                          bybit_bases: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    Seed robusto:
    1) Binance∪Bybit
    2) Normalizza (1000XXX -> XXX)
    3) Filtra con has_perps_on_both()
    4) Mappa a CoinGecko id con /coins/list?include_platform=true
    """
    raw_syms = set(binance_bases.keys()) | set(bybit_bases.keys())
    norm_syms = sorted({normalize_symbol(s) for s in raw_syms if s})

    eligible = [s for s in norm_syms if has_perps_on_both(binance_bases, bybit_bases, s)]

    cg_list = load_cg_coins_list_cached(cg_rl, force_refresh=settings.refresh_cg)

    mapped: List[Tuple[str, str]] = []
    missing: List[str] = []

    for sym in eligible:
        picked = choose_best_cg_id_for_symbol(sym, cg_list)
        if picked is None:
            missing.append(sym)
            continue
        cid, _name = picked
        mapped.append((cid, sym))

    # Retry refresh UNA volta se mancano mapping e non era già refresh
    if missing and (not settings.refresh_cg):
        try:
            cg_list_fresh = load_cg_coins_list_cached(cg_rl, force_refresh=True)
            for sym in missing:
                picked = choose_best_cg_id_for_symbol(sym, cg_list_fresh)
                if picked is None:
                    continue
                cid, _name = picked
                mapped.append((cid, sym))
        except Exception:
            pass

    # diagnostica seed
    sys.stderr.write(
        f"[test4 diag] seed_build | binance_perps={len(binance_bases)} bybit_perps={len(bybit_bases)} "
        f"raw_syms={len(raw_syms)} eligible={len(eligible)} mapped={len(mapped)} missing_map={len(missing)}\n"
    )

    return mapped

# --------------------------- Rendering TOP-only --------------------------- #

def format_eur_style_usd(amount: Any) -> str:
    try:
        iv = int(float(amount))
        return f"{iv:,}$".replace(",", ".")
    except Exception:
        return f"{amount}$"

def build_top_only_output(results: List[CheckResult]) -> str:
    all3 = [r for r in results if r.score() >= 2 and r.ok()]
    lines: List[str] = ["— Tutti e 3 i parametri (TOP) —", ""]
    for r in all3:
        lp = r.extra.get("lp_pair", {}) if r.extra else {}
        liq = format_eur_style_usd(lp.get("liquidityUsd"))
        pool_addr = lp.get("pairAddress") or ""
        name = r.name or r.id
        lines.append(f"{r.symbol} · {name} · LP: {liq} · Pool: {pool_addr}")
    return "\n".join(lines)

# --------------------------- CLI --------------------------- #

def parse_args() -> Settings:
    ap = argparse.ArgumentParser(description="Crypto screener per CEX/DEX (3 criteri + TOP-only, cache-aware & robust)")

    ap.add_argument("--pages", type=int, default=1)
    ap.add_argument("--per-page", type=int, default=250)
    ap.add_argument("--ids", type=str, default=None, help="Lista di ids CoinGecko separati da virgola (bypassa seed)")
    ap.add_argument("--workers", type=int, default=6)

    ap.add_argument("--min-liq", type=float, default=150000.0)
    ap.add_argument("--require-v3", action="store_true")

    ap.add_argument("--dominance", type=float, default=0.60)
    ap.add_argument("--max-tickers-scan", type=int, default=15)

    ap.add_argument("--ttl-cg", type=int, default=12*3600)
    ap.add_argument("--ttl-cg-list", type=int, default=None,
                    help="Override TTL cache CoinGecko coins/list (sec). Default: min(--ttl-cg, 2h).")
    ap.add_argument("--ttl-perps", type=int, default=6*3600)
    ap.add_argument("--ttl-ds", type=int, default=24*3600)

    ap.add_argument("--skip-unchanged-days", type=int, default=3)

    ap.add_argument("--cache-root", type=str, default=".cache")
    ap.add_argument("--state-root", type=str, default="state")

    ap.add_argument("--rps-cg", type=float, default_toggle=None)
    ap.add_argument("--rps-ds", type=float, default=2.0)

    ap.add_argument("--funnel-show", type=int, default=30)
    ap.add_argument("--seed-from", type=str, choices=["perps", "cg"], default="perps")

    ap.add_argument("--refresh-perps", action="store_true")
    ap.add_argument("--refresh-cg", action="store_true")
    ap.add_argument("--refresh-all", action="store_true")

    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--no-verbose", dest="verbose", action="store_false")
    ap.set_defaults(verbose=True)

    # compat: alcuni env passano float None -> gestisco dopo
    args = ap.parse_args()

    refresh_perps = bool(args.refresh_perps or args.refresh_all)
    refresh_cg = bool(args.refresh_cg or args.refresh_all)

    ttl_cg_coin = int(args.ttl_cg)
    ttl_cg_list = int(args.ttl_cg_list) if args.ttl_cg_list is not None else min(ttl_cg_coin, 2 * 3600)

    # rps_cg: se non è stato passato, default 1.0
    rps_cg = getattr(args, "rps_cg", None)
    rps_cg = 1.0 if (rps_cg is None) else float(rps_cg)

    return Settings(
        pages=args.pages,
        per_page=args.per_page,
        ids=[x.strip() for x in args.ids.split(",")] if args.ids else None,
        workers=max(1, args.workers),

        min_pair_liquidity_usd=args.min_liq,
        require_v3=args.require_v3,

        max_tickers_scan=args.max_tickers_scan,
        bitget_dominance=args.dominance,

        ttl_cg_coin_sec=ttl_cg_coin,
        ttl_cg_list_sec=ttl_cg_list,
        ttl_perps_sec=int(args.ttl_perps),
        ttl_dexscreener_sec=int(args.ttl_ds),

        skip_unchanged_days=args.skip_unchanged_days,

        cache_root=args.cache_root,
        state_root=args.state_root,

        verbose=args.verbose,
        quiet=args.quiet,

        rps_cg=max(0.2, rps_cg),
        rps_ds=max(0.5, float(args.rps_ds)),

        funnel_show=max(1, args.funnel_show),

        seed_from=args.seed_from,

        refresh_perps=refresh_perps,
        refresh_cg=refresh_cg,
    )

# --------------------------- Main --------------------------- #

def main():
    global settings
    settings = parse_args()

    # TOP-only output su stdout
    settings.quiet = True
    settings.verbose = False

    ensure_dir(settings.cache_root)
    ensure_dir(settings.state_root)

    cg_rl = RateLimiter(settings.rps_cg)
    ds_rl = RateLimiter(settings.rps_ds)

    binance_bases = load_binance_perp_bases_cached(force_refresh=settings.refresh_perps)
    bybit_bases = load_bybit_perp_bases_cached(force_refresh=settings.refresh_perps)

    # debug sizes sempre su stderr
    sys.stderr.write(f"[test4 diag] perps sizes | binance={len(binance_bases)} bybit={len(bybit_bases)}\n")

    if settings.ids:
        id_syms = [(cid, "") for cid in settings.ids]
    else:
        if settings.seed_from == "perps":
            id_syms = build_seed_from_perps(cg_rl, binance_bases, bybit_bases)
        else:
            id_syms = []  # non usato

    state: Dict[str, Any] = load_state()

    results: List[CheckResult] = []
    funnel_rows: List[FunnelRow] = []

    if not id_syms:
        # nessun seed: stampo TOP header comunque, e diagnostica dettagliata
        out = build_top_only_output([])
        print(out)
        sys.stderr.write(
            "[test4 diag] TOP=0 | seeds=0 | statuses={} | "
            f"min_liq={settings.min_pair_liquidity_usd} | dominance={settings.bitget_dominance} | "
            f"max_scan={settings.max_tickers_scan} | ttl_cg_list={settings.ttl_cg_list_sec}s | "
            f"ttl_cg_coin={settings.ttl_cg_coin_sec}s | ttl_perps={settings.ttl_perps_sec}s\n"
        )
        return

    with ThreadPoolExecutor(max_workers=settings.workers) as ex:
        futures = {
            ex.submit(screen_coin, cid, sym, binance_bases, bybit_bases, cg_rl, ds_rl, state): cid
            for (cid, sym) in id_syms
        }
        for fut in as_completed(futures):
            fr, res = fut.result()
            funnel_rows.append(fr)
            if res:
                results.append(res)

    save_state(state)

    out = build_top_only_output(results)
    print(out)

    top_count = len([r for r in results if r.score() >= 2 and r.ok()])
    if top_count == 0:
        try:
            cnt = Counter((v or {}).get("status", "unknown") for v in state.values())
            sys.stderr.write(
                "[test4 diag] TOP=0 | "
                f"seeds={len(id_syms)} | "
                f"statuses={dict(cnt)} | "
                f"min_liq={settings.min_pair_liquidity_usd} | "
                f"dominance={settings.bitget_dominance} | "
                f"max_scan={settings.max_tickers_scan} | "
                f"ttl_cg_list={settings.ttl_cg_list_sec}s | ttl_cg_coin={settings.ttl_cg_coin_sec}s | ttl_perps={settings.ttl_perps_sec}s\n"
            )
        except Exception:
            pass

if __name__ == "__main__":
    main()

