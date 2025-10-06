#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crypto Screener (v2.0) — fast & polite (cache+gating+parallel)

Obiettivi (4 criteri):
  1) CEX spot principale per volume = Bitget (tickers CoinGecko)
  2) Perpetual su ENTRAMBI: Binance Futures (USDT-M) e Bybit (linear)
  3) Ha contratto su BSC e il main LP (maggiore liquidità) è su PancakeSwap (preferibilmente V3)
  4) Nella pool Pancake V3 non c'è liquidità significativa oltre una soglia di prezzo (es. +200%)

Ottimizzazioni:
- Stage-gating aggressivo per fermarsi presto.
- Cache multilivello (perps, CoinGecko per coin, DexScreener, Subgraph ticks).
- Indice locale state/index.json per saltare coin stabili tra i run (skip N giorni).
- Euristiche Bitget (short-circuit su primi tickers e dominanza volume).
- Lazy Subgraph: ricalcolo ticks solo su cambi sostanziali (pair/feeTier/tickSpacing o variazione liquidità > soglia).
- Parallelismo controllato (--workers) con rate-limit per provider (CoinGecko, DexScreener, Subgraph).
- Bybit v5 con paginazione (cursor).

Requisiti: requests, tenacity, rich
  pip install requests tenacity rich
"""
from __future__ import annotations
import argparse
import json
import math
import os
import re
import sys
import time
import threading
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
except Exception:  # pragma: no cover
    def retry(*args, **kwargs):
        def deco(fn):
            return fn
        return deco
    def stop_after_attempt(n): return None
    def wait_exponential(**kwargs): return None
    def retry_if_exception_type(*args, **kwargs): return None

try:
    from rich.console import Console
    from rich.table import Table
    from rich.progress import Progress, BarColumn, TimeElapsedColumn
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
PANCAKE_V3_SUBGRAPH = "https://api.thegraph.com/subgraphs/name/pancakeswap/exchange-v3-bsc"

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
    """Semplice rate limiter per-sorgente (token-bucket minimo)."""
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

# --------------------------- Settings --------------------------- #

@dataclass
class Settings:
    pages: int = 1
    per_page: int = 250
    ids: Optional[List[str]] = None
    workers: int = 6

    # Stage gating thresholds
    min_pair_liquidity_usd: float = 150_000.0
    price_multiplier: float = 3.0           # 3.0 => +200%
    max_ticks_above: int = 0
    require_v3: bool = False

    # Heuristica Bitget
    max_tickers_scan: int = 15              # guarda solo i primi N tickers
    bitget_dominance: float = 0.60          # short-circuit se Bitget >60% dei tickers visti

    # TTLs
    ttl_cg_coin_sec: int = 48 * 3600
    ttl_perps_sec: int = 7 * 24 * 3600
    ttl_dexscreener_sec: int = 24 * 3600
    ttl_subgraph_sec: int = 24 * 3600

    # Lazy subgraph: ricalcola ticks solo se cambia molto
    dex_liq_delta_ratio: float = 0.4        # 40% change = ricalcola ticks
    skip_unchanged_days: int = 3            # salta coin non cambiate per N giorni

    # Paths
    cache_root: str = ".cache"
    state_root: str = "state"

    # Verbose & safety
    verbose: bool = True
    quiet: bool = False

    # Rate limits (rps)
    rps_cg: float = 1.0      # CoinGecko
    rps_ds: float = 2.0      # DexScreener
    rps_sg: float = 1.0      # Subgraph (TheGraph)

# --------------------------- Utilities --------------------------- #

def now_ts() -> int:
    return int(time.time())

def ensure_dir(p: str):
    Path(p).mkdir(parents=True, exist_ok=True)

def joinp(*a) -> str:
    return os.path.join(*a)

def log(msg: str):
    if not settings.quiet:
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
            wait = int(r.headers.get("Retry-After", "5"))
            time.sleep(max(wait, 5))
            raise requests.RequestException(f"{r.status_code} Too Many Requests")
        if r.status_code in (403, 401):
            time.sleep(5)
            raise requests.RequestException(f"{r.status_code} Forbidden/Unauthorized")
        if r.status_code >= 400:
            raise requests.RequestException(f"GET {url} -> {r.status_code} {r.text[:200]}")
        return r

    @staticmethod
    @retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=1, max=8),
           retry=retry_if_exception_type((requests.RequestException,)))
    def post(url: str, **kwargs) -> requests.Response:
        headers = kwargs.pop("headers", {})
        all_headers = {**DEFAULT_HEADERS, **headers}
        r = Http.sess.post(url, timeout=30, headers=all_headers, **kwargs)
        if r.status_code in (429, 418):
            wait = int(r.headers.get("Retry-After", "5"))
            time.sleep(max(wait, 5))
            raise requests.RequestException(f"{r.status_code} Too Many Requests")
        if r.status_code in (403, 401):
            time.sleep(5)
            raise requests.RequestException(f"{r.status_code} Forbidden/Unauthorized")
        if r.status_code >= 400:
            raise requests.RequestException(f"POST {url} -> {r.status_code} {r.text[:200]}")
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
    for k in ("10", "100", "1000", "10000"): cands.add(f"{k}{base}")
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
    """Ritorna (is_bitget_top, top_name) con short-circuit euristico."""
    tickers = coin.get("tickers", []) or []
    best_name, best_vol = None, -1.0
    seen = 0
    sum_vol = 0.0
    bitget_vol = 0.0
    dex_like = {"uniswap", "pancakeswap", "sushiswap", "curve", "quickswap", "raydium", "balancer"}
    for t in tickers:
        market = (t.get("market") or {})
        name = (market.get("name") or "").strip()
        ident = (market.get("identifier") or "").lower()
        if any(d in ident for d in dex_like):
            continue
        conv = t.get("converted_volume") or {}
        vol = None
        for k in ("usd", "eur", "btc"):
            if conv.get(k) is not None: vol = float(conv[k]); break
        if vol is None: vol = 0.0
        seen += 1
        sum_vol += vol
        if vol > best_vol:
            best_vol, best_name = vol, name
        if name.lower().startswith("bitget"):
            bitget_vol += vol
        # short-circuit: primi N tickers
        if seen >= max_scan and sum_vol > 0:
            if bitget_vol / sum_vol >= dominance:
                return True, "Bitget"
            break
    # fallback: “top” assoluto
    return (best_name or "").lower().startswith("bitget"), best_name

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
    if "pancake" not in dexid: return None
    return best

# --------------------------- Subgraph (Pancake v3) --------------------------- #

def sg_cache_path(pool_addr: str) -> str:
    return joinp(settings.cache_root, "subgraph", f"{pool_addr.lower()}.json")

def sg_graphql(query: str, variables: Optional[Dict[str, Any]], sg_rl: RateLimiter) -> Dict[str, Any]:
    sg_rl.wait()
    r = Http.post(PANCAKE_V3_SUBGRAPH, json={"query": query, "variables": variables or {}})
    j = r.json()
    if "errors" in j:
        raise RuntimeError(f"Subgraph error: {j['errors']}")
    return j["data"]

def sg_get_pool_core(pool_addr: str, sg_rl: RateLimiter) -> Optional[Dict[str, Any]]:
    q = """
    query($id: ID!) {
      pool(id: $id) {
        id tick feeTier liquidity sqrtPrice
        token0 { id symbol decimals }
        token1 { id symbol decimals }
        tickSpacing
      }
    }
    """
    d = sg_graphql(q, {"id": pool_addr.lower()}, sg_rl).get("pool")
    return d

def sg_count_ticks_above(pool_addr: str, tick_threshold: int, sg_rl: RateLimiter, hard_limit: int = 3000) -> int:
    q = """
    query($pool: String!, $minTick: BigInt!, $skip: Int!) {
      ticks(first: 1000, skip: $skip,
            where: { pool: $pool, tickIdx_gt: $minTick, liquidityGross_gt: 0 },
            orderBy: tickIdx, orderDirection: asc) {
        tickIdx liquidityGross
      }
    }
    """
    total, skip = 0, 0
    while True:
        d = sg_graphql(q, {"pool": pool_addr.lower(), "minTick": tick_threshold, "skip": skip}, sg_rl)
        arr = d.get("ticks") or []
        total += len(arr)
        if len(arr) < 1000 or total >= hard_limit: break
        skip += 1000
    return total

# --------------------------- Math helpers --------------------------- #

def price_from_sqrtprice(sqrtPriceX96: int, dec0: int, dec1: int) -> float:
    ratio = (sqrtPriceX96 / (2 ** 96)) ** 2
    scale = (10 ** dec0) / (10 ** dec1)
    return ratio * scale

def tick_from_price(p: float) -> int:
    return math.floor(math.log(p, 1.0001))

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

def screen_coin(cid: str,
                binance_bases: Dict[str, Any],
                bybit_bases: Dict[str, Any],
                cg_rl: RateLimiter, ds_rl: RateLimiter, sg_rl: RateLimiter,
                state: Dict[str, Any]) -> Optional[CheckResult]:
    # ---- Pre-skip da indice locale ----
    st = state.get(cid, {})
    last_checked = st.get("last_checked")
    # Se invariata da pochi giorni e non c'è nulla di nuovo, si può saltare
    if not stale_by_days(last_checked, settings.skip_unchanged_days):
        if settings.verbose:
            log(f"[SKIP] {cid} recente (<= {settings.skip_unchanged_days}d)")
        return None

    # ---- CoinGecko (con cache e rate limit) ----
    try:
        coin = fetch_coin_full(cid, cg_rl)
    except Exception as e:
        if settings.verbose: log(f"[WARN] CoinGecko error for {cid}: {e}")
        return None

    name = coin.get("name") or cid
    symbol = (coin.get("symbol") or "").upper()
    reasons_ok: List[str] = []
    reasons_ko: List[str] = []
    extra: Dict[str, Any] = {}

    # ---- 2) Gate: Perps su entrambi gli exchange (più economico prima di tickers completi) ----
    if not has_perps_on_both(binance_bases, bybit_bases, symbol):
        # annota state e stop
        state[cid] = {**st, "last_checked": now_ts(), "status": "no_perps"}
        return None

    # ---- 1) Gate: Top spot CEX = Bitget (euristica dominanza+short-scan) ----
    bitget_ok, top_name = top_spot_cex_bitget_ok(coin, settings.bitget_dominance, settings.max_tickers_scan)
    extra["top_spot_cex"] = top_name
    if bitget_ok:
        reasons_ok.append("Top spot CEX = Bitget")
    else:
        # Non Bitget top → scarta (e salva state)
        state[cid] = {**st, "last_checked": now_ts(), "status": "top_cex_not_bitget", "top": top_name}
        return None

    # ---- 3) Gate: BSC + main LP Pancake ----
    bsc_addr = bsc_contract_address(coin)
    extra["bsc_address"] = bsc_addr
    if not bsc_addr:
        state[cid] = {**st, "last_checked": now_ts(), "status": "no_bsc"}
        return None

    try:
        pair = main_bsc_pair_on_pancake(bsc_addr, ds_rl)
    except Exception as e:
        pair = None
        if settings.verbose: log(f"[WARN] Dexscreener error for {cid}: {e}")
    if not pair:
        state[cid] = {**st, "last_checked": now_ts(), "status": "no_pancake_or_low_liq"}
        return None

    dexid = (pair.get("dexId") or "")
    lp_liq = float(((pair.get("liquidity") or {}).get("usd") or 0))
    pool_addr = (pair.get("pairAddress") or "").lower()
    extra["lp_pair"] = {
        "dexId": dexid, "pairAddress": pool_addr,
        "liquidityUsd": lp_liq, "priceUsd": pair.get("priceUsd"),
    }

    is_v3 = "v3" in dexid.lower()
    if is_v3: reasons_ok.append("LP principale Pancake V3")
    else:
        if settings.require_v3:
            state[cid] = {**st, "last_checked": now_ts(), "status": "pancake_not_v3", "lp": extra["lp_pair"]}
            return None
        reasons_ok.append("LP principale Pancake (non V3)")

    # ---- 4) Lazy Subgraph: decidi se ricalcolare i ticks ----
    # ricalcola solo se:
    # - prima non avevi pool, o pool/feeTier/tickSpacing sono cambiati, o liquidità delta > soglia
    need_ticks = True
    prev = st.get("lp_pair") or {}
    prev_pool = (prev.get("pairAddress") or "").lower()
    prev_dex = (prev.get("dexId") or "")
    prev_liq = float(prev.get("liquidityUsd") or 0.0)

    liq_delta = abs(lp_liq - prev_liq) / (prev_liq + 1e-9) if prev_liq > 0 else 1.0
    if prev_pool == pool_addr and prev_dex == dexid and liq_delta < settings.dex_liq_delta_ratio:
        # pool invariata e liquidità simile → si può riusare eventuali ticks/giudizio
        prev_ticks = st.get("ticks_above_threshold")
        prev_thr = st.get("tick_threshold")
        if prev_ticks is not None and prev_thr is not None:
            need_ticks = False

    ticks_above = None
    tick_thr = None
    tick_now = None
    p_now = None
    p_threshold = None

    if need_ticks:
        try:
            pool = sg_get_pool_core(pool_addr, sg_rl)
        except Exception as e:
            pool = None
            if settings.verbose: log(f"[WARN] Subgraph pool fetch failed for {name} {pool_addr}: {e}")
        if not pool:
            reasons_ko.append("Pool V3 non disponibile sul subgraph")
            # anche se non disponibile, restituiamo il risultato parziale (3/4)
            res = CheckResult(cid, symbol, name, reasons_ok, reasons_ko, extra)
            # salva state
            state[cid] = {
                **st, "last_checked": now_ts(), "status": "subgraph_missing",
                "lp_pair": extra["lp_pair"],
            }
            return res

        sqrt_x96 = int(pool["sqrtPrice"])
        dec0 = int(pool["token0"]["decimals"]) if pool.get("token0") else 18
        dec1 = int(pool["token1"]["decimals"]) if pool.get("token1") else 18
        p_now = price_from_sqrtprice(sqrt_x96, dec0, dec1)
        p_threshold = p_now * settings.price_multiplier
        tick_now = int(pool.get("tick") or 0)
        tick_thr = tick_from_price(p_threshold)

        try:
            ticks_above = sg_count_ticks_above(pool_addr, tick_thr, sg_rl)
        except Exception as e:
            ticks_above = None
            if settings.verbose: log(f"[WARN] Tick scan failed for {name}: {e}")
    else:
        # riusa i valori precedenti
        ticks_above = st.get("ticks_above_threshold")
        tick_thr = st.get("tick_threshold")
        tick_now = st.get("tick_now")
        p_now = st.get("p_now")
        p_threshold = st.get("p_threshold")

    extra["ticks_above_threshold"] = ticks_above
    extra["tick_now"] = tick_now
    extra["tick_threshold"] = tick_thr
    extra["p_now"] = p_now
    extra["p_threshold"] = p_threshold

    if ticks_above is not None and ticks_above <= settings.max_ticks_above:
        reasons_ok.append(f"No/low V3 liquidity sopra +{(settings.price_multiplier-1)*100:.0f}% (ticks={ticks_above})")
    else:
        reasons_ko.append(f"Troppa liquidità V3 sopra soglia (ticks={ticks_above})")

    # aggiorna state persistente
    state[cid] = {
        "last_checked": now_ts(),
        "status": "done",
        "top": extra.get("top_spot_cex"),
        "lp_pair": extra.get("lp_pair"),
        "ticks_above_threshold": ticks_above,
        "tick_threshold": tick_thr,
        "tick_now": tick_now,
        "p_now": p_now,
        "p_threshold": p_threshold,
    }

    return CheckResult(cid, symbol, name, reasons_ok, reasons_ko, extra)

# --------------------------- Markets seeds --------------------------- #

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

# --------------------------- Rendering --------------------------- #

def print_results(results: List[CheckResult]):
    all4 = [r for r in results if r.score() >= 4 and r.ok()]
    at_least3 = [r for r in results if r.score() >= 3]

    if RICH:
        if all4:
            t = Table(title="Coin che rispettano TUTTI e 4 i parametri")
            t.add_column("Symbol"); t.add_column("Name")
            t.add_column("Top Spot CEX"); t.add_column("LP (Dex)")
            t.add_column("LP Liquidity $"); t.add_column("Ticks sopra soglia")
            for r in all4:
                lp = r.extra.get("lp_pair", {})
                t.add_row(r.symbol, r.name,
                          str(r.extra.get("top_spot_cex")),
                          str(lp.get("dexId")), f"{lp.get('liquidityUsd')}",
                          str(r.extra.get("ticks_above_threshold")))
            console.print(t)
        else:
            console.print("[bold yellow]Nessuna coin che rispetta tutti e 4 i vincoli[/bold yellow]")

        if at_least3:
            t = Table(title="Coin che rispettano ALMENO 3 parametri")
            t.add_column("Symbol"); t.add_column("Name"); t.add_column("OK"); t.add_column("KO")
            for r in at_least3:
                t.add_row(r.symbol, r.name, "; ".join(r.reasons_ok), ", ".join(r.reasons_ko))
            console.print(t)
        else:
            console.print("[bold yellow]Nessuna coin con almeno 3 parametri[/bold yellow]")
    else:
        print("== Coin che rispettano TUTTI e 4 i parametri ==")
        for r in all4:
            lp = r.extra.get("lp_pair", {})
            print(f"{r.symbol:<8} | {r.name:<24} | topCEX={r.extra.get('top_spot_cex')} | LP={lp.get('dexId')} ${lp.get('liquidityUsd')} | ticksAbove={r.extra.get('ticks_above_threshold')}")
        print("== Coin che rispettano ALMENO 3 parametri ==")
        for r in at_least3:
            print(f"{r.symbol:6} | OK: {r.reasons_ok} | KO: {r.reasons_ko}")

# --------------------------- CLI --------------------------- #

def parse_args() -> Settings:
    ap = argparse.ArgumentParser(description="Crypto screener per CEX/DEX criteri custom (ottimizzato)")
    ap.add_argument("--pages", type=int, default=1)
    ap.add_argument("--per-page", type=int, default=250)
    ap.add_argument("--ids", type=str, default=None, help="Lista di ids CoinGecko separati da virgola (bypassa pages)")
    ap.add_argument("--workers", type=int, default=6, help="Numero di worker concorrenti")

    ap.add_argument("--min-liq", type=float, default=150000.0)
    ap.add_argument("--price-mult", type=float, default=3.0)
    ap.add_argument("--max-ticks-above", type=int, default=0)
    ap.add_argument("--require-v3", action="store_true")

    ap.add_argument("--dominance", type=float, default=0.60, help="Dominanza Bitget per short-circuit (0-1)")
    ap.add_argument("--max-tickers-scan", type=int, default=15)

    ap.add_argument("--ttl-cg", type=int, default=48*3600)
    ap.add_argument("--ttl-perps", type=int, default=7*24*3600)
    ap.add_argument("--ttl-ds", type=int, default=24*3600)
    ap.add_argument("--ttl-sg", type=int, default=24*3600)

    ap.add_argument("--dex-liq-delta", type=float, default=0.4, help="Soglia variazione liquidità per ricalcolare ticks")
    ap.add_argument("--skip-unchanged-days", type=int, default=3)

    ap.add_argument("--cache-root", type=str, default=".cache")
    ap.add_argument("--state-root", type=str, default="state")

    ap.add_argument("--rps-cg", type=float, default=1.0)
    ap.add_argument("--rps-ds", type=float, default=2.0)
    ap.add_argument("--rps-sg", type=float, default=1.0)

    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--no-verbose", dest="verbose", action="store_false")
    ap.set_defaults(verbose=True)
    args = ap.parse_args()

    s = Settings(
        pages=args.pages, per_page=args.per_page,
        ids=[x.strip() for x in args.ids.split(",")] if args.ids else None,
        workers=max(1, args.workers),

        min_pair_liquidity_usd=args.min_liq, price_multiplier=args.price_mult,
        max_ticks_above=args.max_ticks_above, require_v3=args.require_v3,

        max_tickers_scan=args.max_tickers_scan, bitget_dominance=args.dominance,

        ttl_cg_coin_sec=args.ttl_cg, ttl_perps_sec=args.ttl_perps,
        ttl_dexscreener_sec=args.ttl_ds, ttl_subgraph_sec=args.ttl_sg,

        dex_liq_delta_ratio=args.dex_liq_delta, skip_unchanged_days=args.skip_unchanged_days,

        cache_root=args.cache_root, state_root=args.state_root,
        verbose=args.verbose, quiet=args.quiet,

        rps_cg=max(0.2, args.rps_cg), rps_ds=max(0.5, args.rps_ds), rps_sg=max(0.2, args.rps_sg),
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
    sg_rl = RateLimiter(settings.rps_sg)

    # Perps (cache 7g)
    log("Scarico lista perps (cache-aware)...")
    binance_bases = load_binance_perp_bases_cached()
    bybit_bases = load_bybit_perp_bases_cached()
    log(f"Binance perps: {len(binance_bases)} | Bybit perps: {len(bybit_bases)}")

    # Seeds
    if settings.ids:
        id_syms = [(cid, "") for cid in settings.ids]
    else:
        log("Scarico lista CoinGecko markets (seed)...")
        id_syms = list_market_id_symbols(settings.pages, settings.per_page, cg_rl)
        log(f"Totale markets scaricati: {len(id_syms)}")

        # Pre-filtra per simbolo: mantieni SOLO quelli che hanno perps sia su Binance che Bybit
        inter = set(binance_bases.keys()) & set(bybit_bases.keys())
        before = len(id_syms)
        id_syms = [row for row in id_syms if (row[1] or "").upper() in inter]
        log(f"Candidati dopo filtro perps Binance&Bybit: {len(id_syms)} (da {before})")

    # Stato persistente
    state = load_state()

    # Concorrenza controllata
    results: List[CheckResult] = []
    tasks = []
    if RICH:
        prog = Progress("[progress.description]{task.description}",
                        BarColumn(), "[progress.percentage]{task.percentage:>3.0f}%",
                        TimeElapsedColumn())
        with prog:
            task_id = prog.add_task("Analisi coin", total=len(id_syms))
            with ThreadPoolExecutor(max_workers=settings.workers) as ex:
                futures = {
                    ex.submit(screen_coin, cid, binance_bases, bybit_bases, cg_rl, ds_rl, sg_rl, state): cid
                    for (cid, _sym) in id_syms
                }
                for fut in as_completed(futures):
                    prog.advance(task_id)
                    r = fut.result()
                    if r: results.append(r)
    else:
        with ThreadPoolExecutor(max_workers=settings.workers) as ex:
            futures = {
                ex.submit(screen_coin, cid, binance_bases, bybit_bases, cg_rl, ds_rl, sg_rl, state): cid
                for (cid, _sym) in id_syms
            }
            for fut in as_completed(futures):
                r = fut.result()
                if r: results.append(r)

    # Salva stato
    save_state(state)

    # Stampa
    print_results(results)

if __name__ == "__main__":
    main()
