#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crypto Screener + V3 Threshold Analyzer (merged)
------------------------------------------------
- Parte 1 (Screener): filtra coin che rispettano i 3 criteri
  1) Perpetual su ENTRAMBI: Binance Futures (USDT-M) e Bybit (linear)
  2) Top spot CEX = Bitget (euristica sui tickers CoinGecko)
  3) Contratto su BSC e main LP (maggiore liquidità) su PancakeSwap
     (opzione --require-v3 per limitare solo a Pancake v3)

- Parte 2 (Analyzer): per OGNI coin che passa il filtro sopra, esegue analisi
  su pool v3 (pair address) per stimare la QUANTITÀ di token1 necessaria a
  spingere il prezzo in su fino a +50%, +100%, +200% (fattori 1.5, 2.0, 3.0).

Output finale: una tabella con
  symbol, name, pair_address, liquidity_usd, market_cap_usd (se disponibile),
  token1_to_50, token1_to_100, token1_to_200

Note:
- Per il market cap si prova prima da Dexscreener (marketCap/fdv), altrimenti
  si tenta CoinGecko/markets.
- RPC BSC: default "https://bsc-dataseed.binance.org" o variabile BSC_RPC o --rpc.
- Richiede: requests, web3, (opzionale) pandas, tenacity, rich

MIT License (c) 2025

python yoge.py --require-v3 --min-liq 150000 --rpc https://bsc-dataseed.binance.org --save-csv results.csv

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

# rich è opzionale; l'output finale è comunque testo semplice
try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
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

    # Analyzer
    rpc_url: Optional[str] = None
    factors_str: str = "1.5,2.0,3.0"  # +50%, +100%, +200%
    save_csv: Optional[str] = None

# global
settings: Settings

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
    if RICH and console:
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

# fetch "full" (tickers ecc) — market_data=False per screener; market cap lo cerchiamo altrove

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

# /coins/list?include_platform=true  --> mappa SYMBOL -> id, preferendo chi ha BSC

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
    sym_up = symbol.upper()
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

# lock globale per lo stato (thread-safe)
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

    if not has_perps_on_both(binance_bases, bybit_bases, fr.symbol):
        with state_lock:
            state[cid] = {**st, "last_checked": now_ts(), "status": "no_perps"}
        return fr, None
    fr.s1_perps = True

    try:
        coin = fetch_coin_full(cid, cg_rl)
        fr.name = coin.get("name") or cid
    except Exception as e:
        with state_lock:
            state[cid] = {**st, "last_checked": now_ts(), "status": "cg_error"}
        return fr, None

    reasons_ok: List[str] = []
    reasons_ko: List[str] = []
    extra: Dict[str, Any] = {}

    bitget_ok, top_name = top_spot_cex_bitget_ok(coin, settings.bitget_dominance, settings.max_tickers_scan)
    extra["top_spot_cex"] = top_name
    fr.top_spot_cex = top_name
    if not bitget_ok:
        with state_lock:
            state[cid] = {**st, "last_checked": now_ts(), "status": "top_cex_not_bitget", "top": top_name}
        return fr, None
    fr.s2_bitget = True
    reasons_ok.append("Top spot CEX = Bitget")

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
    market_cap_ds = pair.get("marketCap") or pair.get("fdv")  # fallback FDV

    fr.s3_bsc_pancake = True
    fr.lp_pair = {
        "dexId": dexid, "pairAddress": pool_addr,
        "liquidityUsd": lp_liq, "priceUsd": pair.get("priceUsd"),
        "marketCap": market_cap_ds,
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
    inter_syms = sorted(set(binance_bases.keys()) & set(bybit_bases.keys()))
    if RICH and not settings.quiet:
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

    if RICH and not settings.quiet:
        t = Table(title="Mappatura Symbol → CoinGecko ID", box=box.SIMPLE_HEAVY)
        t.add_column("Tot Intersezione", justify="right")
        t.add_column("Mappati", justify="right")
        t.add_column("Non mappati", justify="right")
        t.add_row(str(len(inter_syms)), str(len(mapped)), str(len(unmapped)))
        console.print(t)
    else:
        log_info(f"Intersezione: {len(inter_syms)} | Mappati: {len(mapped)} | Non mappati: {len(unmapped)}")

    return mapped  # [(cg_id, symbol)]

# --------------------------- CG markets (market cap fallback) --------------------------- #

def cg_markets_cache_path(cid: str) -> str:
    return joinp(settings.cache_root, "coingecko", f"markets_{cid}.json")

def fetch_cg_markets_row(cid: str, cg_rl: RateLimiter) -> Optional[dict]:
    path = cg_markets_cache_path(cid)
    j = cache_get(path, settings.ttl_cg_coin_sec)
    if j is not None:
        return j
    # ricerca per singolo id — per semplicità scarichiamo la prima pagina generale e filtriamo
    url = f"{COINGECKO}/coins/markets?vs_currency=usd&ids={cid}&order=market_cap_desc&per_page=1&page=1&sparkline=false&locale=en"
    cg_rl.wait()
    try:
        data = Http.get(url).json()
        row = (data or [None])[0]
    except Exception:
        row = None
    if row:
        write_json(path, row)
    return row

# --------------------------- Formatter --------------------------- #

def format_eur_style_usd(amount: Any) -> str:
    try:
        iv = int(float(amount))
        return f"{iv:,}$".replace(",", ".")
    except Exception:
        return str(amount)

# --------------------------- Analyzer (V3 math) --------------------------- #

from decimal import Decimal, getcontext
import math
try:
    from web3 import Web3
    try:
        from web3.middleware import ExtraDataToPOAMiddleware  # web3.py v6
    except ImportError:
        ExtraDataToPOAMiddleware = None
    try:
        from web3.middleware import geth_poa_middleware        # web3.py v5
    except ImportError:
        geth_poa_middleware = None
except Exception:
    Web3 = None  # type: ignore
    ExtraDataToPOAMiddleware = None
    geth_poa_middleware = None

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from web3 import Web3 as Web3Type
else:
    # segnaposto runtime (Pylance ignora questo ramo)
    class Web3Type:  # type: ignore
        pass

getcontext().prec = 80
Q96 = 2 ** 96

UNISWAP_V3_POOL_MINI_ABI = [
    {
        "inputs": [],
        "name": "slot0",
        "outputs": [
            {"internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160"},
            {"internalType": "int24",   "name": "tick", "type": "int24"},
            {"internalType": "uint16",  "name": "observationIndex", "type": "uint16"},
            {"internalType": "uint16",  "name": "observationCardinality", "type": "uint16"},
            {"internalType": "uint16",  "name": "observationCardinalityNext", "type": "uint16"},
            {"internalType": "uint32",  "name": "feeProtocol", "type": "uint32"},
            {"internalType": "bool",    "name": "unlocked", "type": "bool"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "tickSpacing",
        "outputs": [{"internalType": "int24", "name": "", "type": "int24"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "int16", "name": "wordPosition", "type": "int16"}],
        "name": "tickBitmap",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "int24", "name": "tick", "type": "int24"}],
        "name": "ticks",
        "outputs": [
            {"internalType": "uint128", "name": "liquidityGross", "type": "uint128"},
            {"internalType": "int128", "name": "liquidityNet", "type": "int128"},
            {"internalType": "uint256", "name": "feeGrowthOutside0X128", "type": "uint256"},
            {"internalType": "uint256", "name": "feeGrowthOutside1X128", "type": "uint256"},
            {"internalType": "int56", "name": "tickCumulativeOutside", "type": "int56"},
            {"internalType": "uint160", "name": "secondsPerLiquidityOutsideX128", "type": "uint160"},
            {"internalType": "uint32", "name": "secondsOutside", "type": "uint32"},
            {"internalType": "bool", "name": "initialized", "type": "bool"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "liquidity",
        "outputs": [{"internalType": "uint128", "name": "", "type": "uint128"}],
        "stateMutability": "view",
        "type": "function",
    },
    {"inputs": [], "name": "token0", "outputs": [{"internalType": "address", "name": "", "type": "address"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "token1", "outputs": [{"internalType": "address", "name": "", "type": "address"}], "stateMutability": "view", "type": "function"},
]

ERC20_MINI_ABI = [
    {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "payable": False, "stateMutability": "view", "type": "function"},
    {"constant": True, "inputs": [], "name": "symbol",   "outputs": [{"name": "", "type": "string"}], "payable": False, "stateMutability": "view", "type": "function"},
]

ERC20_MINI_ABI_BYTES32 = [
    {"constant": True, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "bytes32"}], "payable": False, "stateMutability": "view", "type": "function"},
    {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "payable": False, "stateMutability": "view", "type": "function"},
]

@dataclass
class TokenMeta:
    address: str
    decimals: int
    symbol: str

@dataclass
class PoolSnapshot:
    sqrtPriceX96: int
    tick: int
    tickSpacing: int
    liquidity: int
    token0: TokenMeta
    token1: TokenMeta
    block_number: int
    block_timestamp: int

def _inject_poa(w3: Web3Type) -> None:
    # web3.py v6+
    if ExtraDataToPOAMiddleware is not None:
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        return
    # web3.py v5.x
    if geth_poa_middleware is not None:
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        return

def floor_div_neg(a: int, b: int) -> int:
    return a // b

def price_from_sqrtX96(sqrtX96: int, dec0: int, dec1: int) -> Decimal:
    sp = Decimal(sqrtX96) / Decimal(Q96)
    price = sp * sp * (Decimal(10) ** Decimal(dec1 - dec0))
    return price

def tick_from_price(price: Decimal, dec0: int, dec1: int) -> int:
    base = Decimal('1.0001')
    val = price * (Decimal(10) ** Decimal(dec0 - dec1))
    t = (val.ln() / base.ln())
    return math.floor(t)

def sqrt_unscaled_from_tick_delta(sqrt_curr_unscaled: Decimal, delta_tick: int) -> Decimal:
    base = Decimal('1.0001')
    exponent = Decimal(delta_tick) / Decimal(2)
    return sqrt_curr_unscaled * (base ** exponent)

def sqrtX96_from_unscaled(sqrt_unscaled: Decimal) -> int:
    return int((sqrt_unscaled * Decimal(Q96)).to_integral_value(rounding="ROUND_FLOOR"))

def scan_initialized_ticks(w3: Web3Type, pool, t0: int, t_target: int, spacing: int) -> List[int]:

    c0 = floor_div_neg(t0, spacing)
    cT = floor_div_neg(t_target, spacing)
    w0 = floor_div_neg(c0, 256)
    wT = floor_div_neg(cT, 256)
    in_range: List[int] = []
    direction = 1 if t_target >= t0 else -1

    if direction >= 0:
        word_range = range(w0, wT + 1)
    else:
        word_range = range(w0, wT - 1, -1)

    for w in word_range:
        bitmap = pool.functions.tickBitmap(w).call()
        if bitmap == 0:
            continue
        for b in range(256):
            if (bitmap >> b) & 1 == 1:
                c_i = w * 256 + b
                tick_i = c_i * spacing
                if direction >= 0:
                    if tick_i > t0 and tick_i <= t_target:
                        in_range.append(tick_i)
                else:
                    if tick_i <= t0 and tick_i > t_target:
                        in_range.append(tick_i)

    in_range = sorted(in_range) if direction >= 0 else sorted(in_range, reverse=True)
    return in_range

def fetch_liquidity_nets(w3: Web3Type, pool, tick_indices: List[int]) -> Dict[int, int]:

    liq_nets: Dict[int, int] = {}
    for ti in tick_indices:
        data = pool.functions.ticks(ti).call()
        liquidityNet = int(data[1])
        liq_nets[ti] = liquidityNet
    return liq_nets

def read_pool_snapshot(w3: Web3Type, pool_addr: str) -> PoolSnapshot:

    pool = w3.eth.contract(address=Web3.to_checksum_address(pool_addr), abi=UNISWAP_V3_POOL_MINI_ABI)
    sqrtPriceX96, tick, *_rest = pool.functions.slot0().call()
    tickSpacing = int(pool.functions.tickSpacing().call())
    liquidity = int(pool.functions.liquidity().call())
    token0_addr = pool.functions.token0().call()
    token1_addr = pool.functions.token1().call()

    c_str0 = w3.eth.contract(address=token0_addr, abi=ERC20_MINI_ABI)
    c_b320 = w3.eth.contract(address=token0_addr, abi=ERC20_MINI_ABI_BYTES32)
    c_str1 = w3.eth.contract(address=token1_addr, abi=ERC20_MINI_ABI)
    c_b321 = w3.eth.contract(address=token1_addr, abi=ERC20_MINI_ABI_BYTES32)

    def _read_meta(c_str, c_b32, addr) -> TokenMeta:
        decimals = int(c_str.functions.decimals().call())
        try:
            symbol = c_str.functions.symbol().call()
            if isinstance(symbol, (bytes, bytearray)):
                symbol = symbol.decode('utf-8', 'ignore').strip('\x00')
        except Exception:
            raw = c_b32.functions.symbol().call()
            symbol = raw.decode('utf-8', 'ignore').strip('\x00') if isinstance(raw, (bytes, bytearray)) else str(raw)
        return TokenMeta(address=addr, decimals=decimals, symbol=symbol or "TOKEN")

    tok0_meta = _read_meta(c_str0, c_b320, token0_addr)
    tok1_meta = _read_meta(c_str1, c_b321, token1_addr)

    latest_block = w3.eth.get_block("latest")

    snapshot = PoolSnapshot(
        sqrtPriceX96=int(sqrtPriceX96),
        tick=int(tick),
        tickSpacing=int(tickSpacing),
        liquidity=int(liquidity),
        token0=tok0_meta,
        token1=tok1_meta,
        block_number=int(latest_block.number),
        block_timestamp=int(latest_block.timestamp),
    )
    return snapshot

def integrate_token1_to_target(snapshot: PoolSnapshot, w3: Web3Type, pool, tick_target: int) -> Decimal:

    t0 = snapshot.tick
    if tick_target < t0:
        raise ValueError("Only upward moves supported in this integration.")

    spacing = snapshot.tickSpacing
    ticks_in_range = scan_initialized_ticks(w3, pool, t0, tick_target, spacing)
    liq_nets = fetch_liquidity_nets(w3, pool, ticks_in_range)

    sqrt_curr_unscaled = Decimal(snapshot.sqrtPriceX96) / Decimal(Q96)

    boundary_ticks = ticks_in_range.copy()
    L = int(snapshot.liquidity)
    token1_raw_total = 0

    all_stops: List[Tuple[str, int]] = [("tick", ti) for ti in boundary_ticks] + [("target", tick_target)]

    prev_sqrtX96 = snapshot.sqrtPriceX96
    for kind, stop_tick in all_stops:
        delta_tick = stop_tick - t0
        sqrt_stop_unscaled = sqrt_unscaled_from_tick_delta(sqrt_curr_unscaled, delta_tick)
        sqrt_stop_X96 = sqrtX96_from_unscaled(sqrt_stop_unscaled)
        delta_sqrtX96 = sqrt_stop_X96 - prev_sqrtX96
        if delta_sqrtX96 < 0:
            raise RuntimeError("Negative delta sqrt while moving upward.")
        seg_amount1_raw = (int(L) * int(delta_sqrtX96)) // Q96
        token1_raw_total += seg_amount1_raw
        if kind == "tick":
            L = L + int(liq_nets[stop_tick])
        prev_sqrtX96 = sqrt_stop_X96

    token1_human = Decimal(token1_raw_total) / (Decimal(10) ** Decimal(snapshot.token1.decimals))
    return token1_human

def compute_thresholds(snapshot: PoolSnapshot, w3: Web3Type, pool, factors: List[Decimal]) -> Dict[str, Tuple[Decimal, str]]:

    """Ritorna mappa {label: (amount_token1, token1_symbol)}"""
    res: Dict[str, Tuple[Decimal, str]] = {}
    # prezzo corrente (token1 per 1 token0)
    p0 = price_from_sqrtX96(snapshot.sqrtPriceX96, snapshot.token0.decimals, snapshot.token1.decimals)
    for f in factors:
        p_target = p0 * f
        t_target = tick_from_price(p_target, snapshot.token0.decimals, snapshot.token1.decimals)
        amt = integrate_token1_to_target(snapshot, w3, pool, t_target)
        label = f"+{int((f-1)*100)}%"
        res[label] = (amt, snapshot.token1.symbol)
    return res

# --------------------------- CLI --------------------------- #

def parse_args() -> Settings:
    ap = argparse.ArgumentParser(description="Crypto screener + thresholds su pool v3 (BSC/Pancake)")
    ap.add_argument("--pages", type=int, default=1)
    ap.add_argument("--per-page", type=int, default=250)
    ap.add_argument("--ids", type=str, default=None, help="Lista di ids CoinGecko separati da virgola (bypassa seed)")
    ap.add_argument("--workers", type=int, default=6, help="Numero di worker concorrenti")

    ap.add_argument("--min-liq", type=float, default=150000.0)
    ap.add_argument("--require-v3", action="store_true", help="Accetta solo LP Pancake v3")

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

    ap.add_argument("--funnel-show", type=int, default=30)
    ap.add_argument("--seed-from", type=str, choices=["perps","cg"], default="perps",
                    help="Origine seed: 'perps' (intersezione Binance∩Bybit) o 'cg' (coins/markets)")

    # Analyzer
    ap.add_argument("--rpc", type=str, default=None, help="BSC RPC URL (default env BSC_RPC o public)")
    ap.add_argument("--factors", type=str, default="1.5,2.0,3.0", help="Lista fattori es: 1.5,2.0,3.0 => +50,+100,+200")
    ap.add_argument("--save-csv", type=str, default=None, help="Se specificato, salva CSV con i risultati finali")

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

        rpc_url=args.rpc or os.environ.get("BSC_RPC") or "https://bsc-dataseed.binance.org",
        factors_str=args.factors,
        save_csv=args.save_csv,
    )
    return s

# --------------------------- Main --------------------------- #

def main():
    global settings
    settings = parse_args()

    # Forza modalità silenziosa per lo screener; stamperemo solo il report finale
    settings.quiet = True
    settings.verbose = False

    ensure_dir(settings.cache_root); ensure_dir(settings.state_root)

    # Rate limiters
    cg_rl = RateLimiter(settings.rps_cg)
    ds_rl = RateLimiter(settings.rps_ds)

    # Perps (cache 7g)
    binance_bases = load_binance_perp_bases_cached()
    bybit_bases   = load_bybit_perp_bases_cached()

    # Seeds
    if settings.ids:
        id_syms = [(cid, "") for cid in settings.ids]
    else:
        if settings.seed_from == "perps":
            id_syms = build_seed_from_perps(cg_rl, binance_bases, bybit_bases)  # [(cg_id, symbol)]
        else:
            id_syms = list_market_id_symbols(settings.pages, settings.per_page, cg_rl)
            inter = set(binance_bases.keys()) & set(bybit_bases.keys())
            id_syms = [row for row in id_syms if (row[1] or "").upper() in inter]

    # Stato persistente
    state = load_state()

    # Screener concorrente
    results: List[CheckResult] = []
    with ThreadPoolExecutor(max_workers=settings.workers) as ex:
        futures = {
            ex.submit(screen_coin, cid, sym, binance_bases, bybit_bases, cg_rl, ds_rl, state): cid
            for (cid, sym) in id_syms
        }
        for fut in as_completed(futures):
            _fr, res = fut.result()
            if res: results.append(res)

    # Salva stato screener
    save_state(state)

    # Filtra solo chi ha passato tutti e 3 i criteri
    passed = [r for r in results if r.score() >= 2 and r.ok() and r.extra.get("lp_pair")]

    # Se non ci sono pool, termina con messaggio minimale
    if not passed:
        print("Nessuna coin ha soddisfatto i 3 criteri.")
        return

    # Prepara web3 per l'analisi
    if Web3 is None:
        print("web3.py non disponibile: impossibile eseguire l'analisi dei thresholds. Installa 'web3'.")
        return

    w3 = Web3(Web3.HTTPProvider(settings.rpc_url, request_kwargs={"timeout": 30}))
    _inject_poa(w3)
    if not w3.is_connected():
        raise RuntimeError("Connessione RPC fallita. Fornisci una BSC RPC valida con --rpc o variabile BSC_RPC.")

    # Parsing fattori
    try:
        factors = [Decimal(x.strip()) for x in settings.factors_str.split(',') if x.strip()]
    except Exception:
        factors = [Decimal('1.5'), Decimal('2.0'), Decimal('3.0')]

    # Analizza ogni pool
    rows_out: List[Dict[str, Any]] = []

    for r in passed:
        lp = r.extra.get("lp_pair", {})
        pool_addr = lp.get("pairAddress")
        liq_usd = lp.get("liquidityUsd")
        mcap_usd = lp.get("marketCap")  # potrebbe essere None

        # fallback market cap da CG markets
        if mcap_usd in (None, 0):
            mr = fetch_cg_markets_row(r.id, cg_rl)
            if mr:
                mcap_usd = mr.get("market_cap")

        # Esegui lettura on-chain e stima soglie
        try:
            snapshot = read_pool_snapshot(w3, pool_addr)
            pool = w3.eth.contract(address=Web3.to_checksum_address(pool_addr), abi=UNISWAP_V3_POOL_MINI_ABI)
            thr = compute_thresholds(snapshot, w3, pool, factors)
        except Exception as e:
            thr = {}

        # Estrarre +50, +100, +200 (se i fattori sono standard). Altrimenti prendere quelli disponibili.
        def fmt_thr(label: str) -> str:
            if label in thr:
                amt, sym1 = thr[label]
                # stampa compatta con 6 decimali max
                q = amt.quantize(Decimal('0.000001')) if amt < Decimal('1000000') else amt.quantize(Decimal('1'))
                return f"{q} {sym1}"
            return "-"

        out_row = {
            "symbol": r.symbol,
            "name": r.name,
            "pair_address": pool_addr,
            "liquidity_usd": liq_usd,
            "market_cap_usd": mcap_usd,
            "to_50": fmt_thr("+50%"),
            "to_100": fmt_thr("+100%"),
            "to_200": fmt_thr("+200%"),
        }
        rows_out.append(out_row)

    # Stampa tabella testuale ordinata per liquidità decrescente
    rows_out.sort(key=lambda x: (x["liquidity_usd"] or 0), reverse=True)

    # Prova con pandas per una stampa pulita, ma degrada se non disponibile
    header = ["symbol","name","pair_address","liquidity_usd","market_cap_usd","to_50","to_100","to_200"]
    try:
        import pandas as pd
        df = pd.DataFrame(rows_out, columns=header)
        # formattazioni
        def _fmt_usd(v):
            return format_eur_style_usd(v) if pd.notnull(v) else "-"
        df["liquidity_usd"] = df["liquidity_usd"].apply(_fmt_usd)
        df["market_cap_usd"] = df["market_cap_usd"].apply(_fmt_usd)
        print("\n=== Risultati finali (TOP con thresholds) ===")
        print(df.to_string(index=False))
        if settings.save_csv:
            out_path = settings.save_csv
            df_raw = pd.DataFrame(rows_out, columns=header)  # salva i raw per CSV
            ensure_dir(os.path.dirname(out_path) or ".")
            df_raw.to_csv(out_path, index=False)
            print(f"\nSalvato CSV: {out_path}")
    except Exception:
        # fallback semplice CSV
        print(",".join(header))
        for row in rows_out:
            print(
                f"{row['symbol']},{row['name']},{row['pair_address']},{row['liquidity_usd'] or ''},{row['market_cap_usd'] or ''},{row['to_50']},{row['to_100']},{row['to_200']}"
            )

if __name__ == "__main__":
    main()
