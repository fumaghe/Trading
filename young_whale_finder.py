#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Young Whale Finder — BSC (Moralis-min, DexScreener-first) — STABLE TOKEN-FIRST
- Token-first scan (/erc20/:token/swaps) con filtro Pancake + pair hint
- Early-stop robusto e dinamico
- Balance SEMPRE aggiornato (no cache di default)
- First-tx via /wallets/{address}/history (affidabile) con cache 1y
- Filtro extra: escludi wallet con balance_usd < 1
- Assegnazione nome utente pseudo-random da file nomi.txt, con cache per address

Uso:
python young_whale_finder.py --token 0x48a18A4782b65a0fBed4dcA608BB28038B7bE339 --minutes 130 --min-usd 100 --young-days 170
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

# -------------------- Logging (su STDERR per tenere stdout pulito) -------------------- #
import logging
LOG_LEVEL = os.getenv("YWF_LOG", "info").upper()
LEVEL = {"DEBUG": logging.DEBUG, "INFO": logging.INFO, "WARNING": logging.WARNING, "WARN": logging.WARNING}.get(LOG_LEVEL, logging.INFO)

try:
    from rich.logging import RichHandler
    from rich.console import Console
    logging.basicConfig(
        level=LEVEL,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=Console(file=sys.stderr), rich_tracebacks=False)]
    )
    RICH = True
except Exception:
    logging.basicConfig(level=LEVEL, format="%(asctime)s | %(levelname)s | %(message)s", stream=sys.stderr)
    RICH = False

log = logging.getLogger("ywf")

# -------------------- Config (stable defaults) -------------------- #

# Moralis
MORALIS_API_KEY = os.getenv("MORALIS_API_KEY", "").strip() or "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub25jZSI6Ijg3YTE1YmJmLWY5NmItNDY5ZS04MWViLTUyMWExNWUxNWU2NSIsIm9yZ0lkIjoiMzU5OTMzIiwidXNlcklkIjoiMzY5OTEzIiwidHlwZUlkIjoiYjVkN2Q2YjctNGM4ZC00NjRhLWIyNGMtMjU2MTk0NzJmNGE5IiwidHlwZSI6IlBST0pFQ1QiLCJpYXQiOjE3NjI1MzAzMDUsImV4cCI6NDkxODI5MDMwNX0.YPGGVEguNqhoy-5bE0k-3BBhMdvKNWxorjh4HFdgh1I"
MORALIS_BASE = "https://deep-index.moralis.io/api/v2.2"

# Free API (DexScreener)
DEXSCREENER_TOKEN = "https://api.dexscreener.com/latest/dex/tokens/{address}"

CHAIN = "bsc"

# Defaults
YOUNG_DAYS_DEFAULT = int(os.getenv("YWF_YOUNG_DAYS", "3"))
RPS = float(os.getenv("YWF_RPS", "2"))
HTTP_TIMEOUT = 25
UA = "Mozilla/5.0 (YoungWhaleFinder/1.9-STABLE; +https://moralis.com)"

TOP_K = int(os.getenv("YWF_TOPK", "20"))
TARGET_WALLETS = int(os.getenv("YWF_TARGET_WALLETS", "22"))

MAX_SWAPS = int(os.getenv("YWF_MAX_SWAPS", "2000"))
BUDGET_CALLS = int(os.getenv("YWF_BUDGET_CALLS", "60"))
WINDOW_SLACK_SEC = int(os.getenv("YWF_WINDOW_SLACK", "90"))

if os.getenv("YWF_FAST", "1") == "1":
    TOP_K = max(8, min(TOP_K, 20))
    MAX_SWAPS = max(300, min(MAX_SWAPS, 1200))

SKIP_DATE_TO_BLOCK = os.getenv("YWF_NO_DTB", "1") == "1"

# Cache
CACHE_PATH = os.getenv("YWF_CACHE", "ywf_cache.json")
TTL_BLOCK = 600
TTL_PRICE_MORALIS = 120
TTL_METADATA = 30 * 86400
TTL_FIRST_TX = 365 * 86400
TTL_BALANCE = int(os.getenv("YWF_BALANCE_TTL", "0"))

# Names file
NAMES_FILE = os.getenv("YWF_NAMES_FILE", "nomi.txt")

# -------------------- Utils & Cache -------------------- #

class SimpleRate:
    def __init__(self, rps: float):
        self.min_interval = 1.0 / max(0.001, rps)
        self.last = 0.0
    def wait(self):
        now = time.time()
        if self.last:
            delta = now - self.last
            if delta < self.min_interval:
                time.sleep(self.min_interval - delta)
        self.last = time.time()

http_rl = SimpleRate(RPS)

class DiskCache:
    def __init__(self, path: str):
        self.path = path
        self.data = {"date_to_block":{}, "metadata":{}, "price_moralis":{},
                     "first_tx_ts":{}, "balance":{}, "names":{}}
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    j = json.load(f)
                    if isinstance(j, dict):
                        self.data.update(j)
            log.debug(f"Cache loaded from {path}")
        except Exception as e:
            log.warning(f"Cache load failed ({e}); starting empty.")
        self.hits = 0
        self.misses = 0
    def get(self, section: str, key: str, ttl: Optional[int] = None):
        sec = self.data.get(section, {})
        rec = sec.get(key)
        if not rec:
            self.misses += 1
            return None
        if ttl is not None:
            ts = rec.get("_ts")
            if not ts or (time.time() - ts) > ttl:
                self.misses += 1
                return None
        self.hits += 1
        return rec.get("value")
    def set(self, section: str, key: str, value):
        if section not in self.data:
            self.data[section] = {}
        self.data[section][key] = {"_ts": time.time(), "value": value}
    def save(self):
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
            log.debug(f"Cache saved to {self.path}")
        except Exception as e:
            log.warning(f"Cache save failed: {e}")

CACHE = DiskCache(CACHE_PATH)

# -------------------- Names helpers -------------------- #

def load_names_list(path: str) -> List[str]:
    """Carica la lista nomi da file; fallback a una lista di default."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            names = [line.strip() for line in f if line.strip()]
        if names:
            return names
    except Exception as e:
        log.warning(f"Impossibile leggere {path}: {e}. Uso lista nomi di default.")
    return ["Topolino", "Paperino", "Pippo", "Minnie", "Paperina", "Pluto"]

def get_or_assign_name(addr: str, names: List[str]) -> str:
    current = CACHE.get("names", addr, ttl=None)
    if current is not None:
        return str(current)

    raw = CACHE.data.get("names") or {}
    used = set()
    for v in raw.values():
        used.add(v.get("value") if isinstance(v, dict) else v)

    available = [n for n in names if n not in used]
    if available:
        candidate = random.SystemRandom().choice(available)
    else:
        base = random.SystemRandom().choice(names)
        i = 2
        candidate = base
        while candidate in used:
            candidate = f"{base}{i}"
            i += 1

    CACHE.set("names", addr, candidate)
    return candidate

# -------------------- HTTP & Budget -------------------- #

class HttpError(RuntimeError):
    def __init__(self, msg: str, status: Optional[int] = None, retryable: bool = False):
        super().__init__(msg)
        self.status = status
        self.retryable = retryable

class BudgetExhausted(RuntimeError):
    pass

MORALIS_CALLS = 0
MORALIS_SAVED_BY_CACHE = 0

def _moralis_headers() -> dict:
    if not MORALIS_API_KEY or MORALIS_API_KEY == "CHANGE_ME":
        raise RuntimeError("Manca MORALIS_API_KEY (env var o costante nel file).")
    return {"accept":"application/json","user-agent":UA,"X-API-Key":MORALIS_API_KEY}

def _retry_if_retryable(e: Exception) -> bool:
    return isinstance(e, HttpError) and getattr(e, "retryable", False)

@retry(stop=stop_after_attempt(4), wait=wait_exponential(1, 1, 8),
       retry=retry_if_exception(_retry_if_retryable))
def moralis_get(path: str, params: dict | None = None) -> dict:
    global MORALIS_CALLS
    if MORALIS_CALLS >= BUDGET_CALLS:
        raise BudgetExhausted(f"Budget Moralis esaurito: {MORALIS_CALLS}/{BUDGET_CALLS} chiamate")
    http_rl.wait()
    url = f"{MORALIS_BASE}{path}"
    r = requests.get(url, headers=_moralis_headers(), params=params or {}, timeout=HTTP_TIMEOUT)
    if r.status_code >= 400:
        retryable = (r.status_code == 429) or (500 <= r.status_code < 600)
        try:
            err = r.json()
        except Exception:
            err = {"status": r.status_code, "text": r.text[:200]}
        raise HttpError(f"GET {path} -> {r.status_code}: {err}", status=r.status_code, retryable=retryable)
    MORALIS_CALLS += 1
    return r.json()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(1, 0.5, 4),
       retry=retry_if_exception(lambda e: isinstance(e, HttpError) and e.retryable))
def http_get(url: str, params: dict | None = None, headers: dict | None = None) -> dict:
    http_rl.wait()
    r = requests.get(url, params=params or {}, headers=headers or {"accept":"application/json","user-agent":UA}, timeout=HTTP_TIMEOUT)
    if r.status_code >= 400:
        retryable = (r.status_code == 429) or (500 <= r.status_code < 600)
        try:
            err = r.json()
        except Exception:
            err = {"status": r.status_code, "text": r.text[:200]}
        raise HttpError(f"GET {url} -> {r.status_code}: {err}", status=r.status_code, retryable=retryable)
    return r.json()

# -------------------- Helpers -------------------- #

def _first_key(d: dict, keys: List[str], default=None):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default

def _as_float(v) -> Optional[float]:
    if v is None: return None
    try: return float(v)
    except Exception:
        try: return float(str(v).replace(",", ""))
        except Exception: return None

# -------------------- Moralis helpers (cached) -------------------- #

def date_to_block_cached(dt_iso: str) -> int:
    k = dt_iso
    v = CACHE.get("date_to_block", k, ttl=TTL_BLOCK)
    if v is not None:
        global MORALIS_SAVED_BY_CACHE
        MORALIS_SAVED_BY_CACHE += 1
        log.debug(f"[cache] dateToBlock({dt_iso}) -> {v}")
        return int(v)
    j = moralis_get("/dateToBlock", params={"chain": CHAIN, "date": dt_iso})
    for f in ("block","block_number","blockNumber"):
        if f in j:
            try:
                block = int(j[f]); CACHE.set("date_to_block", k, block)
                log.debug(f"[net] dateToBlock({dt_iso}) -> {block}")
                return block
            except Exception: ...
    raise RuntimeError(f"dateToBlock senza block valido: {j}")

def get_token_metadata_cached(token: str) -> Tuple[str, int]:
    v = CACHE.get("metadata", token, ttl=TTL_METADATA)
    if v is not None:
        global MORALIS_SAVED_BY_CACHE
        MORALIS_SAVED_BY_CACHE += 1
        sym, dec = v
        log.debug(f"[cache] metadata({token}) -> {sym}/{dec}")
        return sym, int(dec)
    j = moralis_get("/erc20/metadata", params={"chain": CHAIN, "addresses":[token]})
    arr = j if isinstance(j, list) else j.get("result") or j.get("tokens") or j.get("items") or []
    if not arr:
        sym, dec = "TOKEN", 18
    else:
        m = arr[0]; sym = m.get("symbol") or "TOKEN"
        try: dec = int(m.get("decimals", 18))
        except Exception: dec = 18
    CACHE.set("metadata", token, [sym, dec]); log.debug(f"[net] metadata({token}) -> {sym}/{dec}")
    return sym, dec

def get_token_price_usd_moralis_cached(token: str) -> Optional[float]:
    v = CACHE.get("price_moralis", token, ttl=TTL_PRICE_MORALIS)
    if v is not None:
        global MORALIS_SAVED_BY_CACHE
        MORALIS_SAVED_BY_CACHE += 1
        log.debug(f"[cache] price_moralis({token}) -> {v}")
        return _as_float(v)
    j = moralis_get(f"/erc20/{token}/price", params={"chain": CHAIN})
    price = _as_float(_first_key(j, ["usdPrice","priceUsd","usd_price","price_usd"]))
    CACHE.set("price_moralis", token, price); log.debug(f"[net] price_moralis({token}) -> {price}")
    return price

# -------------------- DexScreener (FREE) -------------------- #

def dexscreener_pick_pancake_and_activity(token: str):
    """
    Ritorna: (main_pair, price_usd, m5_buys, m5_sells, liq_usd). Tutto FREE.
    """
    try:
        j = http_get(DEXSCREENER_TOKEN.format(address=token))
    except Exception as e:
        log.warning(f"DexScreener fetch failed: {e}")
        return None, None, None, None, None
    pairs = j.get("pairs") or []
    best = None; best_liq = -1.0
    for p in pairs:
        if (p.get("chainId") or "").lower() != "bsc": continue
        if "pancake" not in (p.get("dexId","{}").lower()): continue
        liq = _as_float(((p.get("liquidity") or {}).get("usd"))); liq = 0.0 if liq is None else liq
        if liq > best_liq:
            best_liq = liq; best = p
    if not best:
        return None, None, None, None, None
    pair = (best.get("pairAddress") or "").strip()
    price = _as_float(best.get("priceUsd"))
    txns = best.get("txns") or {}
    m5 = txns.get("m5") or {}
    buys_5 = int(m5.get("buys") or 0); sells_5 = int(m5.get("sells") or 0)
    log.info(f"DexScreener main pair: {pair} | price={price} | m5 buys={buys_5} sells={m5.get('sells',0)} | liq=${best_liq}")
    return pair or None, price, buys_5, sells_5, best_liq

# -------------------- Filters & USD calc -------------------- #

PANCAKE_TXT_FIELDS = ("exchangeName","exchange","dex","dexName","factoryName","label","market","dexId")
PAIR_FIELDS = ("pairAddress","poolAddress","liquidityPoolAddress","lpAddress","pair","pool")

def is_pancake_swap(s: dict, main_pair_hint: Optional[str]) -> bool:
    txt = " ".join(str(s.get(k) or "").lower() for k in PANCAKE_TXT_FIELDS)
    if "pancake" in txt:
        if main_pair_hint:
            pa = None
            for k in PAIR_FIELDS:
                if s.get(k): pa = s.get(k); break
            if isinstance(pa, str) and pa.strip().lower() == main_pair_hint.strip().lower():
                return True
            if pa is None:
                return True
        else:
            return True
    if main_pair_hint:
        pa = None
        for k in PAIR_FIELDS:
            if s.get(k): pa = s.get(k); break
        if isinstance(pa, str) and pa.strip().lower() == main_pair_hint.strip().lower():
            return True
    return False

def calc_usd_from_swap(s: dict, price_usd: Optional[float]) -> Optional[float]:
    for k in ("totalValueUsd","valueUsd","usdValue","amountUsd","usd_amount","quoteAmountUsd","priceUsd"):
        v = _as_float(s.get(k))
        if v is not None:
            return v
    if price_usd is None:
        return None
    for k in ("tokenAmount","amountToken","token_amount","amount","value","toTokenAmount","buyAmount","amount0In","amount1In","amountIn"):
        v = _as_float(s.get(k))
        if v is not None:
            return v * float(price_usd)
    return None

# -------------------- Swaps iterator (TOKEN-FIRST) -------------------- #

def iterate_swaps_token_first(token: str, main_pair: Optional[str], from_ts: int, to_ts: int):
    cursor = None
    page = 0
    consecutive_old_pages = 0
    while True:
        params = {"chain": CHAIN, "order": "DESC", "limit": 100, "transactionTypes": "buy"}
        if cursor: params["cursor"] = cursor

        try:
            j = moralis_get(f"/erc20/{token}/swaps", params=params)
        except BudgetExhausted as e:
            log.warning(str(e)); return

        result = j.get("result") or []
        page += 1
        log.debug(f"[token page {page}] swaps: {len(result)} (cursor={'yes' if j.get('cursor') else 'no'})")

        page_all_before = True
        any_in_window = False

        for s in result:
            ts_iso = _first_key(s, ["block_timestamp","blockTimestamp"])
            ts_ok = True
            if ts_iso:
                try:
                    ts = int(datetime.fromisoformat(str(ts_iso).replace("Z","+00:00")).timestamp())
                    ts_ok = (from_ts <= ts <= to_ts)
                    if ts >= from_ts: page_all_before = False
                    if ts_ok: any_in_window = True
                except Exception:
                    page_all_before = False; any_in_window = True; ts_ok = True
            if not ts_ok:
                continue
            if not is_pancake_swap(s, main_pair):
                continue
            yield s

        if any_in_window:
            consecutive_old_pages = 0
        elif page_all_before:
            consecutive_old_pages += 1
            if consecutive_old_pages >= 2:
                log.info("Early-stop: 2 pagine consecutive tutte < from_ts (più vecchie della finestra).")
                return

        cursor = j.get("cursor") or None
        if not cursor:
            return

# -------------------- Enrichment (Moralis) -------------------- #

def get_wallet_first_tx_ts_cached(addr: str) -> Optional[int]:
    v = CACHE.get("first_tx_ts", addr, ttl=TTL_FIRST_TX)
    if v is not None:
        global MORALIS_SAVED_BY_CACHE
        MORALIS_SAVED_BY_CACHE += 1
        log.debug(f"[cache] first_tx({addr}) -> {v}")
        return int(v) if v is not None else None
    j = moralis_get(f"/wallets/{addr}/history", params={"chain": CHAIN, "order":"ASC", "limit":1, "from_date":"1970-01-01T00:00:00Z"})
    res = j.get("result") or []
    ts_out = None
    if res:
        ts = _first_key(res[0], ["block_timestamp","blockTimestamp"])
        try: ts_out = int(datetime.fromisoformat(str(ts).replace("Z","+00:00")).timestamp())
        except Exception: ts_out = None
    CACHE.set("first_tx_ts", addr, ts_out); log.debug(f"[net] first_tx({addr}) -> {ts_out}")
    return ts_out

def get_wallet_token_balance_fresh(addr: str, token: str, decimals: int) -> float:
    try:
        j = moralis_get(f"/{addr}/erc20", params={"chain": CHAIN, "token_addresses":[token]})
        arr = j if isinstance(j, list) else j.get("result") or j.get("items") or []
    except Exception:
        j = moralis_get(f"/{addr}/erc20", params={"chain": CHAIN})
        arr = j if isinstance(j, list) else j.get("result") or j.get("items") or []
    bal_raw = 0
    for t in arr:
        ta = (_first_key(t, ["token_address","tokenAddress","address","tokenAddressHash"], "") or "").lower()
        if ta == token.lower():
            try: bal_raw = int(t.get("balance") or 0)
            except Exception: bal_raw = 0
            break
    bal = bal_raw / (10 ** decimals)
    if TTL_BALANCE > 0:
        CACHE.set("balance", f"{addr}:{token}", bal)
    log.debug(f"[net] balance({addr}) -> {bal}")
    return bal

# -------------------- CLI -------------------- #

def parse_args():
    ap = argparse.ArgumentParser(description="Young Whale Finder (BSC; Moralis-min; DexScreener-first; TOKEN-FIRST)")
    ap.add_argument("--token", required=True, help="Token address (BSC)")
    ap.add_argument("--minutes", type=int, default=5, help="Finestra in minuti")
    ap.add_argument("--min-usd", type=float, default=500.0, help="Soglia minima USD per BUY")
    ap.add_argument("--young-days", type=int, default=YOUNG_DAYS_DEFAULT, help="Filtra wallet più giovani di N giorni")
    return ap.parse_args()

# -------------------- Main -------------------- #

def main():
    t0 = time.time()
    args = parse_args()
    token = args.token
    minutes = max(1, int(args.minutes))
    min_usd = max(0.0, float(args.min_usd))
    YOUNG_DAYS = max(0, int(args.young_days))

    log.info(f"Start | token={token} minutes={minutes} min_usd={min_usd} young_days={YOUNG_DAYS} | TOP_K={TOP_K} MAX_SWAPS={MAX_SWAPS} budget_calls={BUDGET_CALLS} log={LOG_LEVEL.lower()}")

    now = datetime.now(timezone.utc)
    from_dt = now - timedelta(minutes=minutes)
    from_iso = from_dt.isoformat().replace("+00:00", "Z")
    to_iso   = now.isoformat().replace("+00:00", "Z")
    from_ts = int(from_dt.timestamp()); to_ts = int(now.timestamp()) + WINDOW_SLACK_SEC

    sym, dec = get_token_metadata_cached(token)
    main_pair, ds_price, m5_buys, m5_sells, liq = dexscreener_pick_pancake_and_activity(token)
    price_usd = ds_price; price_source = "dexscreener"

    if price_usd is None:
        try:
            price_usd = get_token_price_usd_moralis_cached(token)
            price_source = "moralis"
        except BudgetExhausted as e:
            log.warning(str(e))
            price_usd = None
            price_source = "none"

    if SKIP_DATE_TO_BLOCK:
        from_blk = None; to_blk = None
        log.info("NO_DTB attivo: timestamp-only + early-stop.")
    else:
        try:
            from_blk = date_to_block_cached(from_iso)
            to_blk   = date_to_block_cached(to_iso)
        except BudgetExhausted as e:
            log.warning(str(e)); from_blk = None; to_blk = None
        except Exception as e:
            log.warning(f"dateToBlock failed ({e}); fallback timestamp-only.")
            from_blk = None; to_blk = None

    log.info(f"Token: {sym} (dec={dec}) | price={price_usd} [{price_source}] | window {from_iso} → {to_iso}")

    TK = TOP_K
    MAX = MAX_SWAPS
    if m5_buys is not None and m5_buys >= 0:
        est_per_min = max(1, int(round(m5_buys / 5)))
        dyn_cap = max(300, min(MAX_SWAPS, est_per_min * minutes * 120))
        dyn_topk = max(8, min(TOP_K, 5 + est_per_min * minutes))
        if dyn_cap < MAX or dyn_topk < TK:
            log.info(f"Dynamic limits: MAX_SWAPS {MAX}→{dyn_cap}, TOP_K {TK}→{dyn_topk}")
            MAX = dyn_cap; TK = dyn_topk

    buyers_sum_usd: Dict[str, float] = {}
    total_swaps = total_swaps_pancake = total_buys_ge_min = 0
    c_not_buy = c_not_pancake = c_below_min = c_no_wallet = 0

    last_top_wallets: List[str] = []
    stable_pages = 0

    iterator = iterate_swaps_token_first(token, main_pair, from_ts, to_ts)

    try:
        for s in iterator:
            total_swaps += 1

            tx_type = str(_first_key(s, ["transactionType","type","side","action"], "")).lower()
            if tx_type != "buy":
                c_not_buy += 1
                if total_swaps >= MAX: break
                continue

            total_swaps_pancake += 1

            usd_val = calc_usd_from_swap(s, price_usd)
            if usd_val is None or usd_val < min_usd:
                c_below_min += 1
                if total_swaps >= MAX: break
                continue
            total_buys_ge_min += 1

            wallet = _first_key(s, ["walletAddress","trader","maker","sender","fromAddress","from","buyer","recipient","toAddress","to"])
            if not wallet:
                c_no_wallet += 1
                if total_swaps >= MAX: break
                continue

            buyers_sum_usd[str(wallet)] = buyers_sum_usd.get(str(wallet), 0.0) + float(usd_val)

            if total_swaps % 200 == 0:
                current_top = [w for w,_ in sorted(buyers_sum_usd.items(), key=lambda kv: kv[1], reverse=True)[:TK]]
                if current_top == last_top_wallets:
                    stable_pages += 1
                else:
                    stable_pages = 0
                    last_top_wallets = current_top
                if stable_pages >= 2:
                    log.info("Early-stop: TOP-K stabile, fermo la scansione.")
                    break

            if len(buyers_sum_usd) >= TARGET_WALLETS and total_swaps % 100 == 0:
                log.info(f"Early-stop: raggiunto TARGET_WALLETS={TARGET_WALLETS}.")
                break

            if total_swaps >= MAX:
                log.warning(f"Reached MAX_SWAPS={MAX}, stopping swaps scan.")
                break

    except BudgetExhausted as e:
        log.warning(str(e))

    log.info(f"Swaps scanned={total_swaps} | pancake={total_swaps_pancake} | buys≥min={total_buys_ge_min} | wallets={len(buyers_sum_usd)}")
    log.debug(f"Filtered: not_buy={c_not_buy} not_pancake={c_not_pancake} below_min={c_below_min} no_wallet={c_no_wallet}")

    top_wallets = sorted(buyers_sum_usd.items(), key=lambda kv: kv[1], reverse=True)[:TK]
    log.info(f"Evaluating first_tx + balance for TOP_K={len(top_wallets)} (of {len(buyers_sum_usd)})")

    names_list = load_names_list(NAMES_FILE)

    young_cutoff_ts = int(now.timestamp()) - YOUNG_DAYS * 86400
    results: List[dict] = []
    kept = skipped_old = 0

    for addr, sum_usd in top_wallets:
        try:
            first_ts = get_wallet_first_tx_ts_cached(addr)
        except BudgetExhausted as e:
            log.warning(str(e)); break
        if (first_ts is not None) and (first_ts <= young_cutoff_ts):
            skipped_old += 1
            log.debug(f"skip old wallet {addr} (first_tx={first_ts})")
            continue

        try:
            bal_tok = get_wallet_token_balance_fresh(addr, token, dec)
        except BudgetExhausted as e:
            log.warning(str(e)); break

        bal_usd = (price_usd or 0.0) * bal_tok if price_usd is not None else None

        if bal_usd is None or bal_usd < 1.0:
            log.debug(f"skip low balance {addr} (balance_usd={bal_usd})")
            continue

        user_name = get_or_assign_name(addr, names_list)

        results.append({
            "address": addr,
            "user_name": user_name,
            "balance_token": bal_tok,
            "balance_usd": bal_usd,
            "first_tx_ts": first_ts,
            "sum_buys_usd_window": sum_usd,
        })
        kept += 1

    results.sort(key=lambda r: (r["balance_usd"] if r["balance_usd"] is not None else -1), reverse=True)

    out = {
        "chain": CHAIN,
        "token": token,
        "symbol": sym,
        "decimals": dec,
        "price_usd": price_usd,
        "price_source": "dexscreener" if price_source=="dexscreener" else price_source,
        "window_minutes": minutes,
        "min_usd": min_usd,
        "young_days": YOUNG_DAYS,
        "from_block": None if SKIP_DATE_TO_BLOCK else (None if 'from_blk' not in locals() else from_blk),
        "to_block": None if SKIP_DATE_TO_BLOCK else (None if 'to_blk' not in locals() else to_blk),
        "from_iso": from_iso,
        "to_iso": to_iso,
        "main_pair_hint": main_pair,
        "stats": {
            "total_swaps_scanned": total_swaps,
            "total_swaps_pancake": total_swaps_pancake,
            "buys_ge_min_usd": total_buys_ge_min,
            "wallet_unici_ge_min_usd": len(buyers_sum_usd),
            "wallet_valutati_topk": len(top_wallets),
            "wallet_tenuti": kept,
            "wallet_scartati_old": skipped_old,
        },
        "results": results,
    }

    CACHE.save()
    cu_report = {
        "moralis_calls": MORALIS_CALLS,
        "avoid_calls_by_cache": MORALIS_SAVED_BY_CACHE,
        "cache_hits": CACHE.hits, "cache_misses": CACHE.misses,
        "elapsed_sec": round(time.time()-t0, 2),
        "budget_calls": BUDGET_CALLS,
    }
    log.info(f"CU report: {cu_report}")
    print(json.dumps(out, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
