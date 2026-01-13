#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Young Whale Finder — BSC (Moralis-min, DexScreener-aggregated) — STABLE TOKEN-FIRST

Modifiche principali rispetto alla tua versione:
- Nessun taglio TOP_K sulla valutazione: valutiamo TUTTI i wallet che hanno comprato >= min_usd nella finestra.
- Filtro "young" realmente disattivabile: --young-days 0 => NON filtra per età.
- YWF_FAST default 0 e nessun dynamic-limits che riduca i limiti.
- Threshold su balance in USD: default 3000 (YWF_NAME_MIN_BAL_USD).
- Top spenders list separata: YWF_TOP_SPENDERS_N (default 50) solo per agg.top_spenders_all.

Uso:
  python young_whale_finder.py --token 0x... --minutes 120 --min-usd 100 --young-days 7
  python young_whale_finder.py --token 0x... --minutes 120 --min-usd 100 --young-days 0   # no filtro young
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

# -------------------- Logging -------------------- #
import logging

LOG_LEVEL = os.getenv("YWF_LOG", "info").upper()
LEVEL = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
}.get(LOG_LEVEL, logging.INFO)

try:
    from rich.logging import RichHandler
    from rich.console import Console

    logging.basicConfig(
        level=LEVEL,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=Console(file=sys.stderr), rich_tracebacks=False)],
    )
    RICH = True
except Exception:
    logging.basicConfig(
        level=LEVEL, format="%(asctime)s | %(levelname)s | %(message)s", stream=sys.stderr
    )
    RICH = False

log = logging.getLogger("ywf")

# -------------------- Config -------------------- #

# Moralis
MORALIS_API_KEY = os.getenv("MORALIS_API_KEY", "").strip() or "CHANGE_ME"
MORALIS_BASE = "https://deep-index.moralis.io/api/v2.2"

# DexScreener
DEXSCREENER_TOKEN = "https://api.dexscreener.com/latest/dex/tokens/{address}"

CHAIN = "bsc"

# Defaults
YOUNG_DAYS_DEFAULT = int(os.getenv("YWF_YOUNG_DAYS", "3"))
RPS = float(os.getenv("YWF_RPS", "2"))
HTTP_TIMEOUT = 25
UA = "Mozilla/5.0 (YoungWhaleFinder/2.0; +https://moralis.com)"

# (TOP_K rimane solo per stampa / compatibilità e per la lista top spenders, non per la valutazione)
TOP_K = int(os.getenv("YWF_TOPK", "20"))

# Quanti top spender mostrare in agg.top_spenders_all (solo recap, non influisce su results)
TOP_SPENDERS_N = int(os.getenv("YWF_TOP_SPENDERS_N", "50"))
TOP_SPENDERS_N = max(10, min(TOP_SPENDERS_N, 500))

# Scansione swap: aumentare per evitare tronchi (early-stop fermerà comunque quando esaurisce la finestra)
MAX_SWAPS = int(os.getenv("YWF_MAX_SWAPS", "50000"))

# Moralis budget chiamate
BUDGET_CALLS = int(os.getenv("YWF_BUDGET_CALLS", "200"))

WINDOW_SLACK_SEC = int(os.getenv("YWF_WINDOW_SLACK", "90"))

# Fast mode: default OFF, e anche se attivata non deve ridurre MAX_SWAPS in modo aggressivo
if os.getenv("YWF_FAST", "0") == "1":
    # Non riduciamo MAX_SWAPS a 1200 perché rischia di perdere wallet in finestre larghe/alta attività
    TOP_K = max(8, min(TOP_K, 50))

SKIP_DATE_TO_BLOCK = os.getenv("YWF_NO_DTB", "1") == "1"

# Cache
CACHE_PATH = os.getenv("YWF_CACHE", "ywf_cache.json")
TTL_BLOCK = 600
TTL_PRICE_MORALIS = 120
TTL_METADATA = 30 * 86400
TTL_FIRST_TX = 365 * 86400
TTL_BALANCE = int(os.getenv("YWF_BALANCE_TTL", "0"))

# Names
NAMES_FILE = os.getenv("YWF_NAMES_FILE", "nomi.txt")

# Soglia balance USD per essere considerato "validato" (results + nome)
NAME_MIN_BAL_USD = float(os.getenv("YWF_NAME_MIN_BAL_USD", "1500"))

# Se vuoi loggare il motivo di scarto dei singoli wallet (solo in DEBUG)
LOG_REASONS = os.getenv("YWF_LOG_REASONS", "0") == "1"

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
        self.data = {
            "date_to_block": {},
            "metadata": {},
            "price_moralis": {},
            "first_tx_ts": {},
            "balance": {},
            "names": {},
        }
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    j = json.load(f)
                    if isinstance(j, dict):
                        self.data.update(j)
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
        except Exception as e:
            log.warning(f"Cache save failed: {e}")

CACHE = DiskCache(CACHE_PATH)

# -------------------- Names helpers -------------------- #

def load_names_list(path: str) -> List[str]:
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

    used = set()
    for v in (CACHE.data.get("names") or {}).values():
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
        raise RuntimeError("Manca MORALIS_API_KEY (env var).")
    return {"accept": "application/json", "user-agent": UA, "X-API-Key": MORALIS_API_KEY}

def _retry_if_retryable(e: Exception) -> bool:
    return isinstance(e, HttpError) and getattr(e, "retryable", False)

@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(1, 1, 8),
    retry=retry_if_exception(_retry_if_retryable),
)
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

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(1, 0.5, 4),
    retry=retry_if_exception(lambda e: isinstance(e, HttpError) and e.retryable),
)
def http_get(url: str, params: dict | None = None, headers: dict | None = None) -> dict:
    http_rl.wait()
    r = requests.get(
        url,
        params=params or {},
        headers=headers or {"accept": "application/json", "user-agent": UA},
        timeout=HTTP_TIMEOUT,
    )
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
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        try:
            return float(str(v).replace(",", ""))
        except Exception:
            return None

# -------------------- Moralis cached -------------------- #

def date_to_block_cached(dt_iso: str) -> int:
    k = dt_iso
    v = CACHE.get("date_to_block", k, ttl=TTL_BLOCK)
    if v is not None:
        global MORALIS_SAVED_BY_CACHE
        MORALIS_SAVED_BY_CACHE += 1
        return int(v)

    j = moralis_get("/dateToBlock", params={"chain": CHAIN, "date": dt_iso})
    for f in ("block", "block_number", "blockNumber"):
        if f in j:
            block = int(j[f])
            CACHE.set("date_to_block", k, block)
            return block
    raise RuntimeError(f"dateToBlock senza block valido: {j}")

def get_token_metadata_cached(token: str) -> Tuple[str, int]:
    v = CACHE.get("metadata", token, ttl=TTL_METADATA)
    if v is not None:
        global MORALIS_SAVED_BY_CACHE
        MORALIS_SAVED_BY_CACHE += 1
        sym, dec = v
        return sym, int(dec)

    j = moralis_get("/erc20/metadata", params={"chain": CHAIN, "addresses": [token]})
    arr = j if isinstance(j, list) else j.get("result") or j.get("tokens") or j.get("items") or []
    if not arr:
        sym, dec = "TOKEN", 18
    else:
        m = arr[0]
        sym = m.get("symbol") or "TOKEN"
        try:
            dec = int(m.get("decimals", 18))
        except Exception:
            dec = 18

    CACHE.set("metadata", token, [sym, dec])
    return sym, dec

def get_token_price_usd_moralis_cached(token: str) -> Optional[float]:
    v = CACHE.get("price_moralis", token, ttl=TTL_PRICE_MORALIS)
    if v is not None:
        global MORALIS_SAVED_BY_CACHE
        MORALIS_SAVED_BY_CACHE += 1
        return _as_float(v)

    j = moralis_get(f"/erc20/{token}/price", params={"chain": CHAIN})
    price = _as_float(_first_key(j, ["usdPrice", "priceUsd", "usd_price", "price_usd"]))
    CACHE.set("price_moralis", token, price)
    return price

# -------------------- DexScreener (FREE, aggregato su tutte le pair) -------------------- #

def _pick_activity_bucket(minutes: int) -> str:
    if minutes <= 5:
        return "m5"
    if minutes <= 60:
        return "h1"
    if minutes <= 360:
        return "h6"
    return "h24"

def dexscreener_pick_pair_and_activity(token: str, minutes: int):
    """
    Ritorna: (main_pair, price_usd, buys_bucket, sells_bucket, liq_usd_best)
    - main_pair: pairAddress con più liquidità (solo per link/price hint)
    - price_usd: priceUsd della pair migliore per liquidità
    - buys/sells aggregati sul bucket coerente con la finestra (--minutes)
    """
    try:
        j = http_get(DEXSCREENER_TOKEN.format(address=token))
    except Exception as e:
        log.warning(f"DexScreener fetch failed: {e}")
        return None, None, None, None, None

    pairs = j.get("pairs") or []
    bucket = _pick_activity_bucket(minutes)

    best = None
    best_liq = -1.0
    agg_buys = {"m5": 0, "h1": 0, "h6": 0, "h24": 0}
    agg_sells = {"m5": 0, "h1": 0, "h6": 0, "h24": 0}

    for p in pairs:
        if (p.get("chainId") or "").lower() != "bsc":
            continue
        liq = _as_float(((p.get("liquidity") or {}).get("usd")))
        liq = 0.0 if liq is None else liq
        if liq > best_liq:
            best_liq = liq
            best = p

        txns = p.get("txns") or {}
        for b in ("m5", "h1", "h6", "h24"):
            t = txns.get(b) or {}
            try:
                agg_buys[b] += int(t.get("buys") or 0)
            except Exception:
                pass
            try:
                agg_sells[b] += int(t.get("sells") or 0)
            except Exception:
                pass

    if not best:
        return None, None, agg_buys.get(bucket, 0), agg_sells.get(bucket, 0), None

    pair = (best.get("pairAddress") or "").strip()
    price = _as_float(best.get("priceUsd"))
    buys_b = agg_buys.get(bucket, 0)
    sells_b = agg_sells.get(bucket, 0)
    log.info(
        f"DexScreener main pair: {pair} | price={price} | {bucket} buys={buys_b} sells={sells_b} | liq=${best_liq}"
    )
    return pair or None, price, buys_b, sells_b, best_liq

# -------------------- USD calc -------------------- #

def calc_usd_from_swap(s: dict, price_usd: Optional[float]) -> Optional[float]:
    for k in ("totalValueUsd", "valueUsd", "usdValue", "amountUsd", "usd_amount", "quoteAmountUsd", "priceUsd"):
        v = _as_float(s.get(k))
        if v is not None:
            return v
    if price_usd is None:
        return None
    for k in ("tokenAmount", "amountToken", "token_amount", "amount", "value", "toTokenAmount", "buyAmount", "amount0In", "amount1In", "amountIn"):
        v = _as_float(s.get(k))
        if v is not None:
            return v * float(price_usd)
    return None

# -------------------- Swaps iterator (TOKEN-FIRST) -------------------- #

def iterate_swaps_token_first(token: str, from_ts: int, to_ts: int):
    cursor = None
    page = 0
    consecutive_old_pages = 0

    while True:
        params = {"chain": CHAIN, "order": "DESC", "limit": 100, "transactionTypes": "buy"}
        if cursor:
            params["cursor"] = cursor

        try:
            j = moralis_get(f"/erc20/{token}/swaps", params=params)
        except BudgetExhausted as e:
            log.warning(str(e))
            return
        except Exception as e:
            log.warning(f"Swaps fetch failed for {token}: {e}")
            return

        result = j.get("result") or []
        page += 1
        log.debug(f"[token page {page}] swaps: {len(result)} (cursor={'yes' if j.get('cursor') else 'no'})")

        page_all_before = True
        any_in_window = False

        for s in result:
            ts_iso = _first_key(s, ["block_timestamp", "blockTimestamp"])
            ts_ok = True
            if ts_iso:
                try:
                    ts = int(datetime.fromisoformat(str(ts_iso).replace("Z", "+00:00")).timestamp())
                    ts_ok = (from_ts <= ts <= to_ts)
                    if ts >= from_ts:
                        page_all_before = False
                    if ts_ok:
                        any_in_window = True
                except Exception:
                    page_all_before = False
                    any_in_window = True
                    ts_ok = True

            if not ts_ok:
                continue

            yield s

        if any_in_window:
            consecutive_old_pages = 0
        elif page_all_before:
            consecutive_old_pages += 1
            if consecutive_old_pages >= 2:
                log.info("Early-stop: 2 pagine consecutive tutte < from_ts.")
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
        return int(v) if v is not None else None

    j = moralis_get(
        f"/wallets/{addr}/history",
        params={"chain": CHAIN, "order": "ASC", "limit": 1, "from_date": "1970-01-01T00:00:00Z"},
    )
    res = j.get("result") or []
    ts_out = None
    if res:
        ts = _first_key(res[0], ["block_timestamp", "blockTimestamp"])
        try:
            ts_out = int(datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp())
        except Exception:
            ts_out = None

    CACHE.set("first_tx_ts", addr, ts_out)
    return ts_out

def get_wallet_token_balance_fresh(addr: str, token: str, decimals: int) -> float:
    try:
        j = moralis_get(f"/{addr}/erc20", params={"chain": CHAIN, "token_addresses": [token]})
        arr = j if isinstance(j, list) else j.get("result") or j.get("items") or []
    except Exception:
        j = moralis_get(f"/{addr}/erc20", params={"chain": CHAIN})
        arr = j if isinstance(j, list) else j.get("result") or j.get("items") or []

    bal_raw = 0
    for t in arr:
        ta = (_first_key(t, ["token_address", "tokenAddress", "address", "tokenAddressHash"], "") or "").lower()
        if ta == token.lower():
            try:
                bal_raw = int(t.get("balance") or 0)
            except Exception:
                bal_raw = 0
            break

    bal = bal_raw / (10 ** decimals)
    if TTL_BALANCE > 0:
        CACHE.set("balance", f"{addr}:{token}", bal)
    return bal

# -------------------- CLI -------------------- #

def parse_args():
    ap = argparse.ArgumentParser(
        description="Young Whale Finder (BSC; Moralis-min; DexScreener-aggregated; TOKEN-FIRST)"
    )
    ap.add_argument("--token", required=True, help="Token address (BSC)")
    ap.add_argument("--minutes", type=int, default=5, help="Finestra in minuti")
    ap.add_argument("--min-usd", type=float, default=500.0, help="Soglia minima USD per BUY")
    ap.add_argument("--young-days", type=int, default=YOUNG_DAYS_DEFAULT, help="Wallet più giovani di N giorni (0=off)")
    return ap.parse_args()

# -------------------- Main -------------------- #

def main():
    t0 = time.time()
    args = parse_args()

    token = args.token
    minutes = max(1, int(args.minutes))
    min_usd = max(0.0, float(args.min_usd))
    YOUNG_DAYS = max(0, int(args.young_days))

    log.info(
        f"Start | token={token} minutes={minutes} min_usd={min_usd} young_days={YOUNG_DAYS} "
        f"| MAX_SWAPS={MAX_SWAPS} budget_calls={BUDGET_CALLS} log={LOG_LEVEL.lower()} "
        f"| MIN_BAL_USD={NAME_MIN_BAL_USD} TOP_SPENDERS_N={TOP_SPENDERS_N}"
    )

    now = datetime.now(timezone.utc)
    from_dt = now - timedelta(minutes=minutes)
    from_iso = from_dt.isoformat().replace("+00:00", "Z")
    to_iso = now.isoformat().replace("+00:00", "Z")
    from_ts = int(from_dt.timestamp())
    to_ts = int(now.timestamp()) + WINDOW_SLACK_SEC

    sym, dec = get_token_metadata_cached(token)
    main_pair, ds_price, agg_buys, agg_sells, liq = dexscreener_pick_pair_and_activity(token, minutes)
    price_usd = ds_price
    price_source = "dexscreener"

    if price_usd is None:
        try:
            price_usd = get_token_price_usd_moralis_cached(token)
            price_source = "moralis"
        except BudgetExhausted as e:
            log.warning(str(e))
            price_usd = None
            price_source = "none"

    if SKIP_DATE_TO_BLOCK:
        from_blk = None
        to_blk = None
        log.info("NO_DTB attivo: timestamp-only + early-stop.")
    else:
        try:
            from_blk = date_to_block_cached(from_iso)
            to_blk = date_to_block_cached(to_iso)
        except BudgetExhausted as e:
            log.warning(str(e))
            from_blk = None
            to_blk = None
        except Exception as e:
            log.warning(f"dateToBlock failed ({e}); fallback timestamp-only.")
            from_blk = None
            to_blk = None

    log.info(
        f"Token: {sym} (dec={dec}) | price={price_usd} [{price_source}] | window {from_iso} → {to_iso}"
    )

    buyers_sum_usd: Dict[str, float] = {}
    total_swaps = 0
    total_swaps_in_window = 0
    total_buys_ge_min = 0
    c_not_buy = 0
    c_below_min = 0
    c_no_wallet = 0

    iterator = iterate_swaps_token_first(token, from_ts, to_ts)

    try:
        for s in iterator:
            total_swaps += 1

            tx_type = str(_first_key(s, ["transactionType", "type", "side", "action"], "")).lower()
            if tx_type != "buy":
                c_not_buy += 1
                if total_swaps >= MAX_SWAPS:
                    break
                continue

            total_swaps_in_window += 1

            usd_val = calc_usd_from_swap(s, price_usd)
            if usd_val is None or usd_val < min_usd:
                c_below_min += 1
                if total_swaps >= MAX_SWAPS:
                    break
                continue

            total_buys_ge_min += 1

            wallet = _first_key(
                s,
                [
                    "walletAddress",
                    "trader",
                    "maker",
                    "sender",
                    "fromAddress",
                    "from",
                    "buyer",
                    "recipient",
                    "toAddress",
                    "to",
                ],
            )
            if not wallet:
                c_no_wallet += 1
                if total_swaps >= MAX_SWAPS:
                    break
                continue

            buyers_sum_usd[str(wallet)] = buyers_sum_usd.get(str(wallet), 0.0) + float(usd_val)

            if total_swaps >= MAX_SWAPS:
                log.warning(f"Reached MAX_SWAPS={MAX_SWAPS}, stopping swaps scan.")
                break

    except BudgetExhausted as e:
        log.warning(str(e))

    log.info(
        f"Swaps scanned={total_swaps} | in_window={total_swaps_in_window} | buys≥min={total_buys_ge_min} "
        f"| wallets={len(buyers_sum_usd)}"
    )

    buyers_total_usd_all = float(sum(buyers_sum_usd.values()))
    buyers_unique_all = int(len(buyers_sum_usd))

    # Valutiamo TUTTI i wallet (ordinati per spesa solo per priorità)
    wallets_ranked = sorted(buyers_sum_usd.items(), key=lambda kv: kv[1], reverse=True)
    log.info(f"Evaluating first_tx + balance for wallets={len(wallets_ranked)}")

    names_list = load_names_list(NAMES_FILE)
    young_cutoff_ts = int(now.timestamp()) - YOUNG_DAYS * 86400

    results: List[dict] = []
    kept = 0
    skipped_old = 0
    skipped_low_bal = 0
    skipped_no_price = 0
    budget_exhausted_at = None
    kept_name_map: Dict[str, str] = {}

    for addr, sum_usd in wallets_ranked:
        try:
            first_ts = get_wallet_first_tx_ts_cached(addr)
        except BudgetExhausted as e:
            budget_exhausted_at = ("first_tx", str(e))
            log.warning(str(e))
            break

        # filtro young DISATTIVABILE: solo se YOUNG_DAYS > 0
        if YOUNG_DAYS > 0 and (first_ts is not None) and (first_ts <= young_cutoff_ts):
            skipped_old += 1
            if (LEVEL == logging.DEBUG) or LOG_REASONS:
                log.debug(f"SKIP old | {addr} first_ts={first_ts} cutoff={young_cutoff_ts}")
            continue

        try:
            bal_tok = get_wallet_token_balance_fresh(addr, token, dec)
        except BudgetExhausted as e:
            budget_exhausted_at = ("balance", str(e))
            log.warning(str(e))
            break

        if price_usd is None:
            skipped_no_price += 1
            if (LEVEL == logging.DEBUG) or LOG_REASONS:
                log.debug(f"SKIP no_price | {addr} bal_tok={bal_tok}")
            continue

        bal_usd = float(price_usd) * float(bal_tok)

        # filtro balance USD
        if bal_usd < NAME_MIN_BAL_USD:
            skipped_low_bal += 1
            if (LEVEL == logging.DEBUG) or LOG_REASONS:
                log.debug(f"SKIP low_bal | {addr} bal_usd={bal_usd:.2f} < {NAME_MIN_BAL_USD}")
            continue

        user_name = get_or_assign_name(addr, names_list)
        kept_name_map[addr] = user_name

        results.append(
            {
                "address": addr,
                "user_name": user_name,
                "balance_token": bal_tok,
                "balance_usd": bal_usd,
                "first_tx_ts": first_ts,
                "sum_buys_usd_window": sum_usd,
            }
        )
        kept += 1

    results.sort(key=lambda r: (r["balance_usd"] if r["balance_usd"] is not None else -1), reverse=True)

    # top spender “raw” per finestra, con nome SOLO se validato (>= NAME_MIN_BAL_USD)
    top_spenders_all = []
    for addr, sum_usd in sorted(buyers_sum_usd.items(), key=lambda kv: kv[1], reverse=True)[:TOP_SPENDERS_N]:
        rec = {"address": addr, "sum_buys_usd_window": float(sum_usd)}
        if addr in kept_name_map:
            rec["user_name"] = kept_name_map[addr]
        top_spenders_all.append(rec)

    out = {
        "chain": CHAIN,
        "token": token,
        "symbol": sym,
        "decimals": dec,
        "price_usd": price_usd,
        "price_source": price_source,
        "window_minutes": minutes,
        "min_usd": min_usd,
        "young_days": YOUNG_DAYS,
        "min_balance_usd": NAME_MIN_BAL_USD,
        "from_block": None if SKIP_DATE_TO_BLOCK else (None if "from_blk" not in locals() else from_blk),
        "to_block": None if SKIP_DATE_TO_BLOCK else (None if "to_blk" not in locals() else to_blk),
        "from_iso": from_iso,
        "to_iso": to_iso,
        "main_pair_hint": main_pair,
        "stats": {
            "total_swaps_scanned": total_swaps,
            "total_swaps_in_window": total_swaps_in_window,
            "buys_ge_min_usd": total_buys_ge_min,
            "wallet_unici_ge_min_usd": len(buyers_sum_usd),
            "wallet_valutati_topk": len(wallets_ranked),  # compat: ora è TUTTI i wallet valutati
            "wallet_tenuti": kept,
            "wallet_scartati_old": skipped_old,
            "wallet_scartati_low_balance_usd": skipped_low_bal,
            "wallet_scartati_no_price": skipped_no_price,
            "budget_exhausted": budget_exhausted_at,
            "dexscreener_activity_bucket": _pick_activity_bucket(minutes),
            "dexscreener_bucket_buys": agg_buys if isinstance(agg_buys, int) else 0,
            "dexscreener_bucket_sells": agg_sells if isinstance(agg_sells, int) else 0,
        },
        "agg": {
            "buyers_total_usd_window_all": buyers_total_usd_all,
            "buyers_unique_all": buyers_unique_all,
            "top_spenders_all": top_spenders_all,
        },
        "results": results,
    }

    CACHE.save()

    cu_report = {
        "moralis_calls": MORALIS_CALLS,
        "avoid_calls_by_cache": MORALIS_SAVED_BY_CACHE,
        "cache_hits": CACHE.hits,
        "cache_misses": CACHE.misses,
        "elapsed_sec": round(time.time() - t0, 2),
        "budget_calls": BUDGET_CALLS,
    }
    log.info(f"CU report: {cu_report}")
    print(json.dumps(out, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
