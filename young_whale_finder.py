#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Young Whale Finder — BSC (Moralis-min, DexScreener-aggregated) — STABLE TOKEN-FIRST

Estensioni (persistenza wallet):
- Introduce un DB persistente su file (wallet_db.json) per:
  - nome assegnato stabile per address
  - first_tx_ts stabile (età wallet) per ridurre chiamate Moralis e non ricalcolare
  - first_seen/last_seen e seen_count
- Ogni wallet in results include:
  - is_new: True se address mai visto prima nel wallet_db
  - first_seen_ts, last_seen_ts, seen_count (post-update)

ESTENSIONE TRANSFER:
- Oltre ai BUY per wallet "young" (vincolo via --young-days), aggiunge controllo TRANSFER in ingresso
  con soglia USD (--transfer-min-usd).
- NEW: filtro età SOLO per i TRANSFER via --transfer-young-days (0=off).
  Esempio: --young-days 7 (BUY) e --transfer-young-days 40 (TRANSFER).

Nota sicurezza:
- NON lasciare chiavi Moralis hardcoded nel file. Usa sempre la env MORALIS_API_KEY (GitHub Secrets).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any

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
MORALIS_API_KEY = os.getenv("MORALIS_API_KEY", "").strip()
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

# Quanti top transfers mostrare in agg.top_transfers_all (solo recap)
TOP_TRANSFERS_N = int(os.getenv("YWF_TOP_TRANSFERS_N", "50"))
TOP_TRANSFERS_N = max(10, min(TOP_TRANSFERS_N, 500))

# Scansione swap/transfer
MAX_SWAPS = int(os.getenv("YWF_MAX_SWAPS", "50000"))
MAX_TRANSFERS = int(os.getenv("YWF_MAX_TRANSFERS", "50000"))

# Moralis budget chiamate
BUDGET_CALLS = int(os.getenv("YWF_BUDGET_CALLS", "200"))

WINDOW_SLACK_SEC = int(os.getenv("YWF_WINDOW_SLACK", "90"))

# Fast mode: default OFF
if os.getenv("YWF_FAST", "0") == "1":
    TOP_K = max(8, min(TOP_K, 50))

SKIP_DATE_TO_BLOCK = os.getenv("YWF_NO_DTB", "1") == "1"

# Cache (non persistente per forza tra run; dipende da workflow)
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

# TRANSFER threshold (USD)
TRANSFER_MIN_USD_DEFAULT = float(os.getenv("YWF_TRANSFER_MIN_USD", "5000"))

# Se vuoi loggare il motivo di scarto dei singoli wallet (solo in DEBUG)
LOG_REASONS = os.getenv("YWF_LOG_REASONS", "0") == "1"

# Wallet DB persistente (da committare nel repo via GH Actions)
WALLET_DB_PATH = os.getenv("YWF_WALLET_DB", "wallet_db.json")


# -------------------- Utils -------------------- #

def normalize_addr(addr: str) -> str:
    a = (addr or "").strip()
    return a.lower()

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

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

def _parse_ts(ts_iso: Any) -> Optional[int]:
    if not ts_iso:
        return None
    try:
        return int(datetime.fromisoformat(str(ts_iso).replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


# -------------------- Wallet Registry (persistente) -------------------- #

class WalletRegistry:
    """
    File JSON persistente per stabilizzare:
      - name assegnato
      - first_tx_ts (età wallet)
      - first_seen / last_seen / count
    """
    def __init__(self, path: str):
        self.path = path
        self.data: Dict[str, Any] = {"version": 1, "updated_at": None, "wallets": {}}
        self._load()

    def _load(self):
        try:
            if os.path.exists(self.path):
                with open(self.path, "r", encoding="utf-8") as f:
                    j = json.load(f)
                if isinstance(j, dict):
                    # migrazioni soft:
                    # - se manca wallets ma sembra già un mapping addr->rec, lo importiamo
                    if "wallets" not in j and any(k.startswith("0x") for k in j.keys()):
                        wallets = {}
                        for k, v in j.items():
                            nk = normalize_addr(k)
                            if isinstance(v, str):
                                wallets[nk] = {"name": v}
                            elif isinstance(v, dict):
                                wallets[nk] = v
                            else:
                                wallets[nk] = {}
                        self.data["wallets"] = wallets
                        self.data["updated_at"] = utc_now_iso()
                    else:
                        self.data.update(j)
                        if "wallets" not in self.data or not isinstance(self.data["wallets"], dict):
                            self.data["wallets"] = {}
        except Exception as e:
            log.warning(f"Wallet DB load failed ({e}); starting empty.")
            self.data = {"version": 1, "updated_at": None, "wallets": {}}

    @property
    def wallets(self) -> Dict[str, dict]:
        w = self.data.get("wallets")
        if not isinstance(w, dict):
            self.data["wallets"] = {}
        return self.data["wallets"]

    def get(self, addr: str) -> Optional[dict]:
        return self.wallets.get(normalize_addr(addr))

    def upsert(self, addr: str, patch: dict):
        a = normalize_addr(addr)
        rec = self.wallets.get(a) or {}
        if not isinstance(rec, dict):
            rec = {}
        rec.update({k: v for k, v in patch.items() if v is not None})
        self.wallets[a] = rec

    def used_names(self) -> set:
        out = set()
        for rec in self.wallets.values():
            if isinstance(rec, dict):
                n = rec.get("name")
                if isinstance(n, str) and n.strip():
                    out.add(n.strip())
        return out

    def mark_seen(
        self,
        addr: str,
        now_ts: int,
        symbol: Optional[str] = None,
        token: Optional[str] = None,
        first_tx_ts: Optional[int] = None,
        balance_usd: Optional[float] = None,
        balance_token: Optional[float] = None,
        sum_buys_usd_window: Optional[float] = None,
        sum_transfers_usd_window: Optional[float] = None,
    ) -> Tuple[bool, dict]:
        """
        Ritorna (is_new, rec_post_update)
        """
        a = normalize_addr(addr)
        rec = self.wallets.get(a)
        is_new = rec is None
        if rec is None or not isinstance(rec, dict):
            rec = {"first_seen_ts": now_ts, "seen_count": 0}

        # incrementa contatore
        try:
            rec["seen_count"] = int(rec.get("seen_count", 0)) + 1
        except Exception:
            rec["seen_count"] = 1

        rec["last_seen_ts"] = now_ts

        if symbol:
            rec["last_symbol"] = symbol
        if token:
            rec["last_token"] = token

        # first_tx_ts: set solo se non già presente
        if first_tx_ts is not None and not rec.get("first_tx_ts"):
            rec["first_tx_ts"] = int(first_tx_ts)

        # snapshot last values
        if balance_usd is not None:
            rec["last_balance_usd"] = float(balance_usd)
        if balance_token is not None:
            rec["last_balance_token"] = float(balance_token)
        if sum_buys_usd_window is not None:
            rec["last_sum_buys_usd_window"] = float(sum_buys_usd_window)
        if sum_transfers_usd_window is not None:
            rec["last_sum_transfers_usd_window"] = float(sum_transfers_usd_window)

        self.wallets[a] = rec
        return is_new, rec

    def save(self):
        try:
            self.data["updated_at"] = utc_now_iso()
            # scrittura atomica
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self.path)
        except Exception as e:
            log.warning(f"Wallet DB save failed: {e}")


WALLET_DB = WalletRegistry(WALLET_DB_PATH)


# -------------------- Rate limiter -------------------- #

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


# -------------------- Disk Cache (best effort) -------------------- #

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

# -------------------- Names helpers (persistenti via WALLET_DB) -------------------- #

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
    a = normalize_addr(addr)

    # 1) Prima: wallet_db persistente
    rec = WALLET_DB.get(a)
    if rec and isinstance(rec, dict):
        nm = rec.get("name")
        if isinstance(nm, str) and nm.strip():
            return nm.strip()

    # 2) Compat: cache names locale (se presente)
    current = CACHE.get("names", a, ttl=None)
    if current is not None:
        nm = str(current).strip()
        if nm:
            WALLET_DB.upsert(a, {"name": nm})
            return nm

    # 3) Assegna nome nuovo unico (globale)
    used = set()
    used |= WALLET_DB.used_names()
    # includi anche cache locale (se c'è)
    for v in (CACHE.data.get("names") or {}).values():
        if isinstance(v, dict):
            vv = v.get("value")
            if isinstance(vv, str) and vv.strip():
                used.add(vv.strip())

    available = [n for n in names if n not in used]
    if available:
        candidate = random.SystemRandom().choice(available)
    else:
        base = random.SystemRandom().choice(names) if names else "User"
        i = 2
        candidate = base
        while candidate in used:
            candidate = f"{base}{i}"
            i += 1

    WALLET_DB.upsert(a, {"name": candidate})
    CACHE.set("names", a, candidate)
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


# -------------------- Moralis cached -------------------- #

def date_to_block_cached(dt_iso: str) -> int:
    k = dt_iso
    v = CACHE.get("date_to_block", k, ttl=600)
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
    for k in (
        "tokenAmount", "amountToken", "token_amount", "amount", "value",
        "toTokenAmount", "buyAmount", "amount0In", "amount1In", "amountIn"
    ):
        v = _as_float(s.get(k))
        if v is not None:
            return v * float(price_usd)
    return None

def calc_usd_from_transfer(t: dict, price_usd: Optional[float], decimals: int) -> Optional[float]:
    # Moralis può fornire già un valore USD (dipende dall'endpoint / disponibilità)
    for k in ("value_usd", "valueUsd", "usdValue", "usd_value", "amountUsd", "amount_usd"):
        v = _as_float(t.get(k))
        if v is not None:
            return v

    if price_usd is None:
        return None

    raw = _first_key(t, ["value", "amount", "token_value", "tokenValue", "value_raw"])
    if raw is None:
        # alcuni payload hanno "value_decimal" già normalizzato
        vd = _as_float(_first_key(t, ["value_decimal", "valueDecimal", "amount_decimal", "amountDecimal"]))
        if vd is not None:
            return vd * float(price_usd)
        return None

    try:
        ival = int(str(raw))
        tok_amt = ival / (10 ** int(decimals))
        return tok_amt * float(price_usd)
    except Exception:
        # fallback: prova float diretto
        fv = _as_float(raw)
        if fv is None:
            return None
        # se fv è già in token (non raw), non possiamo distinguere perfettamente: assumiamo token-normalizzato
        return fv * float(price_usd)


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
            ts = _parse_ts(_first_key(s, ["block_timestamp", "blockTimestamp"]))
            if ts is not None:
                ts_ok = (from_ts <= ts <= to_ts)
                if ts >= from_ts:
                    page_all_before = False
                if ts_ok:
                    any_in_window = True
                if not ts_ok:
                    continue
            else:
                # se non parse, non possiamo early-stop affidabile: trattiamolo come in-window
                page_all_before = False
                any_in_window = True

            yield s

        if any_in_window:
            consecutive_old_pages = 0
        elif page_all_before:
            consecutive_old_pages += 1
            if consecutive_old_pages >= 2:
                log.info("Early-stop: 2 pagine consecutive tutte < from_ts (swaps).")
                return

        cursor = j.get("cursor") or None
        if not cursor:
            return


# -------------------- Transfers iterator (TOKEN-FIRST) -------------------- #

def iterate_transfers_token_first(token: str, from_ts: int, to_ts: int):
    """
    Endpoint Moralis:
      GET /erc20/:address/transfers  (address = token contract)
    """
    cursor = None
    page = 0
    consecutive_old_pages = 0

    while True:
        params = {"chain": CHAIN, "order": "DESC", "limit": 100}
        if cursor:
            params["cursor"] = cursor

        try:
            j = moralis_get(f"/erc20/{token}/transfers", params=params)
        except BudgetExhausted as e:
            log.warning(str(e))
            return
        except Exception as e:
            log.warning(f"Transfers fetch failed for {token}: {e}")
            return

        result = j.get("result") or []
        page += 1
        log.debug(f"[token page {page}] transfers: {len(result)} (cursor={'yes' if j.get('cursor') else 'no'})")

        page_all_before = True
        any_in_window = False

        for t in result:
            ts = _parse_ts(_first_key(t, ["block_timestamp", "blockTimestamp"]))
            if ts is not None:
                ts_ok = (from_ts <= ts <= to_ts)
                if ts >= from_ts:
                    page_all_before = False
                if ts_ok:
                    any_in_window = True
                if not ts_ok:
                    continue
            else:
                page_all_before = False
                any_in_window = True

            yield t

        if any_in_window:
            consecutive_old_pages = 0
        elif page_all_before:
            consecutive_old_pages += 1
            if consecutive_old_pages >= 2:
                log.info("Early-stop: 2 pagine consecutive tutte < from_ts (transfers).")
                return

        cursor = j.get("cursor") or None
        if not cursor:
            return


# -------------------- Enrichment (Moralis + WalletDB) -------------------- #

def get_wallet_first_tx_ts_cached(addr: str) -> Optional[int]:
    """
    Ordine lookup:
      1) WALLET_DB (persistente) -> se presente, usa quello
      2) CACHE first_tx_ts (best effort)
      3) Moralis call -> salva in CACHE + WALLET_DB
    """
    a = normalize_addr(addr)

    # 1) wallet_db
    rec = WALLET_DB.get(a)
    if rec and isinstance(rec, dict):
        vdb = rec.get("first_tx_ts")
        if isinstance(vdb, (int, float)) and vdb > 0:
            return int(vdb)

    # 2) disk cache best-effort
    v = CACHE.get("first_tx_ts", a, ttl=TTL_FIRST_TX)
    if v is not None:
        global MORALIS_SAVED_BY_CACHE
        MORALIS_SAVED_BY_CACHE += 1
        try:
            iv = int(v) if v is not None else None
        except Exception:
            iv = None
        if iv:
            WALLET_DB.upsert(a, {"first_tx_ts": iv})
        return iv

    # 3) moralis
    j = moralis_get(
        f"/wallets/{addr}/history",
        params={"chain": CHAIN, "order": "ASC", "limit": 1, "from_date": "1970-01-01T00:00:00Z"},
    )
    res = j.get("result") or []
    ts_out = None
    if res:
        ts = _first_key(res[0], ["block_timestamp", "blockTimestamp"])
        ts_out = _parse_ts(ts)

    CACHE.set("first_tx_ts", a, ts_out)
    if ts_out is not None:
        WALLET_DB.upsert(a, {"first_tx_ts": int(ts_out)})

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
        CACHE.set("balance", f"{normalize_addr(addr)}:{token.lower()}", bal)
    return bal


# -------------------- CLI -------------------- #

def parse_args():
    ap = argparse.ArgumentParser(
        description="Young Whale Finder (BSC; Moralis-min; DexScreener-aggregated; TOKEN-FIRST)"
    )
    ap.add_argument("--token", required=True, help="Token address (BSC)")
    ap.add_argument("--minutes", type=int, default=5, help="Finestra in minuti")
    ap.add_argument("--min-usd", type=float, default=500.0, help="Soglia minima USD per BUY (wallet young)")
    ap.add_argument(
        "--transfer-min-usd",
        type=float,
        default=TRANSFER_MIN_USD_DEFAULT,
        help="Soglia minima USD per TRANSFER in ingresso",
    )
    ap.add_argument("--young-days", type=int, default=YOUNG_DAYS_DEFAULT, help="BUY: wallet più giovani di N giorni (0=off)")

    # NEW: filtro età SOLO per TRANSFER
    ap.add_argument("--transfer-young-days", type=int, default=0,
                    help="TRANSFER: segnala solo wallet con età <= N giorni (0=off)")

    ap.add_argument("--disable-transfers", action="store_true", help="Disabilita scansione transfers")
    return ap.parse_args()


# -------------------- Main -------------------- #

def main():
    t0 = time.time()
    args = parse_args()

    token = args.token
    minutes = max(1, int(args.minutes))
    min_usd = max(0.0, float(args.min_usd))
    transfer_min_usd = max(0.0, float(args.transfer_min_usd))

    # BUY: invariato
    YOUNG_DAYS = max(0, int(args.young_days))

    # NEW: TRANSFER only
    TRANSFER_YOUNG_DAYS = max(0, int(getattr(args, "transfer_young_days", 0) or 0))

    transfers_enabled = (not args.disable_transfers) and (transfer_min_usd > 0)

    log.info(
        f"Start | token={token} minutes={minutes} min_usd(BUY)={min_usd} "
        f"transfer_min_usd(TRANSFER)={transfer_min_usd} transfers_enabled={int(transfers_enabled)} "
        f"young_days(BUY)={YOUNG_DAYS} transfer_young_days(TRANSFER)={TRANSFER_YOUNG_DAYS} | "
        f"MAX_SWAPS={MAX_SWAPS} MAX_TRANSFERS={MAX_TRANSFERS} "
        f"budget_calls={BUDGET_CALLS} log={LOG_LEVEL.lower()} | MIN_BAL_USD={NAME_MIN_BAL_USD} "
        f"TOP_SPENDERS_N={TOP_SPENDERS_N} TOP_TRANSFERS_N={TOP_TRANSFERS_N} | WALLET_DB={WALLET_DB_PATH}"
    )

    now = datetime.now(timezone.utc)
    now_ts = int(now.timestamp())

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

    # ---------------- BUY scan ----------------
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

            w = normalize_addr(str(wallet))
            buyers_sum_usd[w] = buyers_sum_usd.get(w, 0.0) + float(usd_val)

            if total_swaps >= MAX_SWAPS:
                log.warning(f"Reached MAX_SWAPS={MAX_SWAPS}, stopping swaps scan.")
                break

    except BudgetExhausted as e:
        log.warning(str(e))

    log.info(
        f"Swaps scanned={total_swaps} | in_window={total_swaps_in_window} | buys≥min={total_buys_ge_min} "
        f"| buy_wallets={len(buyers_sum_usd)}"
    )

    # ---------------- TRANSFER scan ----------------
    transfers_sum_usd: Dict[str, float] = {}
    transfers_count: Dict[str, int] = {}
    transfers_max: Dict[str, float] = {}

    transfers_last_ts: Dict[str, int] = {}
    transfers_last_iso: Dict[str, str] = {}

    total_transfers = 0
    total_transfers_in_window = 0
    total_transfers_ge_min = 0
    c_transfer_no_to = 0
    c_transfer_below_min = 0

    if transfers_enabled:
        it2 = iterate_transfers_token_first(token, from_ts, to_ts)
        try:
            for t in it2:
                total_transfers += 1
                total_transfers_in_window += 1

                to_addr = _first_key(t, ["to_address", "toAddress", "to", "recipient", "receiver"])
                if not to_addr:
                    c_transfer_no_to += 1
                    if total_transfers >= MAX_TRANSFERS:
                        break
                    continue

                usd_val = calc_usd_from_transfer(t, price_usd, dec)
                if usd_val is None or usd_val < transfer_min_usd:
                    c_transfer_below_min += 1
                    if total_transfers >= MAX_TRANSFERS:
                        break
                    continue

                total_transfers_ge_min += 1
                t_ts = _parse_ts(_first_key(t, ["block_timestamp", "blockTimestamp"]))
                if t_ts is not None:
                    prev_ts = transfers_last_ts.get(w := normalize_addr(str(to_addr)))
                    if (prev_ts is None) or (int(t_ts) > int(prev_ts)):
                        transfers_last_ts[w] = int(t_ts)
                        transfers_last_iso[w] = datetime.fromtimestamp(int(t_ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                else:
                    w = normalize_addr(str(to_addr))

                transfers_sum_usd[w] = transfers_sum_usd.get(w, 0.0) + float(usd_val)
                transfers_count[w] = transfers_count.get(w, 0) + 1
                prev = transfers_max.get(w, 0.0)
                if float(usd_val) > prev:
                    transfers_max[w] = float(usd_val)

                if total_transfers >= MAX_TRANSFERS:
                    log.warning(f"Reached MAX_TRANSFERS={MAX_TRANSFERS}, stopping transfers scan.")
                    break

        except BudgetExhausted as e:
            log.warning(str(e))

        log.info(
            f"Transfers scanned={total_transfers} | in_window={total_transfers_in_window} | transfers≥min={total_transfers_ge_min} "
            f"| transfer_wallets={len(transfers_sum_usd)}"
        )
    else:
        log.info("Transfers scan disabilitato (transfer_min_usd=0 o --disable-transfers).")

    # union candidates
    candidate_wallets = set(buyers_sum_usd.keys()) | set(transfers_sum_usd.keys())

    log.info(f"Evaluating first_tx + balance for candidate_wallets={len(candidate_wallets)}")

    names_list = load_names_list(NAMES_FILE)

    # BUY cutoff (invariato)
    young_cutoff_ts = int(now.timestamp()) - YOUNG_DAYS * 86400 if YOUNG_DAYS > 0 else None
    # TRANSFER cutoff (nuovo, separato)
    transfer_young_cutoff_ts = int(now.timestamp()) - TRANSFER_YOUNG_DAYS * 86400 if TRANSFER_YOUNG_DAYS > 0 else None

    results: List[dict] = []
    kept = 0
    skipped_old = 0
    skipped_transfer_old = 0
    skipped_low_bal = 0
    skipped_no_price = 0
    budget_exhausted_at = None
    kept_name_map: Dict[str, str] = {}
    new_wallets_in_results = 0

    # stats support
    eligible_buy_candidates = 0
    eligible_transfer_candidates = 0

    for addr in candidate_wallets:
        buy_sum = buyers_sum_usd.get(addr, 0.0)
        transfer_sum = transfers_sum_usd.get(addr, 0.0)

        # enrichment: first tx
        try:
            first_ts = get_wallet_first_tx_ts_cached(addr)
        except BudgetExhausted as e:
            budget_exhausted_at = ("first_tx", str(e))
            log.warning(str(e))
            break

        # BUY young (invariato)
        is_young = None
        if YOUNG_DAYS > 0 and first_ts is not None and young_cutoff_ts is not None:
            is_young = (first_ts > young_cutoff_ts)

        # TRANSFER young (nuovo, separato)
        is_young_transfer = None
        if TRANSFER_YOUNG_DAYS > 0 and first_ts is not None and transfer_young_cutoff_ts is not None:
            is_young_transfer = (first_ts > transfer_young_cutoff_ts)

        # eligibility per trigger
        eligible_buy = (buy_sum >= min_usd) and (buy_sum > 0)
        eligible_transfer = transfers_enabled and (transfer_sum >= transfer_min_usd) and (transfer_sum > 0)

        # BUY: applica vincolo come prima (solo young)
        if YOUNG_DAYS > 0 and is_young is not None:
            if eligible_buy and (not is_young):
                eligible_buy = False
                skipped_old += 1
                if (LEVEL == logging.DEBUG) or LOG_REASONS:
                    log.debug(f"SKIP buy old | {addr} first_ts={first_ts} cutoff={young_cutoff_ts}")

        # TRANSFER: applica vincolo SOLO se impostato --transfer-young-days
        if TRANSFER_YOUNG_DAYS > 0 and is_young_transfer is not None:
            if eligible_transfer and (not is_young_transfer):
                eligible_transfer = False
                skipped_transfer_old += 1
                if (LEVEL == logging.DEBUG) or LOG_REASONS:
                    log.debug(f"SKIP transfer old | {addr} first_ts={first_ts} cutoff={transfer_young_cutoff_ts}")

        if not eligible_buy and not eligible_transfer:
            continue

        if eligible_buy:
            eligible_buy_candidates += 1
        if eligible_transfer:
            eligible_transfer_candidates += 1

        # balance
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

        trigger_parts = []
        if eligible_buy:
            trigger_parts.append("BUY")
        if eligible_transfer:
            trigger_parts.append("TRANSFER")
        trigger = "+".join(trigger_parts) if trigger_parts else ""

        # marca seen su wallet_db + calcola is_new
        is_new, rec_post = WALLET_DB.mark_seen(
            addr=addr,
            now_ts=now_ts,
            symbol=sym,
            token=token,
            first_tx_ts=first_ts,
            balance_usd=bal_usd,
            balance_token=bal_tok,
            sum_buys_usd_window=(buy_sum if eligible_buy else None),
            sum_transfers_usd_window=(transfer_sum if eligible_transfer else None),
        )
        if is_new:
            new_wallets_in_results += 1

        results.append(
            {
                "address": addr,
                "user_name": user_name,
                "trigger": trigger,
                "is_new": bool(is_new),
                "first_seen_ts": rec_post.get("first_seen_ts"),
                "last_seen_ts": rec_post.get("last_seen_ts"),
                "seen_count": rec_post.get("seen_count"),
                "balance_token": bal_tok,
                "balance_usd": bal_usd,
                "first_tx_ts": first_ts,
                "sum_buys_usd_window": (buy_sum if eligible_buy else None),
                "sum_transfers_usd_window": (transfer_sum if eligible_transfer else None),
                "transfers_count_window": (transfers_count.get(addr) if eligible_transfer else None),
                "max_transfer_usd_window": (transfers_max.get(addr) if eligible_transfer else None),
                "last_transfer_ts_window": (transfers_last_ts.get(addr) if eligible_transfer else None),
                "last_transfer_iso_window": (transfers_last_iso.get(addr) if eligible_transfer else None),
            }
        )
        kept += 1

    results.sort(key=lambda r: (r["balance_usd"] if r["balance_usd"] is not None else -1), reverse=True)

    # agg: top spender “raw” per finestra (BUY)
    top_spenders_all = []
    for addr, sum_usd in sorted(buyers_sum_usd.items(), key=lambda kv: kv[1], reverse=True)[:TOP_SPENDERS_N]:
        rec = {"address": addr, "sum_buys_usd_window": float(sum_usd)}
        if addr in kept_name_map:
            rec["user_name"] = kept_name_map[addr]
        else:
            dbrec = WALLET_DB.get(addr)
            if dbrec and isinstance(dbrec, dict) and isinstance(dbrec.get("name"), str) and dbrec.get("name").strip():
                rec["user_name"] = dbrec["name"].strip()
        top_spenders_all.append(rec)

    # agg: top transfers “raw” per finestra (TRANSFER)
    top_transfers_all = []
    for addr, sum_usd in sorted(transfers_sum_usd.items(), key=lambda kv: kv[1], reverse=True)[:TOP_TRANSFERS_N]:
        rec = {
            "address": addr,
            "sum_transfers_usd_window": float(sum_usd),
            "transfers_count_window": int(transfers_count.get(addr, 0)),
            "max_transfer_usd_window": float(transfers_max.get(addr, 0.0)),
        }
        if addr in kept_name_map:
            rec["user_name"] = kept_name_map[addr]
        else:
            dbrec = WALLET_DB.get(addr)
            if dbrec and isinstance(dbrec, dict) and isinstance(dbrec.get("name"), str) and dbrec.get("name").strip():
                rec["user_name"] = dbrec["name"].strip()
        top_transfers_all.append(rec)

    out = {
        "chain": CHAIN,
        "token": token,
        "symbol": sym,
        "decimals": dec,
        "price_usd": price_usd,
        "price_source": price_source,
        "window_minutes": minutes,
        "min_usd": min_usd,
        "transfer_min_usd": transfer_min_usd,
        "transfers_enabled": bool(transfers_enabled),
        "young_days": YOUNG_DAYS,
        "transfer_young_days": TRANSFER_YOUNG_DAYS,
        "min_balance_usd": NAME_MIN_BAL_USD,
        "from_block": None if SKIP_DATE_TO_BLOCK else from_blk,
        "to_block": None if SKIP_DATE_TO_BLOCK else to_blk,
        "from_iso": from_iso,
        "to_iso": to_iso,
        "main_pair_hint": main_pair,
        "wallet_db_path": WALLET_DB_PATH,
        "stats": {
            "total_swaps_scanned": total_swaps,
            "total_swaps_in_window": total_swaps_in_window,
            "buys_ge_min_usd": total_buys_ge_min,
            "wallet_unici_ge_min_usd": len(buyers_sum_usd),
            "total_transfers_scanned": total_transfers,
            "total_transfers_in_window": total_transfers_in_window,
            "transfers_ge_min_usd": total_transfers_ge_min,
            "wallet_unici_ge_min_transfers_usd": len(transfers_sum_usd),
            "candidate_wallets_union": len(candidate_wallets),
            "eligible_buy_candidates": eligible_buy_candidates,
            "eligible_transfer_candidates": eligible_transfer_candidates,
            "wallet_valutati_topk": len(candidate_wallets),
            "wallet_tenuti": kept,
            "wallet_new_in_results": new_wallets_in_results,
            "wallet_scartati_old_buy": skipped_old,
            "wallet_scartati_old_transfer": skipped_transfer_old,
            "wallet_scartati_low_balance_usd": skipped_low_bal,
            "wallet_scartati_no_price": skipped_no_price,
            "budget_exhausted": budget_exhausted_at,
            "dexscreener_activity_bucket": _pick_activity_bucket(minutes),
            "dexscreener_bucket_buys": agg_buys if isinstance(agg_buys, int) else 0,
            "dexscreener_bucket_sells": agg_sells if isinstance(agg_sells, int) else 0,
        },
        "agg": {
            "buyers_total_usd_window_all": float(sum(buyers_sum_usd.values())),
            "buyers_unique_all": int(len(buyers_sum_usd)),
            "transfers_total_usd_window_all": float(sum(transfers_sum_usd.values())),
            "transfers_unique_all": int(len(transfers_sum_usd)),
            "top_spenders_all": top_spenders_all,
            "top_transfers_all": top_transfers_all,
        },
        "results": results,
    }

    # salva cache best-effort + wallet_db persistente
    CACHE.save()
    WALLET_DB.save()

    cu_report = {
        "moralis_calls": MORALIS_CALLS,
        "avoid_calls_by_cache": MORALIS_SAVED_BY_CACHE,
        "cache_hits": CACHE.hits,
        "cache_misses": CACHE.misses,
        "elapsed_sec": round(time.time() - t0, 2),
        "budget_calls": BUDGET_CALLS,
        "wallet_db_size": len(WALLET_DB.wallets),
    }
    log.info(f"CU report: {cu_report}")
    print(json.dumps(out, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
