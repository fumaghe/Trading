#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Young Whale Finder — BSC (Moralis Token API, Pancake v3)

Uso:
  python young_whale_finder_moralis.py --token 0x... --minutes 5 --min-usd 1000

Parametri CLI (solo questi, come richiesto):
  --token     Indirizzo ERC-20 (BSC) della coin target
  --minutes   Finestra temporale (minuti) da analizzare (es. 5)
  --min-usd   Soglia minima USD per considerare un buy (es. 1000)

Dove mettere la key Moralis:
  • Imposta la variabile d'ambiente MORALIS_API_KEY, ad es. (Linux/macOS):
        export MORALIS_API_KEY=xxxxxxxxxxxxxxxx
    (Windows PowerShell):
        setx MORALIS_API_KEY "xxxxxxxxxxxxxxxx"
  • In alternativa, modifica la costante MORALIS_API_KEY qui sotto.

Dipendenze:
  pip install requests tenacity rich

  
python young_whale_finder.py --token 0x48a18A4782b65a0fBed4dcA608BB28038B7bE339 --minutes 10 --min-usd 300

Logica:
  1) Trova la pool Pancake v3 principale della coin (Moralis "Get Token Pairs").
  2) Scarica i "swaps" della pool negli ultimi N minuti (Moralis "Get Swaps by Pair Address"),
     filtra solo i BUY e con totalValueUsd >= min_usd.
  3) Raggruppa per wallet (walletAddress) e somma l'importo USD acquistato nella finestra.
  4) Per ogni wallet unico:
        - recupera la "prima transazione" su BSC (Moralis "Get Wallet History", order=ASC, limit=1);
          se più vecchia del cutoff (YOUNG_DAYS), scarta,
          altrimenti
        - legge il balance corrente della coin (Moralis "Get ERC20 token balances" filtrando l'indirizzo).
  5) Stampa JSON finale con [{address, balance_token, balance_usd, first_tx_ts, sum_buys_usd_window}].

Note:
  - Questa versione usa esclusivamente le REST API Moralis (niente RPC diretto), con rate-limit e retry.
  - Per ridurre chiamate: deduplica i wallet prima dei controlli e delle letture balance.
"""

from __future__ import annotations
import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# -------------------- Config -------------------- #

MORALIS_API_KEY = os.getenv("MORALIS_API_KEY", "").strip() or "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub25jZSI6ImIxYjNmZjA0LWViNTItNGFlYy1iNzg3LTg5MmYyNTg3MWIzYyIsIm9yZ0lkIjoiNDgwMDg5IiwidXNlcklkIjoiNDkzOTA4IiwidHlwZUlkIjoiNDZjMTgwOTktZmI2NC00MGFlLTlkODktZWY1NDRlNzQ4NzAwIiwidHlwZSI6IlBST0pFQ1QiLCJpYXQiOjE3NjI1Mjk0MDEsImV4cCI6NDkxODI4OTQwMX0.EuoHs5sg6ljIZeY6oWYOGzrOyCDcMIJGZlzPU0DdCzI"  # <- opzionale: inserisci qui la tua key
MORALIS_BASE = "https://deep-index.moralis.io/api/v2.2"
DEXSCREENER_TOKEN = "https://api.dexscreener.com/latest/dex/tokens/{address}"

CHAIN = "bsc"     # fisso
YOUNG_DAYS = 3    # ← come da tua richiesta iniziale (cambia qui se vuoi 5)
RPS = 3.0         # rate limiter "soft" per evitare 429

HTTP_TIMEOUT = 25
UA = "Mozilla/5.0 (YoungWhaleFinder/1.1; +https://moralis.com)"

# -------------------- Utils: rate & HTTP -------------------- #

class SimpleRate:
    def __init__(self, rps: float):
        self.min_interval = 1.0 / max(0.001, rps)
        self.last = 0.0
    def wait(self):
        now = time.time()
        delta = now - self.last
        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)
        self.last = time.time()

http_rl = SimpleRate(RPS)

class HttpError(RuntimeError):
    pass

def _moralis_headers() -> dict:
    if not MORALIS_API_KEY:
        raise RuntimeError("Manca MORALIS_API_KEY (env var o costante nel file).")
    return {
        "accept": "application/json",
        "user-agent": UA,
        "X-API-Key": MORALIS_API_KEY,
    }

@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=1, max=8),
       retry=retry_if_exception_type((requests.RequestException, HttpError)))
def moralis_get(path: str, params: dict | None = None) -> dict:
    http_rl.wait()
    url = f"{MORALIS_BASE}{path}"
    r = requests.get(url, headers=_moralis_headers(), params=params or {}, timeout=HTTP_TIMEOUT)
    if r.status_code >= 400:
        try:
            err = r.json()
        except Exception:
            err = {"status": r.status_code, "text": r.text[:200]}
        raise HttpError(f"GET {path} -> {r.status_code}: {err}")
    return r.json()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=0.5, max=4),
       retry=retry_if_exception_type((requests.RequestException, HttpError)))
def http_get(url: str, params: dict | None = None, headers: dict | None = None) -> dict:
    http_rl.wait()
    r = requests.get(url, params=params or {}, headers=headers or {"accept": "application/json", "user-agent": UA}, timeout=HTTP_TIMEOUT)
    if r.status_code >= 400:
        try:
            err = r.json()
        except Exception:
            err = {"status": r.status_code, "text": r.text[:200]}
        raise HttpError(f"GET {url} -> {r.status_code}: {err}")
    return r.json()

# -------------------- Moralis helpers -------------------- #

def date_to_block(dt_iso: str) -> int:
    j = moralis_get("/dateToBlock", params={"chain": CHAIN, "date": dt_iso})
    # risposta tipica: {"block":"...", "timestamp": "..."}
    for k in ("block", "block_number", "blockNumber"):
        if k in j:
            try:
                return int(j[k])
            except Exception:
                pass
    # fallback duro se non c'è il campo previsto
    raise RuntimeError(f"dateToBlock senza block valido: {j}")

def get_token_metadata(token: str) -> Tuple[str, int]:
    j = moralis_get("/erc20/metadata", params={"chain": CHAIN, "addresses": [token]})
    arr = j if isinstance(j, list) else j.get("result") or j.get("tokens") or j.get("items") or []
    if not arr:
        return "TOKEN", 18
    m = arr[0]
    sym = m.get("symbol") or "TOKEN"
    try: dec = int(m.get("decimals", 18))
    except Exception: dec = 18
    return sym, dec

def get_token_price_usd(token: str) -> Optional[float]:
    j = moralis_get(f"/erc20/{token}/price", params={"chain": CHAIN})
    for k in ("usdPrice", "priceUsd", "usd_price", "price_usd"):
        v = j.get(k) if isinstance(j, dict) else None
        if v is not None:
            try: return float(v)
            except Exception: pass
    return None

def iterate_swaps_buy_by_token(token: str, from_blk: int, to_blk: int):
    """
    Swaps per token address (non per pair).
    Filtreremo lato client: exchangeName contiene 'Pancake v3' + transactionType == 'buy'.
    """
    cursor = None
    while True:
        params = {
            "chain": CHAIN,
            "order": "DESC",
            "limit": 100,
            "fromBlock": from_blk,
            "toBlock": to_blk,
            "transactionTypes": "buy",  # se non supportato, filtriamo dopo
        }
        if cursor:
            params["cursor"] = cursor
        j = moralis_get(f"/erc20/{token}/swaps", params=params)
        result = j.get("result") or []
        for it in result:
            yield it
        cursor = j.get("cursor") or None
        if not cursor:
            break

def get_wallet_first_tx_ts(addr: str) -> Optional[int]:
    j = moralis_get(f"/wallets/{addr}/history", params={
        "chain": CHAIN, "order": "ASC", "limit": 1, "from_date": "1970-01-01T00:00:00Z"
    })
    res = j.get("result") or []
    if not res:
        return None
    ts = res[0].get("block_timestamp") or res[0].get("blockTimestamp")
    try:
        return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
    except Exception:
        return None

def get_wallet_token_balance(addr: str, token: str, decimals: int) -> float:
    # prova filtro lato API
    try:
        j = moralis_get(f"/{addr}/erc20", params={"chain": CHAIN, "token_addresses": [token]})
        arr = j if isinstance(j, list) else j.get("result") or j.get("items") or []
    except Exception:
        j = moralis_get(f"/{addr}/erc20", params={"chain": CHAIN})
        arr = j if isinstance(j, list) else j.get("result") or j.get("items") or []
    bal_raw = 0
    for t in arr:
        ta = (t.get("token_address") or t.get("tokenAddress") or "").lower()
        if ta == token.lower():
            try: bal_raw = int(t.get("balance") or 0)
            except Exception: bal_raw = 0
            break
    return bal_raw / (10 ** decimals)

# -------------------- DexScreener fallback (solo per identificare la main pair) -------------------- #

def dexscreener_pick_pancake_v3_main_pair(token: str) -> Tuple[Optional[str], Optional[float]]:
    try:
        j = http_get(DEXSCREENER_TOKEN.format(address=token))
    except Exception:
        return None, None
    pairs = j.get("pairs") or []
    best_addr, best_liq, price_usd = None, -1.0, None
    for p in pairs:
        if (p.get("chainId") or "").lower() != "bsc":
            continue
        if "pancake" not in (p.get("dexId","").lower()):
            continue
        if "v3" not in (p.get("dexId","").lower()):
            continue
        liq = float(((p.get("liquidity") or {}).get("usd") or 0))
        if liq > best_liq:
            best_liq = liq
            best_addr = (p.get("pairAddress") or "").strip()
            try:
                price_usd = float(p.get("priceUsd")) if p.get("priceUsd") is not None else price_usd
            except Exception:
                pass
    return best_addr, price_usd

# -------------------- Main -------------------- #

def parse_args():
    ap = argparse.ArgumentParser(description="Young Whale Finder (BSC, Moralis swaps; DexScreener fallback)")
    ap.add_argument("--token", required=True, help="Token address (BSC)")
    ap.add_argument("--minutes", type=int, default=5, help="Finestra in minuti")
    ap.add_argument("--min-usd", type=float, default=1000.0, help="Soglia minima USD per BUY")
    return ap.parse_args()

def main():
    args = parse_args()
    token = args.token
    minutes = max(1, int(args.minutes))
    min_usd = max(0.0, float(args.min_usd))

    now = datetime.now(timezone.utc)
    from_dt = now - timedelta(minutes=minutes)
    from_iso = from_dt.isoformat().replace("+00:00", "Z")
    to_iso   = now.isoformat().replace("+00:00", "Z")

    # Mappa tempo → blocchi (richiesta esplicita)
    from_blk = date_to_block(from_iso)
    to_blk   = date_to_block(to_iso)

    # Meta & prezzo
    sym, dec = get_token_metadata(token)
    price_usd = get_token_price_usd(token)

    # Fallback informativo: individua "main pair" Pancake v3 via DexScreener (NON blocca il flusso)
    main_pair, ds_price = dexscreener_pick_pancake_v3_main_pair(token)
    if price_usd is None and ds_price is not None:
        price_usd = ds_price

    # 1) Leggi swaps del TOKEN (non della pair) nel range di blocchi e filtra Pancake v3 + BUY >= min_usd
    buyers_sum_usd: Dict[str, float] = {}
    total_swaps = 0

    for s in iterate_swaps_buy_by_token(token, from_blk, to_blk):
        total_swaps += 1
        # filtra tipo transazione (safety in caso l'API ignorasse transactionTypes)
        if (s.get("transactionType") or "").lower() != "buy":
            continue
        # filtra exchange Pancake v3
        ex_name = (s.get("exchangeName") or "").lower()
        if not ("pancake" in ex_name and "v3" in ex_name):
            continue
        # valore USD
        try:
            usd_val = float(s.get("totalValueUsd"))
        except Exception:
            usd_val = None
        if usd_val is None or usd_val < min_usd:
            continue
        # wallet
        wallet = s.get("walletAddress")
        if not wallet:
            continue
        buyers_sum_usd[wallet] = buyers_sum_usd.get(wallet, 0.0) + usd_val

    # 2) Filtro "giovani" + bilancio coin
    young_cutoff_ts = int(now.timestamp()) - YOUNG_DAYS * 86400
    results: List[dict] = []

    for addr, sum_usd in buyers_sum_usd.items():
        first_ts = get_wallet_first_tx_ts(addr)
        if first_ts is not None and first_ts <= young_cutoff_ts:
            continue  # troppo vecchio → scarto
        bal_tok = get_wallet_token_balance(addr, token, dec)
        bal_usd = (price_usd or 0.0) * bal_tok
        results.append({
            "address": addr,
            "balance_token": bal_tok,
            "balance_usd": bal_usd,
            "first_tx_ts": first_ts,
            "sum_buys_usd_window": sum_usd,
        })

    # Ordina per balance_usd desc
    results.sort(key=lambda r: r["balance_usd"], reverse=True)

    out = {
        "chain": CHAIN,
        "token": token,
        "symbol": sym,
        "decimals": dec,
        "price_usd": price_usd,
        "window_minutes": minutes,
        "min_usd": min_usd,
        "young_days": YOUNG_DAYS,
        "from_block": from_blk,
        "to_block": to_blk,
        "main_pair_hint": main_pair,  # informativo (da DexScreener)
        "stats": {
            "total_swaps_scanned": total_swaps,
            "wallet_unici_ge_min_usd": len(buyers_sum_usd),
            "wallet_tenuti": len(results),
        },
        "results": results,
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)