# build_dataset.py
# Dataset builder per selezionare pool simili su PancakeSwap v2 (BSC) e scaricare OHLCV 5m da GeckoTerminal.
# Versione robusta con fallback automatici e filtri adattivi centrati sul target.

import os, time, json, sqlite3, hashlib, math, argparse, sys, traceback
from urllib.parse import urlparse
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd

# -------------------- Costanti --------------------
DEX_API = "https://api.dexscreener.com"
GT_API  = "https://api.geckoterminal.com/api/v2"
DEFAULT_HEADERS = {
    "accept": "application/json",
    "user-agent": "dex-forecaster/2.0 (+cli)",
    "accept-language": "en-US,en;q=0.9",
}
NETWORK = "bsc"

# Quote tokens comuni su BSC
WBNB = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c".lower()
USDT = "0x55d398326f99059ff775485246999027b3197955".lower()
USDC = "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d".lower()
DAI  = "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3".lower()
DEFAULT_ANCHORS = [WBNB, USDT, USDC, DAI]

# Rate-limit per host
HOST_MIN_INTERVAL = {
    "api.geckoterminal.com": 2.2,
    "api.dexscreener.com": 0.15,
}
_last_call_ts: Dict[str, float] = {}

# Filtri hard default (regolabili da CLI)
DEFAULT_FILTERS: Dict[str, float] = {
    "liquidity_min": 4e5,   "liquidity_max": 8e6,
    "mcap_min":     1e7,    "mcap_max":     4e8,
    "vol24_min":    5e6,    "vol24_max":    6e7,
    "tx24_min":     8000,   "tx24_max":     120000,
    "age_days_min": 4,      "age_days_max": 30,
}

# -------------------- HTTP cache & Rate Limit --------------------
def _rate_limit_for(url: str):
    host = urlparse(url).netloc
    min_interval = HOST_MIN_INTERVAL.get(host, 0.2)
    last = _last_call_ts.get(host, 0.0)
    now = time.time()
    wait = last + min_interval - now
    if wait > 0:
        time.sleep(wait)
    _last_call_ts[host] = time.time()

def cache_get(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    cache_dir: str = "data/cache",
    ttl: int = 60,
    retries: int = 5,
    timeout: int = 30,
) -> Any:
    os.makedirs(cache_dir, exist_ok=True)
    key = hashlib.sha256((url + json.dumps(params or {}, sort_keys=True)).encode()).hexdigest()
    path = os.path.join(cache_dir, key + ".json")

    if os.path.exists(path) and (time.time() - os.path.getmtime(path) < ttl):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass

    req_headers = dict(DEFAULT_HEADERS)
    if headers:
        req_headers.update(headers)

    backoff = 1.0
    for attempt in range(1, retries + 1):
        try:
            _rate_limit_for(url)
            r = requests.get(url, params=params, headers=req_headers, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff)
                backoff = min(backoff * 2, 16)
                continue
            r.raise_for_status()
            data = r.json()
            try:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(data, f)
            except Exception:
                pass
            return data
        except requests.RequestException as e:
            if attempt == retries:
                raise RuntimeError(f"HTTP fallita per {url}: {e}") from e
            time.sleep(backoff)
            backoff = min(backoff * 2, 16)
    raise RuntimeError(f"HTTP fallita ripetutamente per {url}")

# -------------------- Utils --------------------
def _safe_float(x, default=0.0):
    try:
        if x in (None, "", "null"):
            return default
        return float(x)
    except Exception:
        return default

def _safe_int(x, default=0):
    try:
        if x in (None, "", "null"):
            return default
        return int(float(x))
    except Exception:
        return default

def _dex_is_pcs_v2(dex_id: str):
    d = (dex_id or "").lower()
    return d.startswith("pancakeswap")

def _age_days_from_ms(created_at):
    if created_at in (None, "", 0):
        return None
    try:
        v = int(created_at)
    except Exception:
        return None
    if v < 10**12:
        v *= 1000  # seconds → ms
    now_ms = int(time.time() * 1000)
    return (now_ms - v) / (1000 * 60 * 60 * 24)

def _mcap_or_fdv(p: Dict[str, Any]) -> float:
    m = p.get("marketCap")
    if m is None:
        m = p.get("fdv")
    return _safe_float(m, 0.0)

def _vol_h24(p: Dict[str, Any]) -> float:
    v = p.get("volume")
    if isinstance(v, dict):
        return _safe_float(v.get("h24"), 0.0)
    for k in ("volume24h", "vol24h"):
        if k in p:
            return _safe_float(p.get(k), 0.0)
    return 0.0

def _txns_h24(p: Dict[str, Any]) -> int:
    t = p.get("txns")
    if isinstance(t, dict):
        h24 = t.get("h24")
        if isinstance(h24, dict):
            return _safe_int(h24.get("buys"), 0) + _safe_int(h24.get("sells"), 0)
        if isinstance(h24, (int, float)):
            return _safe_int(h24, 0)
    for k in ("txns24h", "transactions24h", "txns24", "transactions"):
        v = p.get(k)
        if isinstance(v, dict):
            return _safe_int(v.get("buys"), 0) + _safe_int(v.get("sells"), 0)
        if v is not None:
            return _safe_int(v, 0)
    return 0

def _liq_usd(p: Dict[str, Any]) -> float:
    liq = p.get("liquidity")
    if isinstance(liq, dict):
        return _safe_float(liq.get("usd"), 0.0)
    return _safe_float(p.get("liquidityUsd"), 0.0)

def _price_usd(p: Dict[str, Any]) -> float:
    return _safe_float(p.get("priceUsd"), 0.0)

def _quote_addr_of(p: Dict[str, Any]) -> str:
    q = p.get("quoteToken") or {}
    return (q.get("address") or "").lower()

# -------------------- DexScreener --------------------
def ds_get_pair(chain: str = NETWORK, pair_addr: str = "") -> Dict[str, Any]:
    data = cache_get(f"{DEX_API}/latest/dex/pairs/{chain}/{pair_addr}", ttl=60, retries=6)
    pairs = data.get("pairs", []) or []
    if not pairs:
        raise ValueError(f"Nessun pair trovato per {chain}/{pair_addr}. Verifica che l'indirizzo sia corretto.")
    return pairs[0]

def ds_token_pairs(token_addr: str, chain: str = NETWORK) -> List[Dict[str, Any]]:
    data = cache_get(f"{DEX_API}/token-pairs/v1/{chain}/{token_addr}", ttl=120, retries=6)
    if isinstance(data, dict) and "pairs" in data:
        return data.get("pairs", []) or []
    if isinstance(data, list):
        return data
    return []

def ds_pairs_by_dex(chain: str = NETWORK, dex_ids: Tuple[str, ...] = ("pancakeswapv2", "pancakeswap")) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for d in dex_ids:
        data = cache_get(f"{DEX_API}/latest/dex/pairs/{chain}/{d}", ttl=120, retries=6)
        out.extend(data.get("pairs", []) or [])
    return out

def hydrate_candidate_if_needed(p: Dict[str, Any], chain: str = NETWORK) -> Dict[str, Any]:
    needs = False
    if p.get("pairCreatedAt") in (None, "", 0):
        needs = True
    if p.get("txns") in (None, {}) and all(k not in p for k in ("txns24h", "transactions24h", "txns24", "transactions")):
        needs = True
    if (p.get("volume") or {}).get("h24") in (None, 0):
        needs = True
    if p.get("marketCap") is None and p.get("fdv") is None:
        needs = True
    if not needs:
        return p
    try:
        addr = p.get("pairAddress") or ""
        if not addr:
            return p
        fresh = ds_get_pair(chain, addr)
        for k in ("pairCreatedAt", "txns", "volume", "marketCap", "fdv", "priceUsd", "liquidity"):
            if p.get(k) in (None, "", 0, {}):
                if fresh.get(k) not in (None, "", 0, {}):
                    p[k] = fresh.get(k)
    except Exception:
        pass
    return p

# -------------------- Filtri & punteggi --------------------
def _filters_failures(
    p: Dict[str, Any],
    f: Dict[str, float],
    quote_mode: str,
    quote_single: Optional[str],
    quote_allow_set: "set[str]",
) -> Dict[str, int]:
    fails: Dict[str, int] = {}

    if not _dex_is_pcs_v2(p.get("dexId", "")):
        fails["dex!=pcs"] = 1

    if quote_mode != "off":
        q = _quote_addr_of(p)
        if quote_mode == "single":
            if not quote_single:
                fails["quote_single_not_set"] = 1
            elif q != quote_single:
                fails["quote!=selected"] = 1
        elif quote_mode == "allow":
            if q not in quote_allow_set:
                fails["quote_not_in_allowlist"] = 1

    if _price_usd(p) <= 0:
        fails["price<=0"] = 1

    liq = _liq_usd(p)
    if not (f["liquidity_min"] <= liq <= f["liquidity_max"]):
        fails["liq_range"] = 1

    mcap = _mcap_or_fdv(p)
    if not (f["mcap_min"] <= mcap <= f["mcap_max"]):
        fails["mcap_range"] = 1

    vol24 = _vol_h24(p)
    if not (f["vol24_min"] <= vol24 <= f["vol24_max"]):
        fails["vol24_range"] = 1

    tx24 = _txns_h24(p)
    if not (f["tx24_min"] <= tx24 <= f["tx24_max"]):
        fails["tx24_range"] = 1

    age = _age_days_from_ms(p.get("pairCreatedAt"))
    if age is None or not (f["age_days_min"] <= age <= f["age_days_max"]):
        fails["age_range"] = 1

    return fails

def _similarity_score(p: Dict[str, Any], tgt: Dict[str, Any]) -> float:
    def logdist(a, b):
        a = max(a, 1e-12)
        b = max(b, 1e-12)
        return abs(math.log(a / b))

    liq, mcap, vol, tx = _liq_usd(p), _mcap_or_fdv(p), _vol_h24(p), _txns_h24(p)
    t_l, t_m, t_v, t_t = _liq_usd(tgt), _mcap_or_fdv(tgt), _vol_h24(tgt), _txns_h24(tgt)
    age, t_age = _age_days_from_ms(p.get("pairCreatedAt")), _age_days_from_ms(tgt.get("pairCreatedAt"))

    s = (
        1.5 * logdist(liq, t_l or 1.0)
        + 1.2 * logdist(mcap or 1.0, t_m or 1.0)
        + 1.0 * logdist(vol or 1.0, t_v or 1.0)
        + 0.8 * logdist(tx or 1.0, t_t or 1.0)
    )
    if age is not None and t_age is not None and t_age > 0:
        s += 0.2 * abs((age - t_age) / max(t_age, 1e-6))
    return s

def _apply_relax(filters: Dict[str, float], relax: float) -> Dict[str, float]:
    if relax == 1.0:
        return filters
    f = dict(filters)
    f["liquidity_min"] /= relax
    f["liquidity_max"] *= relax
    f["mcap_min"]      /= relax
    f["mcap_max"]      *= relax
    f["vol24_min"]     /= relax
    f["vol24_max"]     *= relax
    f["tx24_min"]       = int(f["tx24_min"] / relax)
    f["tx24_max"]       = int(f["tx24_max"] * relax)
    f["age_days_min"]  /= relax
    f["age_days_max"]  *= relax
    return f

def _adaptive_filters_around_target(tgt: Dict[str, Any]) -> Dict[str, float]:
    # Costruisce range attorno alle metriche del target con fattori elastici
    liq = max(_liq_usd(tgt), 1.0)
    mcap = max(_mcap_or_fdv(tgt), 1.0)
    vol = max(_vol_h24(tgt), 1.0)
    tx  = max(_txns_h24(tgt), 1)
    age = _age_days_from_ms(tgt.get("pairCreatedAt")) or 7.0

    def band(val, low_mult, high_mult, low_floor, high_cap):
        lo = max(val * low_mult, low_floor)
        hi = min(val * high_mult, high_cap)
        if lo > hi:
            lo, hi = hi * 0.5, hi
        return lo, hi

    liq_min, liq_max = band(liq, 0.2, 5.0, 5e4, 5e8)
    mcap_min, mcap_max = band(mcap, 0.2, 5.0, 5e6, 5e9)
    vol_min, vol_max = band(vol, 0.15, 6.0, 1e5, 2e8)
    tx_min, tx_max = band(tx, 0.1, 6.0, 200, 1_000_000)
    age_min, age_max = band(age, 0.5, 2.5, 1.0, 365.0)

    return {
        "liquidity_min": liq_min, "liquidity_max": liq_max,
        "mcap_min": mcap_min,     "mcap_max": mcap_max,
        "vol24_min": vol_min,     "vol24_max": vol_max,
        "tx24_min": int(tx_min),  "tx24_max": int(tx_max),
        "age_days_min": age_min,  "age_days_max": age_max,
    }

def select_similar(
    target_pair_addr: str,
    count: int = 50,
    chain: str = NETWORK,
    hard_filters: Optional[Dict[str, float]] = None,
    explain: bool = False,
    relax: float = 1.0,
    source: str = "both",
    hydrate_missing: bool = True,
    quote_addr: str = WBNB,
    quotes_allow: Optional[List[str]] = None,
    no_quote_filter: bool = False,
    anchors: Optional[List[str]] = None,
    diag: bool = False,
    dedup_base: bool = True,
    _tgt_obj: Optional[Dict[str, Any]] = None,  # interno, per adaptive
) -> pd.DataFrame:
    hard_filters = hard_filters or DEFAULT_FILTERS
    hf = _apply_relax(hard_filters, relax)

    if no_quote_filter:
        quote_mode, quote_single, quote_allow_set = "off", None, set()
    elif quotes_allow:
        quote_mode, quote_single, quote_allow_set = "allow", None, set(a.lower() for a in quotes_allow)
    else:
        quote_mode, quote_single, quote_allow_set = "single", (quote_addr or "").lower(), set()

    tgt = _tgt_obj or ds_get_pair(chain, target_pair_addr)

    cands: List[Dict[str, Any]] = []

    if anchors and len(anchors) > 0:
        anchors_use = [a.lower() for a in anchors]
    else:
        anchors_use = [WBNB] if source == "wbnb-only" else DEFAULT_ANCHORS

    for a in anchors_use:
        try:
            cands.extend(ds_token_pairs(a, chain=chain))  # ordine corretto
        except Exception:
            if explain:
                print(f"[warn] anchor token-pairs fallita per {a}")

    if source in ("both", "dex-only"):
        try:
            cands.extend(ds_pairs_by_dex(chain, ("pancakeswapv2", "pancakeswap")))
        except Exception:
            if explain:
                print("[warn] fetch pairs_by_dex fallita")

    seen: set = set()
    uniq: List[Dict[str, Any]] = []
    for p in cands:
        addr = (p.get("pairAddress") or "").lower()
        if not addr or addr in seen or addr == target_pair_addr.lower():
            continue
        seen.add(addr)
        if hydrate_missing:
            p = hydrate_candidate_if_needed(p, chain=chain)
        uniq.append(p)

    if explain:
        print(f"[debug] candidati totali (dedup): {len(uniq)}")

    if diag and uniq:
        from collections import Counter
        dex_ct = Counter([(p.get("dexId") or "").lower() for p in uniq]).most_common(5)
        quotes_ct = Counter([_quote_addr_of(p) for p in uniq]).most_common(8)
        print("[diag] top dex:", dex_ct)
        print("[diag] top quote token (addresses):", quotes_ct)

    agg_reasons: Dict[str, int] = {}
    rows: List[Dict[str, Any]] = []

    for p in uniq:
        fails = _filters_failures(p, hf, quote_mode, quote_single, quote_allow_set)
        if not fails:
            base = p.get("baseToken") or {}
            quote = p.get("quoteToken") or {}
            rec = {
                "pairAddress": (p.get("pairAddress") or "").lower(),
                "dexId": p.get("dexId"),
                "liquidity_usd": _liq_usd(p),
                "vol24_usd": _vol_h24(p),
                "tx24": _txns_h24(p),
                "price_usd": _price_usd(p),
                "mcap_usd": _mcap_or_fdv(p),
                "pair_created_at": p.get("pairCreatedAt"),
                "age_days": _age_days_from_ms(p.get("pairCreatedAt")),
                "base_symbol": base.get("symbol"),
                "base_address": (base.get("address") or "").lower(),
                "quote_symbol": quote.get("symbol"),
                "quote_address": (quote.get("address") or "").lower(),
            }
            rec["score"] = _similarity_score(p, tgt)
            rows.append(rec)
        else:
            if explain:
                for k in fails:
                    agg_reasons[k] = agg_reasons.get(k, 0) + 1

    if not rows:
        if explain:
            print("[explain] conteggio motivi di esclusione:")
            if agg_reasons:
                for k, v in sorted(agg_reasons.items(), key=lambda x: -x[1]):
                    print(f"  - {k}: {v}")
            else:
                print("  (nessun dettaglio disponibile)")
        raise ValueError("Nessun candidato che rispetta i filtri hard.")

    if dedup_base:
        rows = sorted(rows, key=lambda r: (-r["liquidity_usd"], r["score"]))
        best_by_base: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            b = r["base_address"]
            if b and b not in best_by_base:
                best_by_base[b] = r
        rows = list(best_by_base.values())

    df = pd.DataFrame(sorted(rows, key=lambda r: r["score"])).head(count)
    return df

# -------------------- GeckoTerminal OHLCV --------------------
def detect_parquet_engine() -> Optional[str]:
    try:
        import pyarrow  # noqa: F401
        return "pyarrow"
    except Exception:
        try:
            import fastparquet  # noqa: F401
            return "fastparquet"
        except Exception:
            return None

def save_parquet_or_csv(df: pd.DataFrame, out_parquet: str) -> Optional[str]:
    if df is None or df.empty:
        return None
    os.makedirs(os.path.dirname(out_parquet), exist_ok=True)
    engine = detect_parquet_engine()
    try:
        if engine:
            df.to_parquet(out_parquet, index=False, engine=engine)
            return out_parquet
        out_csv = out_parquet.replace(".parquet", ".csv.gz")
        df.to_csv(out_csv, index=False, compression="gzip")
        return out_csv
    except Exception:
        try:
            out_csv = out_parquet.replace(".parquet", ".csv")
            df.to_csv(out_csv, index=False)
            return out_csv
        except Exception:
            return None

def gt_fetch_ohlcv_5m(
    pool: str,
    network: str = NETWORK,
    days: int = 30,
    include_empty: bool = True,
    cache_dir: str = "data/cache",
) -> pd.DataFrame:
    end = int(datetime.now(tz=timezone.utc).timestamp())
    start = end - days * 86400
    url = f"{GT_API}/networks/{network}/pools/{pool}/ohlcv/minute"

    out: List[Tuple[int, float, float, float, float, float]] = []
    before: Optional[int] = None
    agg = 5
    guard_max_rows = int(days * 288 + 3000)

    while True:
        params = {
            "aggregate": agg,
            "limit": 1000,
            "currency": "usd",
            "include_empty_intervals": str(include_empty).lower(),
        }
        if before:
            params["before_timestamp"] = before

        try:
            data = cache_get(url, params=params, cache_dir=cache_dir, ttl=60, retries=6, timeout=40)
        except Exception as e:
            print(f"[warn] GeckoTerminal fetch fallito su {pool} (before={before}): {e}")
            break

        lst = ((data.get("data") or {}).get("attributes") or {}).get("ohlcv_list", [])
        if not lst:
            break

        lst_sorted = sorted(lst, key=lambda x: x[0])
        for row in lst_sorted:
            try:
                ts, o, h, l, c, v = row
                if start <= ts <= end:
                    out.append((int(ts), float(o), float(h), float(l), float(c), float(v)))
            except Exception:
                continue

        before = int(lst_sorted[0][0]) - 1
        if before < start or len(out) >= guard_max_rows:
            break

    if not out:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])

    df = (
        pd.DataFrame(out, columns=["ts", "open", "high", "low", "close", "volume"])
        .drop_duplicates("ts")
        .sort_values("ts")
    )

    if df.empty:
        return df

    if not include_empty:
        first, last = int(df["ts"].min()), int(df["ts"].max())
        full = list(range(first, last + 1, 300))
        df = (
            df.set_index("ts").reindex(full).ffill().fillna({"volume": 0.0}).rename_axis("ts").reset_index()
        )

    df = df[(df["close"] > 0) & (df["high"] >= df["low"])]
    return df

def append_sqlite(df: pd.DataFrame, chain: str, pool: str, db_path: str = "data/predictor.db"):
    if df is None or df.empty:
        return
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = sqlite3.connect(db_path)
    try:
        df2 = df.copy()
        df2["chain"] = chain
        df2["pool"] = pool
        df2 = df2[["chain", "pool", "ts", "open", "high", "low", "close", "volume"]]
        df2.to_sql("candles", con, if_exists="append", index=False)
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_candles ON candles(chain,pool,ts)")
        con.commit()
    finally:
        con.close()

# -------------------- Orchestrator + Fallbacks --------------------
def try_select_with_fallbacks(
    target_pair: str,
    count: int,
    network: str,
    filters: Dict[str, float],
    source: str,
    quote_addr: str,
    quotes_allow: Optional[List[str]],
    no_quote_filter: bool,
    anchors: Optional[List[str]],
    diag: bool,
    dedup_base: bool,
    explain: bool,
    auto_fallback: bool,
    adaptive_filters: bool,
) -> pd.DataFrame:
    # carica target una volta sola
    tgt = ds_get_pair(network, target_pair)

    # 1) tentativo base
    try:
        return select_similar(
            target_pair_addr=target_pair, count=count, chain=network,
            hard_filters=filters, explain=explain, relax=1.0, source=source,
            hydrate_missing=True, quote_addr=quote_addr, quotes_allow=quotes_allow,
            no_quote_filter=no_quote_filter, anchors=anchors, diag=diag, dedup_base=dedup_base, _tgt_obj=tgt
        )
    except Exception as e1:
        if not auto_fallback:
            raise

    # 2) relax progressivo
    for relax in (1.2, 1.5, 2.0):
        try:
            print(f"[fallback] relax filtri -> x{relax}")
            return select_similar(
                target_pair_addr=target_pair, count=count, chain=network,
                hard_filters=filters, explain=True, relax=relax, source=source,
                hydrate_missing=True, quote_addr=quote_addr, quotes_allow=quotes_allow,
                no_quote_filter=no_quote_filter, anchors=anchors, diag=diag, dedup_base=dedup_base, _tgt_obj=tgt
            )
        except Exception:
            pass

    # 3) disattiva quote filter
    try:
        print("[fallback] disattiva filtro quote")
        return select_similar(
            target_pair_addr=target_pair, count=count, chain=network,
            hard_filters=filters, explain=True, relax=2.0, source=source,
            hydrate_missing=True, quote_addr=quote_addr, quotes_allow=quotes_allow,
            no_quote_filter=True, anchors=anchors, diag=diag, dedup_base=dedup_base, _tgt_obj=tgt
        )
    except Exception:
        pass

    # 4) filtri adattivi centrati sul target
    if adaptive_filters:
        af = _adaptive_filters_around_target(tgt)
        print("[fallback] filtri adattivi centrati sul target:", json.dumps(af, indent=2))
        # tenta varie combinazioni di sorgenti
        for src in ("both", "dex-only", "wbnb-only"):
            for nq in (True, False):
                try:
                    print(f"[fallback] source={src}, no_quote_filter={nq}")
                    return select_similar(
                        target_pair_addr=target_pair, count=count, chain=network,
                        hard_filters=af, explain=True, relax=1.0, source=src,
                        hydrate_missing=True, quote_addr=quote_addr, quotes_allow=quotes_allow,
                        no_quote_filter=nq, anchors=anchors, diag=diag, dedup_base=dedup_base, _tgt_obj=tgt
                    )
                except Exception:
                    continue

    # 5) estrema: relax molto ampio + no_quote_filter + both
    try:
        print("[fallback] estrema: relax 3.0 + no_quote_filter + both")
        return select_similar(
            target_pair_addr=target_pair, count=count, chain=network,
            hard_filters=filters, explain=True, relax=3.0, source="both",
            hydrate_missing=True, quote_addr=quote_addr, quotes_allow=quotes_allow,
            no_quote_filter=True, anchors=anchors, diag=diag, dedup_base=dedup_base, _tgt_obj=tgt
        )
    except Exception as e_final:
        raise ValueError("Nessun candidato trovato anche con fallback. Prova ad aumentare --days, cambiare --source o usare --anchors custom.") from e_final

def build_dataset(
    target_pair: str,
    count: int = 50,
    days: int = 30,
    outdir: str = "data/gold",
    db: str = "data/predictor.db",
    cache_dir: str = "data/cache",
    network: str = NETWORK,
    save_list_path: str = "data/pools_pancake_list.csv",
    filters: Optional[Dict[str, float]] = None,
    explain: bool = False,
    relax: float = 1.0,  # usato solo nel tentativo base (ma i fallback ne faranno altri)
    source: str = "both",
    quote_addr: str = WBNB,
    quotes_allow: Optional[List[str]] = None,
    no_quote_filter: bool = False,
    anchors: Optional[List[str]] = None,
    diag: bool = False,
    dedup_base: bool = True,
    auto_fallback: bool = True,
    adaptive_filters: bool = True,
) -> Tuple[pd.DataFrame, List[str]]:
    os.makedirs(outdir, exist_ok=True)
    os.makedirs(os.path.dirname(save_list_path), exist_ok=True)
    filters = filters or DEFAULT_FILTERS

    print("[1/4] Selezione PCS v2 (BSC) con filtri hard...")
    # Prova con fallback automatici
    df_pools = try_select_with_fallbacks(
        target_pair=target_pair, count=count, network=network, filters=filters,
        source=source, quote_addr=quote_addr, quotes_allow=quotes_allow, no_quote_filter=no_quote_filter,
        anchors=anchors, diag=diag, dedup_base=dedup_base, explain=explain,
        auto_fallback=auto_fallback, adaptive_filters=adaptive_filters
    )

    df_pools.to_csv(save_list_path, index=False)
    print(f"Trovati {len(df_pools)} pool (saved: {save_list_path})")

    print("[2/4] Ingest OHLCV 5m da GeckoTerminal...")
    saved_files: List[str] = []
    n = len(df_pools)
    for i, row in enumerate(df_pools.itertuples(index=False), start=1):
        pair = row.pairAddress
        subdir = os.path.join(outdir, network, f"POOL_{pair[:6]}")
        out_parquet = os.path.join(subdir, "candles_5m.parquet")

        if os.path.exists(out_parquet) or os.path.exists(out_parquet.replace(".parquet", ".csv.gz")) \
           or os.path.exists(out_parquet.replace(".parquet", ".csv")):
            print(f"  [{i}/{n}] {pair}  (skip: già presente)")
            continue

        try:
            print(f"  [{i}/{n}] {pair}  ... fetching")
            df = gt_fetch_ohlcv_5m(pair, network=network, days=days, include_empty=True, cache_dir=cache_dir)
            saved = save_parquet_or_csv(df, out_parquet)
            append_sqlite(df, network, pair, db_path=db)
            print(f"       -> rows={len(df)} saved={saved}")
            if saved:
                saved_files.append(saved)
        except Exception as e:
            print(f"       !! errore su {pair}: {e}")
            if os.environ.get("DEBUG_TRACE", "0") == "1":
                traceback.print_exc()

    print("[3/4] Completato ingest per i pool selezionati.")
    print("[4/4] Done.")
    return df_pools, saved_files

# -------------------- CLI --------------------
def parse_args():
    ap = argparse.ArgumentParser(
        description="Dataset PCS v2 (BSC) simili al target (OHLCV 5m) con filtri robusti e fallback automatici."
    )
    ap.add_argument("--target_pair", required=True, help="Address della pair target (pool) su BSC.")
    ap.add_argument("--count", type=int, default=50, help="Numero massimo di pool simili da tenere.")
    ap.add_argument("--days", type=int, default=30, help="Giorni di storico OHLCV 5m da scaricare.")
    ap.add_argument("--outdir", default="data/gold", help="Directory base di output per i file OHLCV.")
    ap.add_argument("--db", default="data/predictor.db", help="Percorso DB SQLite per salvare le candele.")
    ap.add_argument("--cache_dir", default="data/cache", help="Directory per la cache HTTP.")
    ap.add_argument("--explain", action="store_true", help="Mostra statistiche sui filtri che escludono i pool.")
    ap.add_argument("--relax", type=float, default=1.0, help="Allarga/Restringe i filtri hard (primo tentativo).")
    ap.add_argument("--source", choices=["both", "wbnb-only", "dex-only"], default="both",
                    help="Sorgenti dei candidati: solo anchor WBNB, solo endpoint DEX, o entrambi.")
    ap.add_argument("--quote_addr", default=WBNB, help="Address della quote in modalità 'single'.")
    ap.add_argument("--quotes_allow", default="",
                    help="Comma-separated allowlist di quote addresses (usa con PCS).")
    ap.add_argument("--no_quote_filter", action="store_true",
                    help="Disattiva completamente il filtro sulla quote (solo tentativo iniziale).")
    ap.add_argument("--anchors", default="",
                    help="Comma-separated token addresses per ampliare i candidati (default: WBNB,USDT,USDC,DAI).")
    ap.add_argument("--diag", action="store_true", help="Stampa statistiche su dex e quote tokens.")
    ap.add_argument("--no_dedup_base", action="store_true", help="Non deduplicare per base token.")

    # fallback controls
    ap.add_argument("--no_auto_fallback", action="store_true", help="Disattiva i fallback automatici.")
    ap.add_argument("--no_adaptive_filters", action="store_true", help="Disattiva i filtri adattivi sul target.")

    # override filtri hard
    for k, v in DEFAULT_FILTERS.items():
        t = float if isinstance(v, float) else int
        ap.add_argument(f"--{k}", type=t, default=v)

    return ap.parse_args()

def main():
    args = parse_args()
    filters = {
        "liquidity_min": args.liquidity_min, "liquidity_max": args.liquidity_max,
        "mcap_min": args.mcap_min, "mcap_max": args.mcap_max,
        "vol24_min": args.vol24_min, "vol24_max": args.vol24_max,
        "tx24_min": args.tx24_min, "tx24_max": args.tx24_max,
        "age_days_min": args.age_days_min, "age_days_max": args.age_days_max,
    }

    quotes_allow = [a.strip().lower() for a in args.quotes_allow.split(",") if a.strip()] or None
    anchors = [a.strip().lower() for a in args.anchors.split(",") if a.strip()] or None

    try:
        pools_df, saved = build_dataset(
            target_pair=(args.target_pair or "").lower(),
            count=args.count,
            days=args.days,
            outdir=args.outdir,
            db=args.db,
            cache_dir=args.cache_dir,
            network=NETWORK,
            save_list_path=os.path.join("data", "pools_pancake_list.csv"),
            filters=filters,
            explain=args.explain,
            relax=args.relax,
            source=args.source,
            quote_addr=(args.quote_addr or "").lower(),
            quotes_allow=quotes_allow,
            no_quote_filter=args.no_quote_filter,
            anchors=anchors,
            diag=args.diag,
            dedup_base=not args.no_dedup_base,
            auto_fallback=not args.no_auto_fallback,
            adaptive_filters=not args.no_adaptive_filters,
        )
        print("\n=== POOLS SELEZIONATI (top 10) ===")
        try:
            print(pools_df.head(10).to_string(index=False))
        except Exception:
            print(pools_df.head(10))
    except Exception as e:
        print(f"Errore: {e}", file=sys.stderr)
        if os.environ.get("DEBUG_TRACE", "0") == "1":
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
