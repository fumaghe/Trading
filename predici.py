#!/usr/bin/env python3
# predici.py
# Unico script: discovery pool simili + OHLCV 5m + forecasting 24h/5m + chart
# Uso rapido:
#   python predici.py 0xPOOLADDRESS
# Output:
#   data/predictions/<chain>/POOL_<xxxxxx>/forecast_5m_<YYYYMMDD_%H%M>.csv
#   data/predictions/<chain>/POOL_<xxxxxx>/model_card.txt
#   data/pools_list.csv
#
# Requisiti:
#   pip install requests pandas numpy
#   (opzionali) pip install lightgbm xgboost scikit-learn

"""
python predici.py 0xee2f63a49cb190962619183103d25af14ce5f538 --days 60 --count_sim 60 --relax 4.0 --chain bsc --allow_any_dex --no_quote_filter --save_ohlcv
"""
import os, sys, time, json, math, argparse, hashlib, sqlite3, traceback, warnings
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ---- clamp returns per stabilità ----
CLIP_RET = 0.20          # max ±20% per step (5m) dopo la fase iniziale
RET_WINSOR_LO = 0.003    # winsorization: 0.3° percentile
RET_WINSOR_HI = 0.997    # winsorization: 99.7° percentile

# =================== Costanti e default ===================
DEX_API = "https://api.dexscreener.com"
GT_API  = "https://api.geckoterminal.com/api/v2"

DEFAULT_HEADERS = {
    "accept": "application/json",
    "user-agent": "dex-forecaster/3.0 (+cli)",
    "accept-language": "en-US,en;q=0.9",
}

NETWORK = "bsc"  # default rete
DRIFT_CAP_24H = 3.0
# Quote tokens comuni su BSC
WBNB = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c".lower()
USDT = "0x55d398326f99059ff775485246999027b3197955".lower()
USDC = "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d".lower()
DAI  = "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3".lower()
DEFAULT_ANCHORS = [WBNB, USDT, USDC, DAI]

DEFAULT_PCS_DEX_IDS = (
    "pancakeswap","pancakeswapv2","pancakeswap-v2",
    "pancakeswapv3","pancakeswap-v3"
)

# Rate limit per host
HOST_MIN_INTERVAL = {
    "api.dexscreener.com": 0.15,
    "api.geckoterminal.com": 2.2,
}
_last_call_ts: Dict[str, float] = {}

# ML backend: LightGBM → XGBoost → RandomForest
Model = None
try:
    import lightgbm as lgb  # type: ignore
    Model = "lgb"
except Exception:
    try:
        import xgboost as xgb  # type: ignore
        Model = "xgb"
    except Exception:
        from sklearn.ensemble import RandomForestRegressor  # type: ignore
        Model = "rf"

warnings.filterwarnings("ignore", category=UserWarning)

# =================== Utils HTTP (cache + RL + retry) ===================
def _rate_limit_for(url: str):
    host = urlparse(url).netloc
    min_interval = HOST_MIN_INTERVAL.get(host, 0.2)
    last = _last_call_ts.get(host, 0.0)
    now = time.time()
    wait = last + min_interval - now
    if wait > 0:
        time.sleep(wait)
    _last_call_ts[host] = time.time()

def cache_get(url: str, params: Optional[Dict[str, Any]] = None,
              headers: Optional[Dict[str, str]] = None,
              cache_dir: str = "data/cache",
              ttl: int = 60, retries: int = 5, timeout: int = 30) -> Any:
    os.makedirs(cache_dir, exist_ok=True)
    key = hashlib.sha256((url + json.dumps(params or {}, sort_keys=True)).encode()).hexdigest()
    path = os.path.join(cache_dir, key + ".json")

    # Cache hit
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
                time.sleep(backoff); backoff = min(backoff * 2, 16); continue
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
                raise RuntimeError(f"HTTP error for {url}: {e}") from e
            time.sleep(backoff); backoff = min(backoff * 2, 16)
    raise RuntimeError(f"HTTP failed repeatedly for {url}")

# =================== Helper numerici ===================
def _safe_float(x, default=0.0):
    try:
        if x in (None, "", "null"): return default
        return float(x)
    except Exception:
        return default

def _safe_int(x, default=0):
    try:
        if x in (None, "", "null"): return default
        return int(float(x))
    except Exception:
        return default

def _age_days_from_ms(created_at):
    if created_at in (None, "", 0): return None
    try:
        v = int(created_at)
    except Exception:
        return None
    if v < 10**12: v *= 1000  # convert s→ms se serve
    now_ms = int(time.time() * 1000)
    return (now_ms - v) / (1000 * 60 * 60 * 24)

def _mcap_or_fdv(p: Dict[str, Any]) -> float:
    m = p.get("marketCap")
    if m is None: m = p.get("fdv")
    return _safe_float(m, 0.0)

def _vol_h24(p: Dict[str, Any]) -> float:
    v = p.get("volume")
    if isinstance(v, dict):
        return _safe_float(v.get("h24"), 0.0)
    for k in ("volume24h","vol24h"):
        if k in p: return _safe_float(p.get(k), 0.0)
    return 0.0

def _txns_h24(p: Dict[str, Any]) -> int:
    t = p.get("txns")
    if isinstance(t, dict):
        h24 = t.get("h24")
        if isinstance(h24, dict):
            return _safe_int(h24.get("buys"),0) + _safe_int(h24.get("sells"),0)
        if isinstance(h24, (int,float)):
            return _safe_int(h24,0)
    for k in ("txns24h","transactions24h","txns24","transactions"):
        v = p.get(k)
        if isinstance(v, dict):
            return _safe_int(v.get("buys"),0) + _safe_int(v.get("sells"),0)
        if v is not None:
            return _safe_int(v,0)
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

# =================== DexScreener ===================
def ds_get_pair(chain: str, pair_addr: str) -> Dict[str, Any]:
    data = cache_get(f"{DEX_API}/latest/dex/pairs/{chain}/{pair_addr}", ttl=60, retries=6)
    pairs = data.get("pairs", []) or []
    if not pairs:
        raise ValueError(f"Nessun pair trovato per {chain}/{pair_addr}.")
    return pairs[0]

def ds_token_pairs(chain: str, token_addr: str) -> List[Dict[str, Any]]:
    data = cache_get(f"{DEX_API}/token-pairs/v1/{chain}/{token_addr}", ttl=120, retries=6)
    if isinstance(data, dict) and "pairs" in data: return data.get("pairs", []) or []
    if isinstance(data, list): return data
    return []

def ds_pairs_by_dex(chain: str, dex_ids: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for d in dex_ids:
        data = cache_get(f"{DEX_API}/latest/dex/pairs/{chain}/{d}", ttl=120, retries=6)
        out.extend(data.get("pairs", []) or [])
    return out

# =================== Similarità & filtri ===================
def _similarity_score(p: Dict[str, Any], tgt: Dict[str, Any]) -> float:
    def logdist(a,b):
        a=max(a,1e-12); b=max(b,1e-12); return abs(math.log(a/b))
    liq, mcap, vol, tx  = _liq_usd(p), _mcap_or_fdv(p), _vol_h24(p), _txns_h24(p)
    t_l, t_m, t_v, t_t  = _liq_usd(tgt), _mcap_or_fdv(tgt), _vol_h24(tgt), _txns_h24(tgt)
    age, t_age          = _age_days_from_ms(p.get("pairCreatedAt")), _age_days_from_ms(tgt.get("pairCreatedAt"))
    s = 1.4*logdist(liq, t_l or 1.0) + 1.1*logdist(mcap or 1.0, t_m or 1.0) + 1.0*logdist(vol or 1.0, t_v or 1.0) + 0.7*logdist(tx or 1.0, t_t or 1.0)
    if age is not None and t_age is not None and t_age > 0:
        s += 0.2*abs((age - t_age)/max(t_age,1e-6))
    return s

def _band(val, low_mult, high_mult, low_floor, high_cap):
    val = max(val, 1e-12)
    lo = max(val * low_mult, low_floor)
    hi = min(val * high_mult, high_cap)
    if lo > hi:
        lo, hi = hi * 0.5, hi
    return lo, hi

def adaptive_filters(tgt: Dict[str, Any], relax: float) -> Dict[str, float]:
    liq = max(_liq_usd(tgt), 1.0)
    mcap = max(_mcap_or_fdv(tgt), 1.0)
    vol = max(_vol_h24(tgt), 1.0)
    tx  = max(_txns_h24(tgt), 1)
    age = _age_days_from_ms(tgt.get("pairCreatedAt")) or 7.0

    liq_min, liq_max = _band(liq, 0.1/relax, 8.0*relax, 2e4, 1e9)
    mcap_min,mcap_max= _band(mcap,0.1/relax, 8.0*relax, 1e6,  1e10)
    vol_min, vol_max = _band(vol, 0.1/relax, 10.0*relax,1e4,  5e8)
    tx_min,  tx_max  = _band(tx,  0.05/relax,10.0*relax,50,   2_000_000)
    age_min, age_max = _band(age, 0.4/relax, 3.0*relax, 0.5,  720.0)

    return {
        "liquidity_min": liq_min, "liquidity_max": liq_max,
        "mcap_min": mcap_min,     "mcap_max": mcap_max,
        "vol24_min": vol_min,     "vol24_max": vol_max,
        "tx24_min": int(tx_min),  "tx24_max": int(tx_max),
        "age_days_min": age_min,  "age_days_max": age_max,
    }

def filter_reasons(p: Dict[str, Any], f: Dict[str, float],
                   quote_mode: str, quote_single: Optional[str], quote_allow: "set[str]",
                   allowed_dex: Optional[set]) -> Dict[str, int]:
    fails: Dict[str, int] = {}

    if allowed_dex is not None:
        d = (p.get("dexId") or "").lower()
        if d not in allowed_dex:
            fails["dex_not_allowed"] = 1

    if quote_mode != "off":
        q = _quote_addr_of(p)
        if quote_mode == "single":
            if quote_single and q != quote_single:
                fails["quote!=selected"] = 1
        elif quote_mode == "allow":
            if q not in quote_allow:
                fails["quote_not_in_allow"] = 1

    if _price_usd(p) <= 0: fails["price<=0"] = 1

    liq = _liq_usd(p)
    if not (f["liquidity_min"] <= liq <= f["liquidity_max"]): fails["liq_range"] = 1

    mcap = _mcap_or_fdv(p)
    if not (f["mcap_min"] <= mcap <= f["mcap_max"]): fails["mcap_range"] = 1

    vol24 = _vol_h24(p)
    if not (f["vol24_min"] <= vol24 <= f["vol24_max"]): fails["vol24_range"] = 1

    tx24 = _txns_h24(p)
    if not (f["tx24_min"] <= tx24 <= f["tx24_max"]): fails["tx24_range"] = 1

    age = _age_days_from_ms(p.get("pairCreatedAt"))
    if age is None or not (f["age_days_min"] <= age <= f["age_days_max"]): fails["age_range"] = 1

    return fails

def hydrate_if_needed(p: Dict[str, Any], chain: str) -> Dict[str, Any]:
    needs = False
    if p.get("pairCreatedAt") in (None, "", 0): needs = True
    if p.get("txns") in (None, {}) and all(k not in p for k in ("txns24h","transactions24h","txns24","transactions")): needs = True
    if (p.get("volume") or {}).get("h24") in (None, 0): needs = True
    if p.get("marketCap") is None and p.get("fdv") is None: needs = True
    if not needs: return p
    try:
        addr = p.get("pairAddress") or ""
        if not addr: return p
        fresh = ds_get_pair(chain, addr)
        for k in ("pairCreatedAt","txns","volume","marketCap","fdv","priceUsd","liquidity"):
            if p.get(k) in (None, "", 0, {}):
                if fresh.get(k) not in (None, "", 0, {}): p[k] = fresh.get(k)
    except Exception:
        pass
    return p

# =================== Discovery + selezione ===================
def discover_candidates(chain: str, target: Dict[str, Any],
                        dex_ids: Optional[List[str]],
                        anchors: Optional[List[str]], source_mode: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    # 1) coppie del base e del quote del target
    base = (target.get("baseToken") or {}).get("address") or ""
    quote = (target.get("quoteToken") or {}).get("address") or ""
    for tok in [base, quote]:
        if tok:
            try:
                out.extend(ds_token_pairs(chain, tok))
            except Exception:
                pass

    # 2) anchor quote tokens
    anchors_use = [a.lower() for a in (anchors or DEFAULT_ANCHORS)]
    for a in anchors_use:
        try:
            out.extend(ds_token_pairs(chain, a))
        except Exception:
            pass

    # 3) dexIds (PCS v2+v3 by default)
    if source_mode in ("both","dex-only") and dex_ids:
        try:
            out.extend(ds_pairs_by_dex(chain, dex_ids))
        except Exception:
            pass

    return out

def select_similar(
    chain: str,
    target_pair_addr: str,
    count: int,
    relax: float,
    dex_ids: Optional[List[str]],
    allow_any_dex: bool,
    quotes_allow: Optional[List[str]],
    no_quote_filter: bool,
    anchors: Optional[List[str]],
    source_mode: str,
    dedup_base: bool,
    explain: bool,
    diag: bool
) -> pd.DataFrame:

    tgt = ds_get_pair(chain, target_pair_addr)
    filters = adaptive_filters(tgt, relax=relax)

    # quote filter mode
    if no_quote_filter:
        quote_mode, quote_single, quote_allow = "off", None, set()
    elif quotes_allow:
        quote_mode, quote_single, quote_allow = "allow", None, set(a.lower() for a in quotes_allow)
    else:
        # usa quote del target
        quote_mode, quote_single, quote_allow = "single", (_quote_addr_of(tgt) or "").lower(), set()

    # dex filter
    allowed_dex = None if allow_any_dex else set([d.lower() for d in (dex_ids or list(DEFAULT_PCS_DEX_IDS))])

    # discovery
    cands_raw = discover_candidates(chain, tgt, dex_ids, anchors, source_mode)

    # dedup + hydrate
    seen = set()
    cands: List[Dict[str, Any]] = []
    for p in cands_raw:
        addr = (p.get("pairAddress") or "").lower()
        if not addr or addr in seen or addr == target_pair_addr.lower():
            continue
        seen.add(addr)
        cands.append(hydrate_if_needed(p, chain))

    if explain:
        print(f"[debug] candidati (dedup): {len(cands)}")

    if diag and cands:
        from collections import Counter
        dex_ct = Counter([(p.get("dexId") or "").lower() for p in cands]).most_common(8)
        quotes_ct = Counter([_quote_addr_of(p) for p in cands]).most_common(8)
        print("[diag] top dex:", dex_ct)
        print("[diag] top quote token:", quotes_ct)

    rows: List[Dict[str, Any]] = []
    reasons: Dict[str, int] = {}

    for p in cands:
        fails = filter_reasons(p, filters, quote_mode, quote_single, quote_allow, allowed_dex)
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
                    reasons[k] = reasons.get(k, 0) + 1

    if not rows:
        if explain and reasons:
            print("[explain] motivi esclusione (conteggio):")
            for k, v in sorted(reasons.items(), key=lambda x: -x[1]):
                print(f"  - {k}: {v}")
            print("[hint] Prova ad aumentare --relax, usare --no_quote_filter oppure --allow_any_dex.")
        raise ValueError("Nessun candidato ha passato i filtri.")

    # dedup per base token (facoltativo)
    if dedup_base:
        rows = sorted(rows, key=lambda r: (r["score"], -r["liquidity_usd"]))
        best_by_base: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            b = r["base_address"]
            if b and b not in best_by_base:
                best_by_base[b] = r
        rows = list(best_by_base.values())

    df = pd.DataFrame(sorted(rows, key=lambda r: r["score"])).head(count)
    return df

# =================== GeckoTerminal OHLCV + persistenza ===================
def detect_parquet_engine() -> Optional[str]:
    try:
        import pyarrow  # noqa
        return "pyarrow"
    except Exception:
        try:
            import fastparquet  # noqa
            return "fastparquet"
        except Exception:
            return None

def save_parquet_or_csv(df: pd.DataFrame, out_path: str) -> Optional[str]:
    if df is None or df.empty: return None
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    engine = detect_parquet_engine()
    try:
        if engine:
            df.to_parquet(out_path, index=False, engine=engine)
            return out_path
        out_csv = out_path.replace(".parquet", ".csv.gz")
        df.to_csv(out_csv, index=False, compression="gzip")
        return out_csv
    except Exception:
        try:
            out_csv = out_path.replace(".parquet", ".csv")
            df.to_csv(out_csv, index=False)
            return out_csv
        except Exception:
            return None

def gt_fetch_ohlcv_5m(pool: str, network: str, days: int,
                      include_empty: bool = True, cache_dir: str = "data/cache") -> pd.DataFrame:
    end = int(datetime.now(tz=timezone.utc).timestamp())
    start = end - days * 86400
    url = f"{GT_API}/networks/{network}/pools/{pool}/ohlcv/minute"
    out: List[Tuple[int, float, float, float, float, float]] = []
    before: Optional[int] = None
    agg = 5  # 5m
    guard_max_rows = int(days * 288 + 3000)

    while True:
        params = {
            "aggregate": agg,
            "limit": 1000,
            "currency": "usd",
            "include_empty_intervals": str(include_empty).lower()
        }
        if before: params["before_timestamp"] = before
        try:
            data = cache_get(url, params=params, cache_dir=cache_dir, ttl=60, retries=6, timeout=40)
        except Exception as e:
            print(f"[warn] GeckoTerminal fetch fallito su {pool} (before={before}): {e}")
            break
        lst = ((data.get("data") or {}).get("attributes") or {}).get("ohlcv_list", [])
        if not lst: break
        lst_sorted = sorted(lst, key=lambda x: x[0])
        for row in lst_sorted:
            try:
                ts,o,h,l,c,v = row
                if start <= ts <= end:
                    out.append((int(ts), float(o), float(h), float(l), float(c), float(v)))
            except Exception:
                continue
        before = int(lst_sorted[0][0]) - 1
        if before < start or len(out) >= guard_max_rows:
            break

    if not out:
        return pd.DataFrame(columns=["ts","open","high","low","close","volume"])

    df = (pd.DataFrame(out, columns=["ts","open","high","low","close","volume"])
            .drop_duplicates("ts")
            .sort_values("ts"))
    df = df[(df["close"] > 0) & (df["high"] >= df["low"])]
    return df

def append_sqlite(df: pd.DataFrame, chain: str, pool: str, db_path: str):
    if df is None or df.empty:
        return

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = sqlite3.connect(db_path)
    try:
        con.execute("""
        CREATE TABLE IF NOT EXISTS candles (
            chain  TEXT NOT NULL,
            pool   TEXT NOT NULL,
            ts     INTEGER NOT NULL,
            open   REAL,
            high   REAL,
            low    REAL,
            close  REAL,
            volume REAL,
            PRIMARY KEY (chain, pool, ts)
        )
        """)
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_candles ON candles(chain,pool,ts)")

        df2 = df.copy()
        df2["chain"] = chain
        df2["pool"]  = pool
        df2 = df2[["chain","pool","ts","open","high","low","close","volume"]]
        df2 = df2.drop_duplicates(subset=["chain","pool","ts"])

        rows = list(df2.itertuples(index=False, name=None))
        con.executemany("""
            INSERT OR IGNORE INTO candles
            (chain, pool, ts, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)

        con.commit()
    finally:
        con.close()

# =================== Feature engineering & metrics ===================
def to_iso(ts: int):
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

def time_features(ts: pd.Series) -> pd.DataFrame:
    dt = pd.to_datetime(ts, unit="s", utc=True)
    minute_of_day = dt.dt.hour * 60 + dt.dt.minute
    rad = 2 * np.pi * (minute_of_day / (24 * 60))
    return pd.DataFrame({
        "sin_day": np.sin(rad),
        "cos_day": np.cos(rad),
        "dow": dt.dt.weekday,
    }, index=ts.index)

def add_lags_rolls(df: pd.DataFrame, col="close", lag_list=(1,2,3,6,12,24,36,72,144), rolls=(3,12,36,144)):
    out = df.copy()
    for L in lag_list:
        out[f"{col}_lag{L}"] = out[col].shift(L)
    for R in rolls:
        out[f"{col}_sma{R}"] = out[col].rolling(R).mean()
        out[f"{col}_std{R}"] = out[col].rolling(R).std()
    out["ret"] = np.log(out[col]).diff()
    for L in (1,2,3,6,12,24,36,72,144):
        out[f"ret_lag{L}"] = out["ret"].shift(L)
    for R in (3,12,36,144):
        out[f"ret_sma{R}"] = out["ret"].rolling(R).mean()
        out[f"ret_std{R}"] = out["ret"].rolling(R).std()
    if "volume" in out.columns:
        for L in (1,2,3,6,12,24,36,72,144):
            out[f"vol_lag{L}"] = out["volume"].shift(L)
        for R in (3,12,36,144):
            out[f"vol_sma{R}"] = out["volume"].rolling(R).mean()
            out[f"vol_std{R}"] = out["volume"].rolling(R).std()
    return out

def mae(y, yhat): return float(np.mean(np.abs(y - yhat)))
def rmse(y, yhat): return float(np.sqrt(np.mean((y - yhat)**2)))

def mape(y, yhat):
    # ATTENZIONE: per i returns può esplodere. Usarlo solo in spazio prezzo.
    return float(np.mean(np.abs((y - yhat) / np.clip(np.abs(y), 1e-8, None)))) * 100.0

def smape(y, yhat):
    num = np.abs(y - yhat)
    den = (np.abs(y) + np.abs(yhat)) / 2.0
    return float(np.mean(num / np.clip(den, 1e-8, None))) * 100.0

# =================== Dataset target + exogenous ===================
def fetch_target_and_exo(target_pair: str, chain: str, days: int, sim_df: pd.DataFrame, top_k_exo: int = 10) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series]:
    df_tgt = gt_fetch_ohlcv_5m(target_pair, network=chain, days=days, include_empty=True)
    if df_tgt is None or df_tgt.empty:
        raise RuntimeError("Nessun OHLCV per il target.")
    df_tgt = df_tgt.sort_values("ts").drop_duplicates("ts")

    exo_series = []
    for addr in (sim_df["pairAddress"].head(top_k_exo) if "pairAddress" in sim_df.columns else []):
        if addr.lower() == target_pair.lower():
            continue
        try:
            df = gt_fetch_ohlcv_5m(addr, network=chain, days=days, include_empty=True)
            if df is None or df.empty:
                continue
            df = df[["ts","close"]].drop_duplicates("ts").sort_values("ts")
            df["ret"] = np.log(df["close"]).diff()
            exo_series.append(df[["ts","ret"]].rename(columns={"ret":f"r_{addr[:6]}"}))
        except Exception:
            continue

    if not exo_series:
        df_exo = pd.DataFrame({"ts": df_tgt["ts"].values, "exo_med_ret": 0.0})
    else:
        df_exo = None
        for s in exo_series:
            if df_exo is None: df_exo = s.copy()
            else: df_exo = pd.merge(df_exo, s, on="ts", how="outer")
        df_exo = df_exo.sort_values("ts")
        df_exo = df_exo.set_index("ts").reindex(df_tgt["ts"]).ffill().bfill().reset_index().rename(columns={"index":"ts"})
        df_exo["exo_med_ret"] = df_exo.drop(columns=["ts"]).median(axis=1, skipna=True).fillna(0.0)
        df_exo = df_exo[["ts","exo_med_ret"]]

    dt = pd.to_datetime(df_exo["ts"], unit="s", utc=True)
    mof = dt.dt.hour * 60 + dt.dt.minute
    df_exo = df_exo.copy()
    df_exo["mof"] = mof
    exo_seasonal = df_exo.groupby("mof")["exo_med_ret"].median()
    exo_seasonal = exo_seasonal.reindex(range(1440)).fillna(df_exo["exo_med_ret"].median())

    return df_tgt, df_exo[["ts","exo_med_ret"]], exo_seasonal

def make_supervised(df_tgt: pd.DataFrame, df_exo: pd.DataFrame) -> pd.DataFrame:
    df = df_tgt.merge(df_exo, on="ts", how="left")
    df["exo_med_ret"] = df["exo_med_ret"].fillna(0.0)

    # feature base
    df = add_lags_rolls(df, col="close")
    for L in (1,2,3,6,12,24,36,72,144):
        df[f"exo_lag{L}"] = df["exo_med_ret"].shift(L)
    tf = time_features(df["ts"])
    df = pd.concat([df.reset_index(drop=True), tf.reset_index(drop=True)], axis=1)

    # target = log-return (t -> t+1)
    df["y"] = np.log(df["close"]).shift(-1) - np.log(df["close"])

    # winsorization SIMMETRICA + clamp duro
    # evitiamo bias: tagliamo in base al quantile dell'ampiezza |y|
    y_abs_q = df["y"].abs().quantile(RET_WINSOR_HI)
    y = df["y"].clip(lower=-float(y_abs_q), upper=float(y_abs_q))
    y = y.clip(-CLIP_RET, CLIP_RET)
    df["y"] = y

    # pulizia finale
    df = df.replace([np.inf, -np.inf], np.nan).dropna().reset_index(drop=True)
    df = df.loc[:, ~df.columns.duplicated()]
    return df

# =================== Modeling ===================
def fit_model(df: pd.DataFrame):
    val_len = min(4032, max(576, int(len(df)*0.15)))  # >=2 giorni
    Xcols = [c for c in df.columns if c not in ("ts","open","high","low","close","volume","y")]
    Xcols = list(dict.fromkeys(Xcols))

    X_train = df.iloc[:-val_len][Xcols]
    y_train = df.iloc[:-val_len]["y"].values
    X_val   = df.iloc[-val_len:][Xcols]
    y_val   = df.iloc[-val_len:]["y"].values

    if Model == "lgb":
        import lightgbm as lgb
        model = lgb.LGBMRegressor(
            n_estimators=1200,
            learning_rate=0.03,
            max_depth=-1,
            num_leaves=64,
            subsample=0.9,
            colsample_bytree=0.9,
            reg_alpha=0.1,
            reg_lambda=1.0,
            min_child_samples=16,
            random_state=42
        )
        model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            eval_metric="l2",
            callbacks=[lgb.log_evaluation(period=0)]
        )
    elif Model == "xgb":
        import xgboost as xgb
        model = xgb.XGBRegressor(
            n_estimators=1200,
            learning_rate=0.03,
            max_depth=8,
            subsample=0.9,
            colsample_bytree=0.9,
            reg_alpha=0.1,
            reg_lambda=1.0,
            random_state=42,
            tree_method="hist"
        )
        model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
    else:
        from sklearn.ensemble import RandomForestRegressor
        model = RandomForestRegressor(
            n_estimators=600,
            max_depth=None,
            min_samples_leaf=2,
            n_jobs=-1,
            random_state=42
        )
        model.fit(X_train, y_train)

    # qualità su validation (returns)
    yhat_val = model.predict(X_val)

    ret_stats = {
        "mu": float(np.mean(y_train)),     # media da sottrarre in forecast (de-bias)
        "sigma": float(np.std(y_train)),
        "p99": float(np.quantile(np.abs(y_train), 0.99)),
        "p995": float(np.quantile(np.abs(y_train), 0.995)),
    }

    metrics = {
        "val_mae_ret": mae(y_val, yhat_val),
        "val_rmse_ret": rmse(y_val, yhat_val),
        "val_smape_ret%": smape(y_val, yhat_val),
        "n_train": int(len(X_train)),
        "n_val": int(len(X_val)),
        "model": Model,
        "Xcols": Xcols,
        "ret_stats": ret_stats,            # <-- usato dopo
    }
    return model, Xcols, metrics

def _prepare_X_for_predict(model, cur_df: pd.DataFrame, Xcols: List[str]) -> pd.DataFrame:
    Xcols = list(dict.fromkeys(Xcols))
    X_df = cur_df.reindex(columns=Xcols)
    X_df = X_df.ffill(axis=0).bfill(axis=0).fillna(0.0)

    n_expected = getattr(model, "n_features_in_", None)
    if n_expected is not None and X_df.shape[1] != n_expected:
        used_names = None
        try:
            if hasattr(model, "booster_") and model.booster_ is not None:
                used_names = list(model.booster_.feature_name())
        except Exception:
            used_names = None

        if used_names:
            keep = [c for c in Xcols if c in used_names]
            if len(keep) == n_expected:
                X_df = X_df[keep]
            else:
                X_df = X_df.iloc[:, :n_expected]
        else:
            X_df = X_df.iloc[:, :n_expected]

    try:
        X_df = X_df.astype(float)
    except Exception:
        pass
    return X_df

def build_features_from_state(state: pd.DataFrame) -> pd.DataFrame:
    base_cols = ["ts","open","high","low","close","volume","exo_med_ret"]
    st = state[base_cols].copy()
    df_feat = add_lags_rolls(st, col="close")
    for L in (1,2,3,6,12,24,36,72,144):
        df_feat[f"exo_lag{L}"] = df_feat["exo_med_ret"].shift(L)
    tf = time_features(df_feat["ts"])
    df_feat = pd.concat([df_feat.reset_index(drop=True), tf.reset_index(drop=True)], axis=1)
    return df_feat

def _minute_of_day(ts: int) -> int:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.hour*60 + dt.minute

def recursive_forecast(df_full: pd.DataFrame, model, Xcols: List[str],
                       exo_seasonal: pd.Series, resid_std_ret: float,
                       horizon_steps: int = 288,
                       bias_mu: float = 0.0) -> pd.DataFrame:
    base_cols = ["ts","open","high","low","close","volume","exo_med_ret"]
    missing = [c for c in base_cols if c not in df_full.columns]
    if missing:
        raise ValueError(f"recursive_forecast: missing base columns {missing}")

    state = df_full[base_cols].copy().reset_index(drop=True)
    last_ts = int(state.loc[state.index[-1], "ts"])
    last_close = float(state.loc[state.index[-1], "close"])
    step = 300  # 5m

    # bound “iniziale” stretto, poi cresce fino a CLIP_RET
    base_bound = max(2.5*resid_std_ret, 0.01)
    base_bound = min(base_bound, CLIP_RET)

    rows = []
    first_close = last_close
    cum_log_ret = 0.0  # drift cumulato in log-space

    for i in range(horizon_steps):
        next_ts = last_ts + step
        mof = _minute_of_day(next_ts)
        exo = float(exo_seasonal.iloc[mof]) if 0 <= mof < len(exo_seasonal) else float(exo_seasonal.median())

        new_row = {"ts": next_ts, "open": np.nan, "high": np.nan, "low": np.nan,
                "close": last_close, "volume": 0.0, "exo_med_ret": exo}
        state = pd.concat([state, pd.DataFrame([new_row])], ignore_index=True)

        feat = build_features_from_state(state)
        cur = feat.iloc[-1:].copy()
        X_df = _prepare_X_for_predict(model, cur, Xcols)

        yhat_ret = float(model.predict(X_df)[0])
        if not np.isfinite(yhat_ret):
            yhat_ret = 0.0

        # --- de-bias verso zero (toglie il drift medio di train) ---
        yhat_ret -= float(bias_mu)

        # --- damping verso lo zero con l'orizzonte ---
        # riduce gradualmente l'impatto della ricorsione, evitando runaway
        damp = 0.985 ** i
        yhat_ret *= damp

        # --- bound dinamico per step ---
        dyn = base_bound + (CLIP_RET - base_bound) * min(i/50.0, 1.0)
        yhat_ret = float(np.clip(yhat_ret, -dyn, dyn))

        # --- cap sulla deriva cumulata nelle 24h ---
        proposed_cum = cum_log_ret + yhat_ret
        capped_cum = float(np.clip(proposed_cum, -DRIFT_CAP_24H, DRIFT_CAP_24H))
        # se serve, ridimensiona lo step corrente per rispettare il cap cumulato
        yhat_ret = capped_cum - cum_log_ret
        cum_log_ret = capped_cum

        # ricostruzione prezzo
        next_close = float(last_close * math.exp(yhat_ret))

        lower = float(last_close * math.exp(yhat_ret - 1.96*resid_std_ret))
        upper = float(last_close * math.exp(yhat_ret + 1.96*resid_std_ret))

        state.loc[state.index[-1], "close"] = next_close
        last_close, last_ts = next_close, next_ts

        rows.append({
            "ts": next_ts,
            "time_iso": to_iso(next_ts),
            "yhat": next_close,
            "yhat_lower": lower,
            "yhat_upper": upper
        })

    return pd.DataFrame(rows)

# ======== Helpers Matplotlib time ========
def _mpl_num_from_epoch_seconds(ts_array):
    ts_array = np.asarray(ts_array, dtype=float)
    dt = pd.to_datetime(ts_array, unit="s", utc=True).tz_convert(None).to_pydatetime()
    return mdates.date2num(dt)

# =================== Plotting ===================
def _candlestick_ax(ax, ts, o, h, l, c, width_sec=120, alpha=0.9):
    ts = np.asarray(ts, dtype=float)
    o = np.asarray(o, dtype=float)
    h = np.asarray(h, dtype=float)
    l = np.asarray(l, dtype=float)
    c = np.asarray(c, dtype=float)

    x = _mpl_num_from_epoch_seconds(ts)
    width_days = float(width_sec) / 86400.0

    for xi, oi, hi, li, ci in zip(x, o, h, l, c):
        ax.plot([xi, xi], [li, hi], linewidth=0.8)
        y0 = min(oi, ci); y1 = max(oi, ci)
        height = (y1 - y0) if (y1 > y0) else 1e-12
        ax.add_patch(
            plt.Rectangle((xi - width_days/2, y0), width_days, height,
                          fill=True, alpha=alpha, linewidth=0.5)
        )

def save_candlestick_with_forecast(df_tgt: pd.DataFrame, fc: pd.DataFrame, out_png: str):
    """
    Candlestick ultimi 3 giorni reali + overlay della previsione (linea con bande).
    Robusta ai tipi tempo e ai NaN/inf.
    """
    if df_tgt is None or df_tgt.empty or fc is None or fc.empty:
        return

    # Ordina e filtra timestamp plausibili (2000–2100 in epoch seconds)
    fc = fc.copy().sort_values("ts")
    fc = fc[(np.isfinite(fc["ts"])) &
            (fc["ts"] >= 946684800) &   # 2000-01-01
            (fc["ts"] <= 4102444800)]   # 2100-01-01
    if fc.empty:
        return

    start_fc = int(fc["ts"].min())
    win_start = start_fc - 3*24*60*60

    hist = (df_tgt[(df_tgt["ts"] >= win_start) & (df_tgt["ts"] <= start_fc)]
            .copy().sort_values("ts"))
    if hist.empty:
        tmax = int(df_tgt["ts"].max())
        hist = df_tgt[df_tgt["ts"] >= (tmax - 3*24*60*60)].copy().sort_values("ts")
        if hist.empty:
            return

    fig, ax = plt.subplots(figsize=(12, 6))

    # --- Candlestick storico ---
    _candlestick_ax(
        ax,
        hist["ts"].to_numpy(dtype=float),
        hist["open"].to_numpy(dtype=float),
        hist["high"].to_numpy(dtype=float),
        hist["low"].to_numpy(dtype=float),
        hist["close"].to_numpy(dtype=float),
        width_sec=90
    )

    # --- Overlay forecast (linea + bande) ---
    t_fc_num = _mpl_num_from_epoch_seconds(fc["ts"].to_numpy(dtype=float))
    yhat = fc["yhat"].to_numpy(dtype=float)

    mask = np.isfinite(t_fc_num) & np.isfinite(yhat)
    t_fc_num = t_fc_num[mask]
    yhat     = yhat[mask]

    ax.plot(t_fc_num, yhat, linewidth=1.4, label="Forecast (close)")

    # adatta y-limits su storico+forecast (con un 10% di margine)
    y_all = np.concatenate([hist["close"].to_numpy(dtype=float), yhat])
    ymin, ymax = np.nanmin(y_all), np.nanmax(y_all)
    margin = (ymax - ymin) * 0.10 if np.isfinite(ymax - ymin) else 0.0
    if np.isfinite(ymin) and np.isfinite(ymax) and ymax > ymin:
        ax.set_ylim(ymin - margin, ymax + margin)

    if {"yhat_lower", "yhat_upper"}.issubset(fc.columns):
        ylow  = fc["yhat_lower"].to_numpy(dtype=float)[mask]
        yhigh = fc["yhat_upper"].to_numpy(dtype=float)[mask]
        ax.fill_between(t_fc_num, ylow, yhigh, alpha=0.25, label="Conf.")

    ax.set_title("Ultimi 3 giorni (5m) + Forecast prossime 24h")
    ax.set_xlabel("Tempo (UTC)")
    ax.set_ylabel("Prezzo")
    ax.legend()

    ax.xaxis_date()
    locator = mdates.AutoDateLocator()
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
    fig.autofmt_xdate()

    plt.tight_layout()
    os.makedirs(os.path.dirname(out_png), exist_ok=True)
    fig.savefig(out_png)
    plt.close(fig)

# =================== Orchestrator completo ===================
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)
    return path

def robust_select_similar(
    chain: str,
    target_pair: str,
    count_sim: int,
    relax: float,
    dex_ids: Optional[List[str]],
    allow_any_dex: bool,
    no_quote_filter: bool,
    explain: bool,
    diag: bool
) -> pd.DataFrame:
    tries = [
        (relax, allow_any_dex, no_quote_filter),
        (max(relax*1.5, 1.0), allow_any_dex, no_quote_filter),
        (max(relax*2.5, 1.0), allow_any_dex, True),
        (max(relax*3.5, 1.0), True, True),
    ]
    last_err = None
    for rlx, anydex, noq in tries:
        try:
            return select_similar(
                chain=chain,
                target_pair_addr=target_pair,
                count=count_sim,
                relax=rlx,
                dex_ids=dex_ids,
                allow_any_dex=anydex,
                quotes_allow=None,
                no_quote_filter=noq,
                anchors=None,
                source_mode="both",
                dedup_base=True,
                explain=explain,
                diag=diag
            )
        except Exception as e:
            last_err = e
            print(f"[info] select_similar fallita con relax={rlx}, allow_any_dex={anydex}, no_quote_filter={noq}: {e}")
            continue
    print("[warn] impossibile trovare simili coerenti. Procedo senza exogenous (fallback).")
    if last_err: print(f"[last_error] {last_err}")
    return pd.DataFrame(columns=["pairAddress"])

def _val_price_metrics(df_sup_tail: pd.DataFrame, yhat_ret: np.ndarray) -> Dict[str, float]:
    close_t = df_sup_tail["close"].to_numpy(dtype=float)
    true_next = close_t * np.exp(df_sup_tail["y"].to_numpy(dtype=float))
    pred_next = close_t * np.exp(yhat_ret.astype(float))
    return {
        "val_mae_price": mae(true_next, pred_next),
        "val_rmse_price": rmse(true_next, pred_next),
        "val_mape_price%": mape(true_next, pred_next),
    }

def run_pipeline(
    target_pair: str,
    chain: str,
    days: int,
    count_sim: int,
    relax: float,
    no_quote_filter: bool,
    dex_ids: Optional[List[str]],
    allow_any_dex: bool,
    outdir: str,
    top_k_exo: int = 10,
    horizon_minutes: int = 24*60,
    step_minutes: int = 5,
    save_ohlcv: bool = False,
    db_path: str = "data/predictor.db",
    explain: bool = False,
    diag: bool = False
):
    _ = ds_get_pair(chain, target_pair)

    # 1) selezione simili
    print("[1/6] Selezione pool simili…")
    df_sim = robust_select_similar(
        chain, target_pair, count_sim, relax,
        dex_ids, allow_any_dex, no_quote_filter, explain, diag
    )
    os.makedirs(os.path.join("data"), exist_ok=True)
    df_sim.to_csv(os.path.join("data","pools_list.csv"), index=False)
    print(f"    → pool simili: {len(df_sim)} (salvati in data/pools_list.csv)")

    # 2) OHLCV + exogenous + profilo stagionale
    print("[2/6] Scarico OHLCV 5m (target + exogenous)…")
    df_tgt, df_exo, exo_seasonal = fetch_target_and_exo(target_pair, chain, days, df_sim, top_k_exo=top_k_exo)

    if save_ohlcv:
        subdir = os.path.join("data", "gold", chain, f"POOL_{target_pair[:6]}")
        ensure_dir(subdir)
        saved = save_parquet_or_csv(df_tgt, os.path.join(subdir, "candles_5m.parquet"))
        append_sqlite(df_tgt, chain, target_pair, db_path=db_path)
        print(f"    → target rows={len(df_tgt)} saved={saved}")

    # 3) supervised dataset
    print("[3/6] Creo dataset supervisionato (returns)…")
    df_sup = make_supervised(df_tgt, df_exo)
    if len(df_sup) < 600:
        print(f"[warn] storico limitato ({len(df_sup)} righe). Le metriche potrebbero essere instabili.")

    # 4) training
    print("[4/6] Addestro il modello…")
    model, Xcols, metrics = fit_model(df_sup)

    # metriche in spazio prezzo
    val_len = metrics["n_val"]
    X_val = df_sup.iloc[-val_len:][Xcols]
    yhat_val_ret = model.predict(X_val)
    price_metrics = _val_price_metrics(df_sup.iloc[-val_len:], yhat_val_ret)

    print(f"    → Model={metrics['model']} | Ntrain={metrics['n_train']} Nval={metrics['n_val']}")
    print(f"      Returns: MAE={metrics['val_mae_ret']:.6e}  RMSE={metrics['val_rmse_ret']:.6e}  sMAPE={metrics['val_smape_ret%']:.4f}%")
    print(f"      Price:   MAE={price_metrics['val_mae_price']:.6f}  RMSE={price_metrics['val_rmse_price']:.6f}  MAPE={price_metrics['val_mape_price%']:.4f}%")

    # 5) forecast 24h/5m
    print("[5/6] Genero forecast…")
    steps = int(horizon_minutes // step_minutes)
    resid_std_ret = float(np.std(df_sup.iloc[-val_len:]["y"].to_numpy() - yhat_val_ret)) if val_len else 0.0
    base_cols = ["ts","open","high","low","close","volume","exo_med_ret"]
    base_state = df_tgt.merge(df_exo, on="ts", how="left").fillna({"exo_med_ret":0.0})
    base_state = base_state[base_cols].copy()
    fc = recursive_forecast(
        base_state, model, Xcols, exo_seasonal, resid_std_ret,
        horizon_steps=steps,
        bias_mu=metrics.get("ret_stats", {}).get("mu", 0.0)
    )

    # 6) salvataggio + chart
    subdir = ensure_dir(os.path.join(outdir, chain, f"POOL_{target_pair[:6]}"))
    ts_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    out_csv = os.path.join(subdir, f"forecast_5m_{ts_str}.csv")
    meta_txt = os.path.join(subdir, "model_card.txt")
    chart_png = os.path.join(subdir, "chart_candles_forecast.png")

    fc.to_csv(out_csv, index=False)
    with open(meta_txt, "w", encoding="utf-8") as f:
        f.write(f"pair: {target_pair}\n")
        f.write(f"chain: {chain}\n")
        f.write(f"model: {metrics['model']}\n")
        f.write(f"n_train: {metrics['n_train']}\n")
        f.write(f"n_val: {metrics['n_val']}\n")
        f.write(f"val_mae_ret: {metrics['val_mae_ret']:.8e}\n")
        f.write(f"val_rmse_ret: {metrics['val_rmse_ret']:.8e}\n")
        f.write(f"val_smape_ret%: {metrics['val_smape_ret%']:.6f}\n")
        f.write(f"val_mae_price: {price_metrics['val_mae_price']:.8f}\n")
        f.write(f"val_rmse_price: {price_metrics['val_rmse_price']:.8f}\n")
        f.write(f"val_mape_price%: {price_metrics['val_mape_price%']:.6f}\n")
        f.write(f"relax: {relax}\n")
        f.write(f"no_quote_filter: {no_quote_filter}\n")
        f.write(f"count_sim: {count_sim}\n")
        f.write(f"top_k_exo: {top_k_exo}\n")
        if not df_sim.empty:
            f.write(f"sim_pools_used: {','.join(df_sim['pairAddress'].head(10))}\n")
        f.write(f"generated_utc: {datetime.now(timezone.utc).isoformat().replace('+00:00','Z')}\n")

    # chart candlestick storico + overlay forecast
    save_candlestick_with_forecast(df_tgt, fc, chart_png)

    print("\n=== MODEL VALIDATION ===")
    print(f"Model: {metrics['model']} | Ntrain={metrics['n_train']} Nval={metrics['n_val']}")
    print(f"Returns: MAE={metrics['val_mae_ret']:.6e}  RMSE={metrics['val_rmse_ret']:.6e}  sMAPE={metrics['val_smape_ret%']:.4f}%")
    print(f"Price:   MAE={price_metrics['val_mae_price']:.6f}  RMSE={price_metrics['val_rmse_price']:.6f}  MAPE={price_metrics['val_mape_price%']:.4f}%")

    print("\n=== FORECAST (head) ===")
    print(fc.head(10).to_string(index=False))
    print(f"\nSaved forecast to: {out_csv}")
    print(f"Saved model card to: {meta_txt}")
    print(f"Saved candlestick chart to: {chart_png}")

    return out_csv

# =================== CLI ===================
def parse_args():
    ap = argparse.ArgumentParser(
        description="Predice i prezzi a 5m per 24h di un pair DexScreener su BSC (default), con discovery di pool simili e grafico candlestick."
    )
    ap.add_argument("target_pair", help="pairAddress DexScreener (es: 0xf0a9...)")
    ap.add_argument("--chain", default=NETWORK, help="Rete (default: bsc)")
    ap.add_argument("--days", type=int, default=60, help="Storico da usare (giorni)")
    ap.add_argument("--count_sim", type=int, default=60, help="Quanti pool simili considerare")
    ap.add_argument("--relax", type=float, default=2.0, help="Ampiezza filtri per simili (>=1.0)")
    ap.add_argument("--no_quote_filter", action="store_true", help="Disattiva filtro sul quote")
    ap.add_argument("--dex_ids", default=",".join(DEFAULT_PCS_DEX_IDS), help="dexIds separati da virgola")
    ap.add_argument("--allow_any_dex", action="store_true", help="Accetta qualsiasi dexId")
    ap.add_argument("--top_k_exo", type=int, default=10, help="Quanti simili usare per la mediana exogenous")
    ap.add_argument("--outdir", default="data/predictions", help="Cartella output")
    ap.add_argument("--horizon_minutes", type=int, default=24*60, help="Orizzonte in minuti (default 1440)")
    ap.add_argument("--step_minutes", type=int, default=5, help="Passo in minuti (default 5)")
    ap.add_argument("--save_ohlcv", action="store_true", help="Salva OHLCV del target (utile per grafico)")
    ap.add_argument("--db", default="data/predictor.db", help="Path DB SQLite per OHLCV (se --save_ohlcv)")
    ap.add_argument("--explain", action="store_true", help="Stampa motivi di esclusione conteggiati")
    ap.add_argument("--diag", action="store_true", help="Stampa diagnostica discovery")
    return ap.parse_args()

def main():
    args = parse_args()
    dex_ids = [d.strip().lower() for d in (args.dex_ids or "").split(",") if d.strip()] or None
    try:
        run_pipeline(
            target_pair=(args.target_pair or "").lower(),
            chain=args.chain,
            days=args.days,
            count_sim=args.count_sim,
            relax=max(args.relax, 1.0),
            no_quote_filter=bool(args.no_quote_filter),
            dex_ids=dex_ids,
            allow_any_dex=bool(args.allow_any_dex),
            outdir=args.outdir,
            top_k_exo=args.top_k_exo,
            horizon_minutes=args.horizon_minutes,
            step_minutes=args.step_minutes,
            save_ohlcv=bool(args.save_ohlcv),
            db_path=args.db,
            explain=args.explain,
            diag=args.diag
        )
    except Exception as e:
        print(f"Errore: {e}")
        if os.environ.get("DEBUG_TRACE","0") == "1":
            traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
