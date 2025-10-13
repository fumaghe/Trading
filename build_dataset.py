# build_dataset.py
# Dataset builder "from scratch" per selezionare molte pool simili al target
# su BSC prendendo i dati da DexScreener e scaricando OHLCV 5m da GeckoTerminal.
# - Discovery ampio: quote anchors + dexIds + coppie del token del target
# - Filtri adattivi centrati sul target + relax
# - Cache, rate limit, retry
# - Export parquet/csv + SQLite append
# - CLI semplice e diagnostica ricca

import os, time, json, math, argparse, hashlib, sqlite3, traceback
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
import pandas as pd



"""
python build_dataset.py --target_pair 0xf0a949d3d93b833c183a27ee067165b6f2c9625e --count 60 --days 60 --relax 2.0 --dex_ids pancakeswap,pancakeswapv2,pancakeswap-v2,pancakeswapv3,pancakeswap-v3 --quotes_allow 0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d,0x55d398326f99059ff775485246999027b3197955,0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c --allow_any_dex --explain --diag

"""
# -------------------- Costanti --------------------
DEX_API = "https://api.dexscreener.com"
GT_API  = "https://api.geckoterminal.com/api/v2"

DEFAULT_HEADERS = {
    "accept": "application/json",
    "user-agent": "dex-forecaster/3.0 (+cli)",
    "accept-language": "en-US,en;q=0.9",
}

NETWORK = "bsc"  # default
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

# -------------------- Utils HTTP (cache + RL + retry) --------------------
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

# -------------------- Helper numerici --------------------
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
    # ms or s
    if v < 10**12: v *= 1000
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
    # fallback keys
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

# -------------------- DexScreener --------------------
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

# -------------------- Similarità & filtri --------------------
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

    # fasce larghe + relax
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

# -------------------- Discovery + selezione --------------------
def discover_candidates(chain: str, target: Dict[str, Any],
                        dex_ids: Optional[List[str]],
                        anchors: Optional[List[str]], source_mode: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    # 1) coppie del base e del quote del target (cattura “vicinato”)
    base = (target.get("baseToken") or {}).get("address") or ""
    quote = (target.get("quoteToken") or {}).get("address") or ""
    for tok in [base, quote]:
        if tok:
            try:
                out.extend(ds_token_pairs(chain, tok))
            except Exception:
                pass

    # 2) anchor quote tokens (USDC/USDT/WBNB/DAI, etc.)
    anchors_use = [a.lower() for a in (anchors or DEFAULT_ANCHORS)]
    for a in anchors_use:
        try:
            out.extend(ds_token_pairs(chain, a))
        except Exception:
            pass

    # 3) dexIds (PCS v2+v3 by default) o all if allow_any_dex è usato a valle
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
        # di default usa stesso quote del target
        quote_mode, quote_single, quote_allow = "single", (_quote_addr_of(tgt) or "").lower(), set()

    # dex filter
    allowed_dex = None if allow_any_dex else set([d.lower() for d in (dex_ids or list(DEFAULT_PCS_DEX_IDS))])

    # discovery ampio
    cands_raw = discover_candidates(chain, tgt, dex_ids, anchors, source_mode)

    # dedup per pairAddress + hydrate
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

# -------------------- GeckoTerminal OHLCV + persistenza --------------------
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
        # fallback csv.gz
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
    # pulizia
    df = df[(df["close"] > 0) & (df["high"] >= df["low"])]
    return df

def append_sqlite(df: pd.DataFrame, chain: str, pool: str, db_path: str):
    if df is None or df.empty: return
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = sqlite3.connect(db_path)
    try:
        df2 = df.copy()
        df2["chain"] = chain
        df2["pool"]  = pool
        df2 = df2[["chain","pool","ts","open","high","low","close","volume"]]
        df2.to_sql("candles", con, if_exists="append", index=False)
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_candles ON candles(chain,pool,ts)")
        con.commit()
    finally:
        con.close()

# -------------------- Orchestrator --------------------
def build_dataset(
    target_pair: str,
    count: int,
    days: int,
    outdir: str,
    db: str,
    cache_dir: str,
    chain: str,
    save_list_path: str,
    relax: float,
    source: str,
    dex_ids: Optional[List[str]],
    allow_any_dex: bool,
    quotes_allow: Optional[List[str]],
    no_quote_filter: bool,
    anchors: Optional[List[str]],
    dedup_base: bool,
    explain: bool,
    diag: bool
) -> Tuple[pd.DataFrame, List[str]]:

    print("[1/4] Selezione pool simili (filtri adattivi + relax)...")
    df_pools = select_similar(
        chain=chain,
        target_pair_addr=target_pair,
        count=count,
        relax=relax,
        dex_ids=dex_ids,
        allow_any_dex=allow_any_dex,
        quotes_allow=quotes_allow,
        no_quote_filter=no_quote_filter,
        anchors=anchors,
        source_mode=source,
        dedup_base=dedup_base,
        explain=explain,
        diag=diag
    )

    os.makedirs(os.path.dirname(save_list_path), exist_ok=True)
    df_pools.to_csv(save_list_path, index=False)
    print(f"Trovati {len(df_pools)} pool (saved: {save_list_path})")

    print("[2/4] Ingest OHLCV 5m da GeckoTerminal...")
    saved_files: List[str] = []
    n = len(df_pools)
    for i, row in enumerate(df_pools.itertuples(index=False), start=1):
        pair = row.pairAddress
        subdir = os.path.join(outdir, chain, f"POOL_{pair[:6]}")
        out_parquet = os.path.join(subdir, "candles_5m.parquet")

        # skip se già salvato
        if any(os.path.exists(x) for x in [
            out_parquet,
            out_parquet.replace(".parquet",".csv.gz"),
            out_parquet.replace(".parquet",".csv")
        ]):
            print(f"  [{i}/{n}] {pair}  (skip: già presente)")
            continue

        try:
            print(f"  [{i}/{n}] {pair}  ... fetching")
            df = gt_fetch_ohlcv_5m(pair, network=chain, days=days, include_empty=True, cache_dir=cache_dir)
            saved = save_parquet_or_csv(df, out_parquet)
            append_sqlite(df, chain, pair, db_path=db)
            print(f"       -> rows={len(df)} saved={saved}")
            if saved: saved_files.append(saved)
        except Exception as e:
            print(f"       !! errore su {pair}: {e}")
            if os.environ.get("DEBUG_TRACE","0") == "1":
                traceback.print_exc()

    print("[3/4] Completato ingest per i pool selezionati.")
    print("[4/4] Done.")
    return df_pools, saved_files

# -------------------- CLI --------------------
def parse_args():
    ap = argparse.ArgumentParser(description="Costruisce un dataset di pool simili e OHLCV 5m da GeckoTerminal.")
    ap.add_argument("--target_pair", required=True, help="pairAddress DexScreener (es: 0xf0a9...)")
    ap.add_argument("--count", type=int, default=60, help="Quante pool tenere (top-N per similarità)")
    ap.add_argument("--days", type=int, default=60, help="Giorni storici per OHLCV")
    ap.add_argument("--outdir", default="data/gold")
    ap.add_argument("--db", default="data/predictor.db")
    ap.add_argument("--cache_dir", default="data/cache")
    ap.add_argument("--save_list_path", default=os.path.join("data","pools_list.csv"))
    ap.add_argument("--chain", default=NETWORK, help="Rete (default: bsc)")
    ap.add_argument("--relax", type=float, default=2.0, help="Allarga/Restringe i filtri adattivi (>=1.0)")
    ap.add_argument("--source", choices=["both","wbnb-only","dex-only"], default="both",
                    help="Sorgente discovery: only WBNB, solo dexIds, oppure entrambi")
    ap.add_argument("--dex_ids", default=",".join(DEFAULT_PCS_DEX_IDS),
                    help="Lista dexIds separati da virgola (es: pancakeswap,pancakeswapv3)")
    ap.add_argument("--allow_any_dex", action="store_true", help="Nessun filtro sul dexId")
    ap.add_argument("--quotes_allow", default="", help="Allowlist di quote token (addr separati da virgola)")
    ap.add_argument("--no_quote_filter", action="store_true", help="Disattiva completamente il filtro quote")
    ap.add_argument("--anchors", default="", help="Anchor tokens per discovery (addr separati da virgola)")
    ap.add_argument("--no_dedup_base", action="store_true", help="Non deduplicare per base token")
    ap.add_argument("--explain", action="store_true", help="Stampa i motivi di esclusione conteggiati")
    ap.add_argument("--diag", action="store_true", help="Stampa top dex e top quote durante discovery")
    return ap.parse_args()

def main():
    args = parse_args()
    dex_ids = [d.strip().lower() for d in args.dex_ids.split(",") if d.strip()] or None
    quotes_allow = [a.strip().lower() for a in args.quotes_allow.split(",") if a.strip()] or None
    anchors = [a.strip().lower() for a in (args.anchors.split(",") if args.anchors else []) if a.strip()] or None

    try:
        pools_df, saved = build_dataset(
            target_pair=(args.target_pair or "").lower(),
            count=args.count,
            days=args.days,
            outdir=args.outdir,
            db=args.db,
            cache_dir=args.cache_dir,
            chain=args.chain,
            save_list_path=args.save_list_path,
            relax=max(args.relax, 1.0),
            source=args.source,
            dex_ids=dex_ids,
            allow_any_dex=bool(args.allow_any_dex),
            quotes_allow=quotes_allow,
            no_quote_filter=bool(args.no_quote_filter),
            anchors=anchors,
            dedup_base=not args.no_dedup_base,
            explain=args.explain,
            diag=args.diag
        )
        print("\n=== POOLS SELEZIONATE (top 10) ===")
        try:
            print(pools_df.head(10).to_string(index=False))
        except Exception:
            print(pools_df.head(10))
    except Exception as e:
        print(f"Errore: {e}")
        if os.environ.get("DEBUG_TRACE","0") == "1":
            traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
