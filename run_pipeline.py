#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import re
import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path

import requests

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
)
HTTP_TIMEOUT = 30
HEADERS = {"Accept": "application/json", "User-Agent": UA}

DEX_PAIRS_URL = "https://api.dexscreener.com/latest/dex/pairs/bsc/{pair}"
CG_LIST_URL = "https://api.coingecko.com/api/v3/coins/list?include_platform=true"
CG_COIN_URL = "https://api.coingecko.com/api/v3/coins/{id}?localization=false&tickers=false&market_data=true&community_data=false&developer_data=false&sparkline=false"

TOP_HEADER = "— Tutti e 3 i parametri (TOP) —"
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

# ---------------- utils logging ---------------- #

def log(msg: str):
    print(msg, flush=True)

def log_step(i: int, n: int, symbol: str, pair: str, msg: str):
    print(f"[{i}/{n}] {symbol} ({pair}) | {msg}", flush=True)

def log_err(i: int, n: int, symbol: str, pair: str, msg: str):
    print(f"[{i}/{n}] {symbol} ({pair}) | ERROR: {msg}", file=sys.stderr, flush=True)

# ---------------- screener ---------------- #

def run_test4_and_capture(
    python_bin: str,
    min_liq: int,
    workers: int,
    dominance: float,
    max_tickers_scan: int,
    rps_cg: float,
    rps_ds: float,
    funnel_show: int,
    skip_unchanged_days: int,
):
    cmd = [
        python_bin,
        "test4.py",
        "--seed-from", "perps",
        "--workers", str(workers),
        "--min-liq", str(min_liq),
        "--max-tickers-scan", str(max_tickers_scan),
        "--dominance", str(dominance),
        "--skip-unchanged-days", str(skip_unchanged_days),
        "--rps-cg", str(rps_cg),
        "--rps-ds", str(rps_ds),
        "--funnel-show", str(funnel_show),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return ANSI_RE.sub("", res.stdout or "")

def parse_top_only(stdout: str):
    entries = []
    in_top = False
    for raw in (stdout or "").splitlines():
        line = (raw or "").strip()
        if not line:
            continue
        if line.startswith(TOP_HEADER):
            in_top = True
            continue
        if not in_top:
            continue

        m = re.match(
            r"""^
            ([A-Z0-9]+)\s*[·•\-]\s*
            (.*?)\s*[·•\-]\s*
            LP:\s*([\d\.\$]+)\s*[·•\-]\s*
            Pool:\s*(0x[a-fA-F0-9]{40})\s*$
            """,
            line,
            flags=re.X,
        )
        if m:
            symbol = m.group(1).upper()
            name = m.group(2).strip()
            lp_txt = m.group(3)
            pair = m.group(4).lower()
            lp_usd = int(re.sub(r"[^\d]", "", lp_txt)) if lp_txt else 0
            entries.append({"symbol": symbol, "name": name, "pair": pair, "lp_usd": lp_usd})
    return entries

# ---------------- marketcap helpers ---------------- #

def fetch_ds_pair_info(pair: str):
    try:
        url = DEX_PAIRS_URL.format(pair=pair)
        r = requests.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        j = r.json()
        p = (j.get("pairs") or [None])[0] or {}
        mc = p.get("marketCap") or p.get("fdv")
        dexId = p.get("dexId")
        return {"marketcap": float(mc) if mc is not None else None, "dexId": dexId}
    except Exception:
        return {"marketcap": None, "dexId": None}

def load_cg_list_cached(cache_path: Path):
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if cache_path.exists() and (time.time() - cache_path.stat().st_mtime) < 48 * 3600:
        try:
            import json
            return json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    r = requests.get(CG_LIST_URL, headers=HEADERS, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    try:
        import json
        cache_path.write_text(json.dumps(data), encoding="utf-8")
    except Exception:
        pass
    return data

def choose_best_cg_id(symbol: str, name: str, cg_list: list):
    sym_up = symbol.upper()
    cands = []
    for row in cg_list:
        row_sym = (row.get("symbol") or "").upper()
        if row_sym != sym_up:
            continue
        platforms = row.get("platforms") or {}
        has_bsc = any(k.lower() in ("binance-smart-chain", "bsc", "bnb-smart-chain", "bnb") for k in platforms.keys())
        cands.append((row.get("id"), (row.get("name") or "").strip(), has_bsc))
    if not cands:
        return None
    pool = [c for c in cands if c[2]] or cands
    name_low = (name or "").lower().strip()
    exact = [c for c in pool if c[1].lower() == name_low]
    if exact:
        return exact[0][0]
    starts = [c for c in pool if c[1].lower().startswith(name_low[:6])]
    if starts:
        return starts[0][0]
    return pool[0][0]

def fetch_cg_marketcap_usd(coin_id: str):
    try:
        url = CG_COIN_URL.format(id=coin_id)
        r = requests.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        j = r.json()
        md = (j.get("market_data") or {}).get("market_cap") or {}
        mc = md.get("usd")
        return float(mc) if mc is not None else None
    except Exception:
        return None

def format_usd(x):
    try:
        return f"{int(round(float(x))):,}$".replace(",", ".")
    except Exception:
        return "n.d."

# ---------------- defi compute ---------------- #

def compute_push_amounts_with_defi(pool: str, rpc_url: str):
    import importlib.util

    mod_path = Path(__file__).with_name("defiFunzionante.py")
    if not mod_path.exists():
        raise RuntimeError("defiFunzionante.py non trovato nella stessa cartella.")

    spec = importlib.util.spec_from_file_location("defiFunzionante", str(mod_path))
    defi = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(defi)  # type: ignore

    w3 = defi.Web3(defi.Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 30}))
    defi._inject_poa(w3)
    if not w3.is_connected():
        raise RuntimeError("Connessione RPC fallita. Controlla --rpc.")

    pool_addr = defi.Web3.to_checksum_address(pool)
    pool_contract = w3.eth.contract(address=pool_addr, abi=defi.UNISWAP_V3_POOL_MINI_ABI)
    snapshot = defi.read_pool_snapshot(w3, pool_addr)

    P0, rows = defi.compute_targets(
        snapshot,
        w3,
        pool_contract,
        factors=[Decimal("1.5"), Decimal("2.0"), Decimal("3.0")],  # +50, +100, +200
    )

    def fmt(r):
        return r["token1_needed"]  # es. "123.456789 USDT"

    return {
        "token1_symbol": snapshot.token1.symbol,
        "plus50": fmt(rows[0]),
        "plus100": fmt(rows[1]),
        "plus200": fmt(rows[2]),
    }

def classify_non_v3_error(err_msg: str) -> bool:
    s = (err_msg or "").lower()
    hints = [
        "function selector",
        "execution reverted",
        "abi",
        "missing required key",
        "could not decode",
    ]
    return any(h in s for h in hints)

# ---------------- main ---------------- #

def main():
    ap = argparse.ArgumentParser(description="Pipeline: screener TOP (con pool) -> analisi DeFi (+50, +100, +200)")
    ap.add_argument("--rpc", default="https://bsc-dataseed.binance.org")
    ap.add_argument("--python-bin", default=sys.executable)
    ap.add_argument("--min-liq", type=int, default=200000)
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--dominance", type=float, default=0.30)
    ap.add_argument("--max-tickers-scan", type=int, default=40)
    ap.add_argument("--rps-cg", type=float, default=0.5)
    ap.add_argument("--rps-ds", type=float, default=2.0)
    ap.add_argument("--funnel-show", type=int, default=100)
    ap.add_argument("--skip-unchanged-days", type=int, default=0)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--sleep", type=float, default=0.0)
    ap.add_argument("--no-cg-fallback", action="store_true")
    ap.add_argument("--debug-screener", action="store_true", help="Stampa lo stdout grezzo di test4.py")
    ap.add_argument("--per_pool_debug", action="store_true", help="Stampa l'errore dettagliato per le pool che falliscono")
    args = ap.parse_args()

    start_all = time.perf_counter()

    # 1) Screener
    log("==> Avvio screener (test4.py)...")
    try:
        stdout = run_test4_and_capture(
            python_bin=args.python_bin,
            min_liq=args.min_liq,
            workers=args.workers,
            dominance=args.dominance,
            max_tickers_scan=args.max_tickers_scan,
            rps_cg=args.rps_cg,
            rps_ds=args.rps_ds,
            funnel_show=args.funnel_show,
            skip_unchanged_days=args.skip_unchanged_days,
        )
    except subprocess.CalledProcessError as e:
        log_err(0, 0, "-", "-", "Lancio test4.py fallito")
        print(e.stdout)
        print(e.stderr, file=sys.stderr)
        sys.exit(1)

    if args.debug_screener:
        print(">> ===== Screener stdout (debug) =====")
        print(stdout)
        print(">> ===================================")

    coins = parse_top_only(stdout)
    if not coins:
        log("Nessuna coin in TOP trovata dall'output di test4.py.")
        sys.exit(0)

    n = len(coins)
    log(f"==> Trovate {n} coin in TOP. Inizio analisi per-pool...")

    # Cache CG
    cg_list = None
    cg_cache = Path(".cache") / "cg_coins_list_include_platform.json"

    rows = []

    for idx, c in enumerate(coins, start=1):
        symbol = c["symbol"]
        name = c["name"]
        pair = c["pair"]
        lp_usd = c["lp_usd"]

        t0 = time.perf_counter()
        log_step(idx, n, symbol, pair, f"START | Name='{name}' | LP={format_usd(lp_usd)}")

        # 2) Market cap
        try:
            log_step(idx, n, symbol, pair, "DexScreener: fetch marketcap...")
            ds_t0 = time.perf_counter()
            ds = fetch_ds_pair_info(pair)
            ds_dt = time.perf_counter() - ds_t0
            marketcap_usd = ds.get("marketcap")
            dexId = ds.get("dexId")
            log_step(idx, n, symbol, pair, f"DexScreener OK in {ds_dt:.2f}s | dexId={dexId} | mcap={format_usd(marketcap_usd) if marketcap_usd else 'n.d.'}")
        except Exception as e:
            marketcap_usd = None
            log_err(idx, n, symbol, pair, f"DexScreener KO: {e}")

        # 2b) CoinGecko fallback
        if (marketcap_usd is None) and (not args.no_cg_fallback):
            try:
                if cg_list is None:
                    log_step(idx, n, symbol, pair, "CoinGecko: download/uso cache coin list...")
                    cg_list = load_cg_list_cached(cg_cache)
                coin_id = choose_best_cg_id(symbol, name, cg_list or [])
                if coin_id:
                    cg_t0 = time.perf_counter()
                    marketcap_usd = fetch_cg_marketcap_usd(coin_id)
                    cg_dt = time.perf_counter() - cg_t0
                    log_step(idx, n, symbol, pair, f"CoinGecko OK in {cg_dt:.2f}s | id={coin_id} | mcap={format_usd(marketcap_usd) if marketcap_usd else 'n.d.'}")
                else:
                    log_step(idx, n, symbol, pair, "CoinGecko: nessun id coerente trovato.")
            except Exception as e:
                log_err(idx, n, symbol, pair, f"CoinGecko KO: {e}")

        # 3) DeFi calc (diretto, niente gate su dexId)
        try:
            log_step(idx, n, symbol, pair, "DeFi: compute +50/+100/+200...")
            d0 = time.perf_counter()
            push = compute_push_amounts_with_defi(pair, args.rpc)
            ddt = time.perf_counter() - d0
            plus50 = push["plus50"]
            plus100 = push["plus100"]
            plus200 = push["plus200"]
            log_step(idx, n, symbol, pair, f"DeFi OK in {ddt:.2f}s | token1={push['token1_symbol']} | +50={plus50} | +100={plus100} | +200={plus200}")
        except Exception as e:
            msg = str(e)
            if args.per_pool_debug:
                log_err(idx, n, symbol, pair, f"DEBUG ERR: {msg}")
            if classify_non_v3_error(msg):
                plus50 = plus100 = plus200 = "n/a (non v3 o pool incompatibile)"
                log_step(idx, n, symbol, pair, "DeFi risultato: n/a (non v3 o pool incompatibile)")
            else:
                plus50 = plus100 = plus200 = f"ERR ({msg})"
                log_step(idx, n, symbol, pair, f"DeFi risultato: {plus50}")

        rows.append(
            {
                "symbol": symbol,
                "name": name,
                "pair": pair,
                "lp_usd": lp_usd,
                "marketcap_usd": marketcap_usd,
                "plus50": plus50,
                "plus100": plus100,
                "plus200": plus200,
            }
        )

        if args.sleep > 0:
            time.sleep(args.sleep)

        dt = time.perf_counter() - t0
        log_step(idx, n, symbol, pair, f"END | elapsed {dt:.2f}s")

    total_dt = time.perf_counter() - start_all

    # 4) Tabella finale
    headers = [
        "SYMBOL",
        "NAME",
        "PAIR",
        "LP (USD)",
        "MARKET CAP (USD)",
        "TOKEN1 per +50%",
        "TOKEN1 per +100%",
        "TOKEN1 per +200%",
    ]
    print("\n================= RISULTATI FINALI =================")
    print(f"(Totale: {len(rows)}) | Tempo totale: {total_dt:.2f}s")
    print(" | ".join(headers))
    print("-" * 120)
    for r in rows:
        lp_fmt = format_usd(r["lp_usd"])
        mc_fmt = format_usd(r["marketcap_usd"]) if r["marketcap_usd"] is not None else "n.d."
        line = [
            r["symbol"],
            r["name"],
            r["pair"],
            lp_fmt,
            mc_fmt,
            r["plus50"],
            r["plus100"],
            r["plus200"],
        ]
        print(" | ".join(line), flush=True)

if __name__ == "__main__":
    main()
