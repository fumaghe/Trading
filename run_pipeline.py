#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import re
import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path
from html import escape as htmlesc
from typing import Optional, Tuple

import requests

"""
Pipeline: screener TOP (DexScreener + CoinGecko) -> analisi DeFi su pool v3 con importi in token1
e conversione degli importi in USD (≈ USDT) usando priceUsd della *stessa* pair DexScreener.
Ordina i risultati per SCORE100 = (market cap USD) / (costo +100% in USD), in ordine decrescente.
Mostra anche SCORE50 e SCORE200.

Fix:
- gestione "no TOP" con exit code = 2 (warning)
- aggiunto flag --require-v3 che viene passato a test4.py (evita pool v2 non compatibili con defiFunzionante)
- aggiunto --ttl-cg-list pass-through verso test4.py (per trovare coin nuove più rapidamente)
- fix: argomento --per-pool-debug (prima era referenziato ma non esisteva)
"""

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
)
HTTP_TIMEOUT = 30
HEADERS = {"Accept": "application/json", "User-Agent": UA}

DEX_PAIRS_URL = "https://api.dexscreener.com/latest/dex/pairs/bsc/{pair}"
DEX_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{address}"
CG_LIST_URL = "https://api.coingecko.com/api/v3/coins/list?include_platform=true"
CG_COIN_URL = "https://api.coingecko.com/api/v3/coins/{id}?localization=false&tickers=false&market_data=true&community_data=false&developer_data=false&sparkline=false"

TOP_HEADER = "— Tutti e 3 i parametri (TOP) —"
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

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
    require_v3: bool = False,
    ttl_perps: Optional[int] = None,
    ttl_cg: Optional[int] = None,
    ttl_cg_list: Optional[int] = None,
    refresh_perps: bool = False,
    refresh_cg: bool = False,
    refresh_all: bool = False,
) -> Tuple[str, str]:
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

    if require_v3:
        cmd += ["--require-v3"]

    if ttl_perps is not None:
        cmd += ["--ttl-perps", str(int(ttl_perps))]
    if ttl_cg is not None:
        cmd += ["--ttl-cg", str(int(ttl_cg))]
    if ttl_cg_list is not None:
        cmd += ["--ttl-cg-list", str(int(ttl_cg_list))]

    if refresh_all:
        cmd += ["--refresh-all"]
    else:
        if refresh_perps:
            cmd += ["--refresh-perps"]
        if refresh_cg:
            cmd += ["--refresh-cg"]

    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    stdout = ANSI_RE.sub("", res.stdout or "")
    stderr = ANSI_RE.sub("", res.stderr or "")
    return stdout, stderr

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

# ---------------- marketcap & pair info helpers ---------------- #

def fetch_ds_pair_info(pair: str):
    try:
        url = DEX_PAIRS_URL.format(pair=pair)
        r = requests.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        j = r.json()
        p = (j.get("pairs") or [None])[0] or {}

        mc = p.get("marketCap") or p.get("fdv")
        base = p.get("baseToken") or {}
        quote = p.get("quoteToken") or {}
        priceUsd = p.get("priceUsd")
        priceNative = p.get("priceNative")
        url_ds = p.get("url")

        return {
            "marketcap": float(mc) if mc is not None else None,
            "dexId": p.get("dexId"),
            "priceUsd": float(priceUsd) if priceUsd is not None else None,
            "priceNative": float(priceNative) if priceNative is not None else None,
            "baseToken": {"address": (base.get("address") or "").lower(), "symbol": base.get("symbol")},
            "quoteToken": {"address": (quote.get("address") or "").lower(), "symbol": quote.get("symbol")},
            "url": url_ds,
        }
    except Exception:
        return {
            "marketcap": None, "dexId": None, "priceUsd": None, "priceNative": None,
            "baseToken": {}, "quoteToken": {}, "url": None
        }

def fetch_ds_token_price_usd_for_pair(token_addr: str, pair_addr: str):
    try:
        url = DEX_TOKEN_URL.format(address=token_addr)
        r = requests.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        j = r.json()
        for p in j.get("pairs", []) or []:
            if (p.get("pairAddress") or "").lower() == pair_addr.lower():
                val = p.get("priceUsd")
                return float(val) if val is not None else None
    except Exception:
        pass
    return None

def fetch_bnb_usd_from_ds(prefer_chain: str = "bsc") -> Optional[float]:
    try:
        WBNB_ADDR = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
        url = DEX_TOKEN_URL.format(address=WBNB_ADDR)
        r = requests.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        j = r.json()
        best = None  # (liq_usd, usd_per_bnb)

        for p in j.get("pairs", []) or []:
            if (p.get("chainId") or "").lower() != prefer_chain:
                continue
            liq = float(((p.get("liquidity") or {}).get("usd") or 0))
            price_usd = p.get("priceUsd")
            base = p.get("baseToken") or {}
            quote = p.get("quoteToken") or {}
            base_addr = (base.get("address") or "").lower()

            if base_addr == WBNB_ADDR and price_usd is not None:
                usd_per_bnb = float(price_usd)
                if usd_per_bnb > 0:
                    if best is None or liq > best[0]:
                        best = (liq, usd_per_bnb)
                continue

            price_nat = p.get("priceNative")
            if price_usd is not None and price_nat not in (None, 0, "0"):
                try:
                    usd_per_bnb = float(price_usd) / float(price_nat)
                    if usd_per_bnb > 0:
                        if best is None or liq > best[0]:
                            best = (liq, usd_per_bnb)
                except Exception:
                    pass

        return best[1] if best else None
    except Exception:
        return None

def load_cg_list_cached(cache_path: Path, force_refresh: bool = False):
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if (not force_refresh) and cache_path.exists() and (time.time() - cache_path.stat().st_mtime) < 48 * 3600:
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

# ---------------- Web3 helper (retry & reuse) ---------------- #

def make_web3_with_retry(defi_module, rpc_url: str, attempts: int = 3, base_sleep: float = 2.0):
    last_err = None
    for i in range(attempts):
        try:
            w3 = defi_module.Web3(defi_module.Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 45}))
            defi_module._inject_poa(w3)
            if w3.is_connected():
                return w3
            last_err = RuntimeError("w3.is_connected() = False")
        except Exception as e:
            last_err = e
        time.sleep(base_sleep * (i + 1))
    raise RuntimeError(f"Connessione RPC fallita dopo {attempts} tentativi: {last_err}")

# ---------------- defi compute ---------------- #

def compute_push_amounts_with_defi(pool: str, rpc_url: str, w3=None):
    import importlib.util

    mod_path = Path(__file__).with_name("defiFunzionante.py")
    if not mod_path.exists():
        raise RuntimeError("defiFunzionante.py non trovato nella stessa cartella.")

    spec = importlib.util.spec_from_file_location("defiFunzionante", str(mod_path))
    defi = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(defi)  # type: ignore

    if w3 is None:
        w3 = make_web3_with_retry(defi, rpc_url, attempts=3, base_sleep=2.0)

    pool_addr = defi.Web3.to_checksum_address(pool)
    pool_contract = w3.eth.contract(address=pool_addr, abi=defi.UNISWAP_V3_POOL_MINI_ABI)
    snapshot = defi.read_pool_snapshot(w3, pool_addr)

    P0, rows = defi.compute_targets(
        snapshot,
        w3,
        pool_contract,
        factors=[Decimal("1.5"), Decimal("2.0"), Decimal("3.0")],
    )

    def parse_amount_token1(r):
        s = r["token1_needed"] or "0"
        num = (s.split()[0] if s else "0")
        try:
            return Decimal(num)
        except Exception:
            return Decimal("0")

    return {
        "token0_symbol": snapshot.token0.symbol,
        "token0_address": snapshot.token0.address.lower(),
        "token1_symbol": snapshot.token1.symbol,
        "token1_address": snapshot.token1.address.lower(),
        "P0": P0,
        "plus50_token1": parse_amount_token1(rows[0]),
        "plus100_token1": parse_amount_token1(rows[1]),
        "plus200_token1": parse_amount_token1(rows[2]),
        "plus50_str": rows[0]["token1_needed"],
        "plus100_str": rows[1]["token1_needed"],
        "plus200_str": rows[2]["token1_needed"],
    }

# ---------------- main ---------------- #

def main():
    ap = argparse.ArgumentParser(description="Pipeline: screener TOP (con pool) -> analisi DeFi (+50, +100%, +200%)")
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
    ap.add_argument("--debug-screener", action="store_true")
    ap.add_argument("--per-pool-debug", action="store_true")

    ap.add_argument("--require-v3", action="store_true", help="Forza solo pool Pancake v3 (compatibile con defiFunzionante).")

    ap.add_argument("--ttl-perps", type=int, default=None)
    ap.add_argument("--ttl-cg", type=int, default=None)
    ap.add_argument("--ttl-cg-list", type=int, default=None, help="TTL (sec) della coins list CoinGecko usata da test4.py")
    ap.add_argument("--refresh-perps", action="store_true")
    ap.add_argument("--refresh-cg", action="store_true")
    ap.add_argument("--refresh-all", action="store_true")

    args = ap.parse_args()

    start_all = time.perf_counter()

    # 1) Screener
    log("==> Avvio screener (test4.py)...")
    try:
        stdout, stderr = run_test4_and_capture(
            python_bin=args.python_bin,
            min_liq=args.min_liq,
            workers=args.workers,
            dominance=args.dominance,
            max_tickers_scan=args.max_tickers_scan,
            rps_cg=args.rps_cg,
            rps_ds=args.rps_ds,
            funnel_show=args.funnel_show,
            skip_unchanged_days=args.skip_unchanged_days,
            require_v3=args.require_v3,
            ttl_perps=args.ttl_perps,
            ttl_cg=args.ttl_cg,
            ttl_cg_list=args.ttl_cg_list,
            refresh_perps=args.refresh_perps,
            refresh_cg=args.refresh_cg,
            refresh_all=args.refresh_all,
        )
    except subprocess.CalledProcessError as e:
        log_err(0, 0, "-", "-", "Lancio test4.py fallito")
        print(e.stdout or "")
        print(e.stderr or "", file=sys.stderr)
        sys.exit(1)

    if args.debug_screener:
        print(">> ===== Screener stdout (debug) =====")
        print(stdout)
        print(">> ===== Screener stderr (debug) =====", file=sys.stderr)
        print(stderr, file=sys.stderr)
        print(">> ===================================")

    coins = parse_top_only(stdout)
    if not coins:
        tel_dir = Path("state/telegram")
        tel_dir.mkdir(parents=True, exist_ok=True)

        diag = (stderr or "").strip()
        msg = "⚠️ Nessuna coin in TOP trovata dall'output di test4.py."
        if diag:
            msg += "\n\n<pre>" + htmlesc(diag[-3500:]) + "</pre>"

        (tel_dir / "msg_summary.html").write_text(msg, encoding="utf-8")

        log("Nessuna coin in TOP trovata dall'output di test4.py.")
        if diag:
            print(diag, file=sys.stderr)

        sys.exit(2)

    if args.limit and args.limit > 0:
        coins = coins[:args.limit]

    n = len(coins)
    log(f"==> Trovate {n} coin in TOP. Inizio analisi per-pool...")

    # Esporta lista TOP per altri tool
    try:
        import json
        Path("state").mkdir(parents=True, exist_ok=True)
        top_pairs = [{"symbol": x["symbol"], "name": x["name"], "pair": x["pair"]} for x in coins]
        Path("state/top_pairs.json").write_text(json.dumps(top_pairs, ensure_ascii=False), encoding="utf-8")
        log("Salvato state/top_pairs.json per volume_watch.")
    except Exception:
        log("WARN: impossibile scrivere state/top_pairs.json")

    cg_list = None
    cg_cache = Path(".cache") / "cg_coins_list_include_platform.json"

    # Connessione web3 condivisa
    import importlib.util
    mod_path = Path(__file__).with_name("defiFunzionante.py")
    spec = importlib.util.spec_from_file_location("defiFunzionante", str(mod_path))
    defi_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(defi_mod)  # type: ignore
    try:
        w3_shared = make_web3_with_retry(defi_mod, args.rpc, attempts=3, base_sleep=2.0)
    except Exception as e:
        log_err(0, 0, "-", "-", f"Impossibile connettersi all'RPC iniziale: {e}")
        sys.exit(1)

    rows = []
    tel_dir = Path("state/telegram")
    tel_dir.mkdir(parents=True, exist_ok=True)

    bnb_usd_global = fetch_bnb_usd_from_ds(prefer_chain="bsc")

    for idx, c in enumerate(coins, start=1):
        symbol = c["symbol"]
        name = c["name"]
        pair = c["pair"]
        lp_usd = c["lp_usd"]

        t0 = time.perf_counter()
        log_step(idx, n, symbol, pair, f"START | Name='{name}' | LP={format_usd(lp_usd)}")

        # 2) DexScreener pair info
        try:
            log_step(idx, n, symbol, pair, "DexScreener: fetch pair info...")
            ds_t0 = time.perf_counter()
            ds = fetch_ds_pair_info(pair)
            ds_dt = time.perf_counter() - ds_t0
            marketcap_usd = ds.get("marketcap")
            dexId = ds.get("dexId")
            log_step(
                idx, n, symbol, pair,
                f"DexScreener OK in {ds_dt:.2f}s | dexId={dexId} | mcap={format_usd(marketcap_usd) if marketcap_usd else 'n.d.'}"
            )
        except Exception as e:
            ds = {"marketcap": None, "dexId": None, "priceUsd": None, "baseToken": {}, "quoteToken": {}, "url": None}
            marketcap_usd = None
            log_err(idx, n, symbol, pair, f"DexScreener KO: {e}")

        # 2b) CoinGecko fallback per marketcap
        if (marketcap_usd is None) and (not args.no_cg_fallback):
            try:
                if cg_list is None:
                    log_step(idx, n, symbol, pair, "CoinGecko: download/uso cache coin list...")
                    cg_list = load_cg_list_cached(
                        cg_cache,
                        force_refresh=bool(args.refresh_cg or args.refresh_all),
                    )
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

        # 3) DeFi calc
        try:
            log_step(idx, n, symbol, pair, "DeFi: compute +50/+100/+200...")
            d0 = time.perf_counter()
            push = compute_push_amounts_with_defi(pair, args.rpc, w3=w3_shared)
            ddt = time.perf_counter() - d0

            plus50_str = push["plus50_str"]
            plus100_str = push["plus100_str"]
            plus200_str = push["plus200_str"]

            log_step(idx, n, symbol, pair, f"DeFi OK in {ddt:.2f}s | token1={push['token1_symbol']} | +50={plus50_str} | +100={plus100_str} | +200={plus200_str}")
        except Exception as e:
            msg = str(e)
            if args.per_pool_debug:
                log_err(idx, n, symbol, pair, f"DEBUG ERR: {msg}")
            plus50_disp = plus100_disp = plus200_disp = f"ERR ({msg})"
            rows.append(
                {
                    "symbol": symbol,
                    "name": name,
                    "pair": pair,
                    "lp_usd": lp_usd,
                    "marketcap_usd": marketcap_usd,
                    "plus50": plus50_disp,
                    "plus100": plus100_disp,
                    "plus200": plus200_disp,
                    "plus50_usd_value": None,
                    "plus100_usd_value": None,
                    "plus200_usd_value": None,
                    "score50": None,
                    "score100": None,
                    "score200": None,
                    "url": ds.get("url") or f"https://dexscreener.com/bsc/{pair}",
                    "priceUsd": ds.get("priceUsd"),
                    "baseToken": ds.get("baseToken") or {},
                }
            )
            log_step(idx, n, symbol, pair, f"DeFi risultato: {plus50_disp}")
            continue

        # --- Variabili token per fallback esterno ---
        t0_addr = (push["token0_address"] or "").lower()
        t1_addr = (push["token1_address"] or "").lower()
        t0_sym  = (push["token0_symbol"] or "").upper().strip()
        t1_sym  = (push["token1_symbol"] or "").upper().strip()

        # 4) Conversione ≈ USDT (U1 = USD/token1)
        U1 = None

        STABLE_SYMS = {"USDT", "USDC", "BUSD", "FDUSD", "DAI", "USDD", "USDP", "TUSD", "USD1"}
        STABLE_ADDRS_BSC = {
            "0x55d398326f99059ff775485246999027b3197955",
            "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
            "0xe9e7cea3dedca5984780bafc599bd69add087d56",
            "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3",
            "0xd17479997f34dd9156deef8f95a52d81d265be9c",
            "0x1456688345527be1f37e9e627da0837d6f08c925",
            "0x14016e85a25aeb13065688cafb43044c2ef86784",
            "0xbd0a4bf098261673d5e6e600fd87ddcd756efb62",
        }

        NATIVE_WRAPPED_SYMS_BSC = {"WBNB"}
        NATIVE_WRAPPED_ADDRS_BSC = {"0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"}

        try:
            P0_dec = push["P0"]
            priceUsd = ds.get("priceUsd")
            priceNative = ds.get("priceNative")

            base_info  = ds.get("baseToken")  or {}
            quote_info = ds.get("quoteToken") or {}

            base_addr  = (base_info.get("address") or "").lower()
            quote_addr = (quote_info.get("address") or "").lower()
            base_sym   = (base_info.get("symbol")  or "").upper().strip()
            quote_sym  = (quote_info.get("symbol") or "").upper().strip()

            if (t1_sym in STABLE_SYMS) or (t1_addr in STABLE_ADDRS_BSC):
                U1 = Decimal("1")

            if U1 is None and priceUsd is not None:
                base_is_t0  = bool(base_addr)  and (base_addr  == t0_addr)
                base_is_t1  = bool(base_addr)  and (base_addr  == t1_addr)
                quote_is_t0 = bool(quote_addr) and (quote_addr == t0_addr)
                quote_is_t1 = bool(quote_addr) and (quote_addr == t1_addr)

                if base_is_t1 or quote_is_t0:
                    U1 = Decimal(str(priceUsd))
                elif (base_is_t0 or quote_is_t1) and P0_dec is not None and P0_dec != 0:
                    U1 = Decimal(str(priceUsd)) / P0_dec

            if (
                U1 is None and priceUsd is not None and priceNative is not None and
                (t1_sym in NATIVE_WRAPPED_SYMS_BSC or t1_addr in NATIVE_WRAPPED_ADDRS_BSC)
            ):
                denom = Decimal(str(priceNative))
                if denom != 0:
                    U1 = Decimal(str(priceUsd)) / denom

            if U1 is None and priceUsd is not None:
                base_is_t0_sym  = base_sym  and (base_sym  == t0_sym)
                base_is_t1_sym  = base_sym  and (base_sym  == t1_sym)
                quote_is_t0_sym = quote_sym and (quote_sym == t0_sym)
                quote_is_t1_sym = quote_sym and (quote_sym == t1_sym)

                if base_is_t1_sym or quote_is_t0_sym:
                    U1 = Decimal(str(priceUsd))
                elif (base_is_t0_sym or quote_is_t1_sym) and P0_dec is not None and P0_dec != 0:
                    U1 = Decimal(str(priceUsd)) / P0_dec

            if U1 is None:
                alt_t1 = fetch_ds_token_price_usd_for_pair(t1_addr, pair)
                if alt_t1 is not None:
                    U1 = Decimal(str(alt_t1))

            if U1 is None:
                alt_t0 = fetch_ds_token_price_usd_for_pair(t0_addr, pair)
                if alt_t0 is not None and P0_dec is not None and P0_dec != 0:
                    U1 = Decimal(str(alt_t0)) / P0_dec

            if U1 is None and (t1_sym in NATIVE_WRAPPED_SYMS_BSC or t1_addr in NATIVE_WRAPPED_ADDRS_BSC):
                bnb_usd = fetch_bnb_usd_from_ds(prefer_chain="bsc")
                if bnb_usd is not None and bnb_usd > 0:
                    U1 = Decimal(str(bnb_usd))

            if U1 is not None and (U1 <= 0 or U1 < Decimal("1e-9")):
                U1 = None
        except Exception:
            U1 = None

        if U1 is None and (t1_sym in {"WBNB"} or t1_addr in {"0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"}):
            if bnb_usd_global is None:
                bnb_usd_global = fetch_bnb_usd_from_ds(prefer_chain="bsc")
            if bnb_usd_global is not None and bnb_usd_global > 0:
                U1 = Decimal(str(bnb_usd_global))

        def decfmt(d: Decimal, decimals: int = 6) -> str:
            try:
                q = Decimal("1." + "0"*decimals)
                return str(d.quantize(q).normalize())
            except Exception:
                return str(d)

        def safe_div(num, den):
            try:
                if num is None or den is None:
                    return None
                if float(den) == 0.0:
                    return None
                return float(num) / float(den)
            except Exception:
                return None

        a50 = push["plus50_token1"]
        a100 = push["plus100_token1"]
        a200 = push["plus200_token1"]

        if U1 is not None:
            plus50_usd_value = float(a50 * U1)
            plus100_usd_value = float(a100 * U1)
            plus200_usd_value = float(a200 * U1)
        else:
            plus50_usd_value = plus100_usd_value = plus200_usd_value = None

        marketcap_val = marketcap_usd if marketcap_usd is not None else None
        score50  = safe_div(marketcap_val, plus50_usd_value)
        score100 = safe_div(marketcap_val, plus100_usd_value)
        score200 = safe_div(marketcap_val, plus200_usd_value)

        def usd_fmt(x):
            return format_usd(x) if x is not None else "n.d."

        plus50_disp = f"{decfmt(a50)} {push['token1_symbol']} | {usd_fmt(plus50_usd_value)}"
        plus100_disp = f"{decfmt(a100)} {push['token1_symbol']} | {usd_fmt(plus100_usd_value)}"
        plus200_disp = f"{decfmt(a200)} {push['token1_symbol']} | {usd_fmt(plus200_usd_value)}"

        row = {
            "symbol": symbol,
            "name": name,
            "pair": pair,
            "lp_usd": lp_usd,
            "marketcap_usd": marketcap_usd,
            "plus50": plus50_disp,
            "plus100": plus100_disp,
            "plus200": plus200_disp,
            "plus50_usd_value": plus50_usd_value,
            "plus100_usd_value": plus100_usd_value,
            "plus200_usd_value": plus200_usd_value,
            "score50": score50,
            "score100": score100,
            "score200": score200,
            "url": ds.get("url") or f"https://dexscreener.com/bsc/{pair}",
            "priceUsd": ds.get("priceUsd"),
            "baseToken": ds.get("baseToken") or {},
        }
        rows.append(row)

        base_sym2 = (row["baseToken"].get("symbol") or "BASE")
        price_str = f"{row['priceUsd']:.8f} USD / {base_sym2}" if row["priceUsd"] is not None else "n.d."
        mc_str = format_usd(row["marketcap_usd"]) if row["marketcap_usd"] is not None else "n.d."
        lp_str = format_usd(row["lp_usd"])
        s50 = f"{row['score50']:.2f}x" if row.get("score50") is not None else "n.d."
        s100 = f"{row['score100']:.2f}x" if row.get("score100") is not None else "n.d."
        s200 = f"{row['score200']:.2f}x" if row.get("score200") is not None else "n.d."

        msg_coin = (
            f"💠 <b>{htmlesc(symbol)}</b> — {htmlesc(name)}\n"
            f"Market Cap: <b>{mc_str}</b>\n"
            f"Liquidity: <b>{lp_str}</b>\n"
            f"Price: <b>{price_str}</b>\n"
            f"Address: <a href=\"{row['url']}\">{row['pair']}</a>\n"
            f"<code>{row['pair']}</code>\n\n"
            f"<b>Token1</b>: {htmlesc(push['token1_symbol'])}\n"
            f"+50%: {htmlesc(plus50_disp)}\n"
            f"+100%: {htmlesc(plus100_disp)}\n"
            f"+200%: {htmlesc(plus200_disp)}\n\n"
            f"SCORE50: <b>{s50}</b> | SCORE100: <b>{s100}</b> | SCORE200: <b>{s200}</b>"
        )
        out_coin = tel_dir / f"coin_{idx:03d}_{symbol}.html"
        out_coin.write_text(msg_coin, encoding="utf-8")

        if args.sleep and args.sleep > 0:
            log_step(idx, n, symbol, pair, f"sleep {args.sleep:.2f}s")
            time.sleep(args.sleep)

        dt = time.perf_counter() - t0
        log_step(idx, n, symbol, pair, f"DONE in {dt:.2f}s")

    # ---------------- sorting & summary ---------------- #

    def key_score100(r):
        v = r.get("score100")
        # Metti None in fondo
        return (-v) if isinstance(v, (int, float)) else float("inf")

    rows_sorted = sorted(rows, key=key_score100)

    # salva report JSON
    try:
        import json
        Path("state").mkdir(parents=True, exist_ok=True)
        Path("state/results.json").write_text(json.dumps(rows_sorted, ensure_ascii=False, indent=2), encoding="utf-8")
        log("Salvato state/results.json")
    except Exception:
        log("WARN: impossibile scrivere state/results.json")

    # Costruisci report HTML riassuntivo per telegram (un solo file)
    lines = []
    lines.append("📌 <b>TOP Screener + DeFi</b>\n")

    for i, r in enumerate(rows_sorted, start=1):
        sym = r.get("symbol") or "?"
        name = r.get("name") or ""
        pair = r.get("pair") or ""
        url = r.get("url") or f"https://dexscreener.com/bsc/{pair}"
        mc = r.get("marketcap_usd")
        lp = r.get("lp_usd")

        mc_str = format_usd(mc) if mc is not None else "n.d."
        lp_str = format_usd(lp) if lp is not None else "n.d."

        s50 = r.get("score50")
        s100 = r.get("score100")
        s200 = r.get("score200")

        s50_str = f"{s50:.2f}x" if isinstance(s50, (int, float)) else "n.d."
        s100_str = f"{s100:.2f}x" if isinstance(s100, (int, float)) else "n.d."
        s200_str = f"{s200:.2f}x" if isinstance(s200, (int, float)) else "n.d."

        plus50 = r.get("plus50") or "n.d."
        plus100 = r.get("plus100") or "n.d."
        plus200 = r.get("plus200") or "n.d."

        lines.append(
            f"#{i} — <b>{htmlesc(sym)}</b> — {htmlesc(name)}\n"
            f"MC: <b>{mc_str}</b> | LP: <b>{lp_str}</b>\n"
            f"SCORE50: <b>{htmlesc(s50_str)}</b> | SCORE100: <b>{htmlesc(s100_str)}</b> | SCORE200: <b>{htmlesc(s200_str)}</b>\n"
            f"+50%: {htmlesc(plus50)}\n"
            f"+100%: {htmlesc(plus100)}\n"
            f"+200%: {htmlesc(plus200)}\n"
            f"Pair: <a href=\"{htmlesc(url)}\">{htmlesc(pair)}</a>\n"
            f"<code>{htmlesc(pair)}</code>\n"
        )

    summary_html = "\n".join(lines).strip() + "\n"

    tel_dir = Path("state/telegram")
    tel_dir.mkdir(parents=True, exist_ok=True)
    (tel_dir / "msg_summary.html").write_text(summary_html, encoding="utf-8")
    log("Salvato state/telegram/msg_summary.html")

    # anche un txt comodo (facoltativo)
    try:
        summary_txt = re.sub(r"<[^>]+>", "", summary_html)
        (tel_dir / "msg_summary.txt").write_text(summary_txt, encoding="utf-8")
        log("Salvato state/telegram/msg_summary.txt")
    except Exception:
        pass

    total_dt = time.perf_counter() - start_all
    log(f"==> Pipeline completata in {total_dt:.2f}s | risultati={len(rows_sorted)}")

    # Exit code OK
    sys.exit(0)


if __name__ == "__main__":
    main()


