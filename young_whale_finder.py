#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Young Whale Finder — BSC (Pancake v3) • ultimi N minuti

Dato un token ERC-20 (la "coin"), cerca negli ultimi M minuti i BUY sulla pool
principale Pancake (BSC). Per ogni buy >= soglia USD:
  - identifica il wallet acquirente (tx.from),
  - se il wallet è "giovane" (prima tx < X giorni fa), lo tiene,
  - salva quanto possiede attualmente di quella coin (balanceOf).

Output: JSON con {address, balance_token, balance_usd, first_tx_ts, sum_buys_usd_window}

python .\young_whale_finder.py --token 0x48a18A4782b65a0fBed4dcA608BB28038B7bE339 --pair  0x6531a0190C9977E48b3a5c8bc5226a6cD821d18c --minutes 5 --min-usd 1000 --young-days 5 --chunk-blocks 600 --chunk-sleep 0.10 --rps-rpc 6   --verbose


Dipendenze:
  pip install web3 requests tenacity rich
"""

from __future__ import annotations
import argparse
import json
import time
from dataclasses import dataclass
from decimal import Decimal
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from web3 import Web3
from web3.types import LogReceipt
from web3.exceptions import ExtraDataLengthError, Web3RPCError

# --------- Rich (opzionale) --------- #
try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.progress import Progress, BarColumn, TimeElapsedColumn, TimeRemainingColumn, TextColumn
    from rich.rule import Rule
    from rich import box
    RICH = True
    console = Console()
except Exception:
    RICH = False
    console = None

def rlog(msg: str):
    if args.quiet:
        return
    if RICH and console:
        console.log(msg)
    else:
        print(msg)

def rwarn(msg: str):
    if args.quiet:
        return
    if RICH and console:
        console.print(f"[yellow][WARN][/yellow] {msg}")
    else:
        print(f"[WARN] {msg}")

def rerr(msg: str):
    if RICH and console:
        console.print(f"[red][ERROR][/red] {msg}")
    else:
        print(f"[ERROR] {msg}")

def rpanel(title: str, body: str, style: str = "cyan"):
    if args.quiet or not RICH or not console:
        return
    console.print(Panel(body, title=title, border_style=style))

# -------------------- Config -------------------- #

DEX_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{address}"
DEX_PAIR_URL  = "https://api.dexscreener.com/latest/dex/pairs/bsc/{pair}"

DEFAULT_RPC = "https://bsc-dataseed.binance.org"
AVG_BLOCK_TIME_SEC = 3.0  # BSC ~3s

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
HTTP_TIMEOUT = 25
HEADERS = {"Accept": "application/json", "User-Agent": UA}

# -------------------- ABIs -------------------- #

UNIV3_POOL_ABI = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True,  "internalType": "address", "name": "sender",    "type": "address"},
            {"indexed": True,  "internalType": "address", "name": "recipient", "type": "address"},
            {"indexed": False, "internalType": "int256",  "name": "amount0",   "type": "int256"},
            {"indexed": False, "internalType": "int256",  "name": "amount1",   "type": "int256"},
            {"indexed": False, "internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160"},
            {"indexed": False, "internalType": "uint128", "name": "liquidity", "type": "uint128"},
            {"indexed": False, "internalType": "int24",   "name": "tick",      "type": "int24"}
        ],
        "name": "Swap",
        "type": "event"
    },
    {"inputs": [], "name": "token0", "outputs": [{"internalType": "address", "name": "", "type": "address"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "token1", "outputs": [{"internalType": "address", "name": "", "type": "address"}], "stateMutability": "view", "type": "function"},
]

ERC20_ABI_MIN = [
    {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name":"","type":"uint8"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "symbol",   "outputs": [{"name":"","type":"string"}], "type": "function"},
    {"constant": True, "inputs": [{"name":"a","type":"address"}], "name": "balanceOf", "outputs": [{"name":"","type":"uint256"}], "type": "function"},
]

# -------------------- HTTP -------------------- #

@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=1, max=8),
       retry=retry_if_exception_type((requests.RequestException,)))
def http_get_json(url: str, **kwargs) -> dict:
    headers = kwargs.pop("headers", {})
    h = {**HEADERS, **headers}
    r = requests.get(url, headers=h, timeout=HTTP_TIMEOUT, **kwargs)
    r.raise_for_status()
    return r.json()

# -------------------- Rate -------------------- #

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

# -------------------- Args -------------------- #

@dataclass
class Args:
    rpc: str
    token: str
    pair: Optional[str]
    minutes: int
    min_usd: float
    young_days: int
    rps_rpc: float
    rps_http: float
    bscscan_key: Optional[str]
    max_logs: int
    chunk_blocks: int
    chunk_sleep: float
    verbose: bool
    quiet: bool

def parse_args() -> Args:
    ap = argparse.ArgumentParser(description="Young Whale Finder (BSC, Pancake v3)")
    ap.add_argument("--rpc", default=DEFAULT_RPC)
    ap.add_argument("--token", required=True)
    ap.add_argument("--pair", default=None)
    ap.add_argument("--minutes", type=int, default=5)
    ap.add_argument("--min-usd", type=float, default=1000.0)
    ap.add_argument("--young-days", type=int, default=5)
    ap.add_argument("--rps-rpc", type=float, default=10.0)
    ap.add_argument("--rps-http", type=float, default=2.0)
    ap.add_argument("--bscscan-key", default=None)
    ap.add_argument("--max-logs", type=int, default=5000)
    ap.add_argument("--chunk-blocks", type=int, default=1200, help="ampiezza iniziale del chunk per getLogs (adattivo)")
    ap.add_argument("--chunk-sleep", type=float, default=0.05, help="sleep tra chunk per non farsi rate-limitare")
    ap.add_argument("--verbose", action="store_true", help="log dettagliati")
    ap.add_argument("--quiet", action="store_true", help="sopprime i log (stamperà solo il JSON finale)")
    a = ap.parse_args()
    return Args(
        rpc=a.rpc,
        token=Web3.to_checksum_address(a.token),
        pair=(Web3.to_checksum_address(a.pair) if a.pair else None),
        minutes=max(1, a.minutes),
        min_usd=max(0.0, a.min_usd),
        young_days=max(1, a.young_days),
        rps_rpc=max(1.0, a.rps_rpc),
        rps_http=max(0.5, a.rps_http),
        bscscan_key=a.bscscan_key or None,
        max_logs=max(100, a.max_logs),
        chunk_blocks=max(100, a.chunk_blocks),
        chunk_sleep=max(0.0, a.chunk_sleep),
        verbose=bool(a.verbose),
        quiet=bool(a.quiet),
    )

# -------------------- DexScreener -------------------- #

def pick_main_bsc_pool(token_addr: str, http_rl: SimpleRate) -> Tuple[str, Optional[float]]:
    http_rl.wait()
    data = http_get_json(DEX_TOKEN_URL.format(address=token_addr))
    pairs = data.get("pairs") or []
    best = None
    best_liq = -1.0
    for p in pairs:
        if (p.get("chainId") or "").lower() != "bsc":
            continue
        if "pancake" not in (p.get("dexId","").lower()):
            continue
        liq = float(((p.get("liquidity") or {}).get("usd") or 0))
        if liq > best_liq:
            best_liq = liq
            best = p
    if not best:
        raise RuntimeError("Nessuna pool Pancake BSC trovata per il token.")
    pair = (best.get("pairAddress") or "").lower()
    price_usd = None
    try:
        price_usd = float(best.get("priceUsd")) if best.get("priceUsd") is not None else None
    except Exception:
        price_usd = None
    return Web3.to_checksum_address(pair), price_usd

def refresh_pair_price(pair_addr: str, http_rl: SimpleRate) -> Optional[float]:
    http_rl.wait()
    j = http_get_json(DEX_PAIR_URL.format(pair=pair_addr))
    p = (j.get("pairs") or [None])[0] or {}
    try:
        return float(p.get("priceUsd")) if p.get("priceUsd") is not None else None
    except Exception:
        return None

# -------------------- POA middleware -------------------- #

def inject_poa(w3: Web3):
    """Prova entrambe le middlewares POA, compatibili con diverse versioni web3."""
    try:
        from web3.middleware import geth_poa_middleware
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    except Exception:
        pass
    try:
        try:
            from web3.middleware.proof_of_authority import ExtraDataToPOAMiddleware  # web3<6
        except Exception:
            from web3.middleware import ExtraDataToPOAMiddleware  # web3>=6
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    except Exception:
        pass

def warmup_poa(w3: Web3):
    for _ in range(3):
        try:
            w3.eth.get_block("latest")
            return
        except ExtraDataLengthError:
            inject_poa(w3)
            time.sleep(0.05)
        except Exception as e:
            if "extraData" in str(e):
                inject_poa(w3)
                time.sleep(0.05)
            else:
                return

# -------------------- RPC utils -------------------- #

@lru_cache(maxsize=4096)
def get_block_ts(w3: Web3, blk: int, rpc_rl: SimpleRate) -> int:
    rpc_rl.wait()
    for _ in range(3):
        try:
            b = w3.eth.get_block(blk, full_transactions=False)
            return int(b["timestamp"])
        except ExtraDataLengthError:
            inject_poa(w3); time.sleep(0.05)
        except Exception as e:
            if "extraData" in str(e):
                inject_poa(w3); time.sleep(0.05)
            else:
                break
    b = w3.eth.get_block(blk, full_transactions=False)
    return int(b["timestamp"])

def find_from_block_for_window(w3: Web3, latest: int, seconds: int, rpc_rl: SimpleRate) -> int:
    target_ts = int(time.time()) - seconds
    lo = 0
    step = int(seconds / AVG_BLOCK_TIME_SEC)
    step = max(50, min(step, 50000))
    b = latest
    while b > 0:
        ts = get_block_ts(w3, b, rpc_rl)
        if ts <= target_ts:
            lo = b
            break
        b = max(0, b - step)
        step = int(step * 1.5)
        if b == 0:
            break
    l, r = lo, latest
    while l + 1 < r:
        m = (l + r) // 2
        ts = get_block_ts(w3, m, rpc_rl)
        if ts <= target_ts:
            l = m
        else:
            r = m
    return max(0, l)

def get_token_meta(w3: Web3, token: str, rpc_rl: SimpleRate) -> Tuple[int, str]:
    c = w3.eth.contract(address=token, abi=ERC20_ABI_MIN)
    rpc_rl.wait()
    decimals = int(c.functions.decimals().call())
    sym = "TOKEN"
    try:
        rpc_rl.wait()
        sym = c.functions.symbol().call()
    except Exception:
        pass
    return decimals, sym

def get_wallet_balance(w3: Web3, token: str, holder: str, rpc_rl: SimpleRate) -> int:
    c = w3.eth.contract(address=token, abi=ERC20_ABI_MIN)
    rpc_rl.wait()
    return int(c.functions.balanceOf(holder).call())

def tx_from(w3: Web3, txhash: str, rpc_rl: SimpleRate) -> str:
    rpc_rl.wait()
    tx = w3.eth.get_transaction(txhash)
    return Web3.to_checksum_address(tx["from"])

def is_contract(w3: Web3, addr: str, rpc_rl: SimpleRate) -> bool:
    rpc_rl.wait()
    code = w3.eth.get_code(addr)
    return bool(code and len(code) > 0)

# ---- Wallet age ---- #

def first_tx_ts_bscscan(addr: str, api_key: str, http_rl: SimpleRate) -> Optional[int]:
    http_rl.wait()
    url = (
        "https://api.bscscan.com/api"
        "?module=account&action=txlist"
        f"&address={addr}"
        "&startblock=0&endblock=99999999&page=1&offset=1&sort=asc"
        f"&apikey={api_key}"
    )
    j = http_get_json(url)
    if str(j.get("status","1")) != "1":
        return None
    res = j.get("result") or []
    if not res:
        return None
    try:
        return int(res[0]["timeStamp"])
    except Exception:
        return None

def first_tx_ts_rpc_heuristic(w3: Web3, addr: str, latest_blk: int, max_days: int, rpc_rl: SimpleRate) -> Optional[int]:
    seconds = max_days * 86400
    start_blk = max(0, latest_blk - int(seconds / AVG_BLOCK_TIME_SEC))
    rpc_rl.wait()
    try:
        tc = w3.eth.get_transaction_count(addr, start_blk)
    except Exception:
        tc = 0
    if tc > 0:
        return 0  # troppo vecchio
    rpc_rl.wait()
    tc_latest = w3.eth.get_transaction_count(addr, latest_blk)
    if tc_latest == 0:
        return None
    l, r = start_blk, latest_blk
    first_blk = latest_blk
    while l <= r:
        m = (l + r) // 2
        rpc_rl.wait()
        tcm = w3.eth.get_transaction_count(addr, m)
        if tcm > 0:
            first_blk = m
            r = m - 1
        else:
            l = m + 1
    ts = get_block_ts(w3, first_blk, rpc_rl)
    return ts

# -------------------- Logs (chunk adattivo + progress) -------------------- #

def get_swap_logs_adaptive_with_progress(event, start_blk: int, end_blk: int, base_chunk: int, sleep_s: float, verbose: bool) -> List[LogReceipt]:
    logs: List[LogReceipt] = []
    cur = start_blk
    chunk = max(100, base_chunk)
    total_blocks = max(0, end_blk - start_blk + 1)

    if RICH and console and not args.quiet:
        progress = Progress(
            TextColumn("[bold blue]Log scan[/bold blue]"),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•",
            TextColumn("blk {task.completed}/{task.total}"),
            "•",
            TimeElapsedColumn(),
            "•",
            TimeRemainingColumn(),
            console=console,
            transient=True,
        )
        task = progress.add_task("scan", total=total_blocks)
        progress.start()
    else:
        progress = None
        task = None

    while cur <= end_blk:
        to_blk = min(end_blk, cur + chunk)
        try:
            part = event.get_logs(from_block=cur, to_block=to_blk)
            logs.extend(part)
            # avanzamento
            if progress:
                progress.update(task, advance=(to_blk - cur + 1))
            cur = to_blk + 1
            if verbose and not args.quiet:
                rlog(f"Chunk ok [{cur-1-chunk}→{to_blk}] +{len(part)} log (chunk={chunk})")
            if chunk < 4000:
                chunk = int(chunk * 1.25)
        except Web3RPCError as e:
            msg = (str(e) or "").lower()
            if "-32005" in msg or "limit exceeded" in msg:
                if verbose and not args.quiet:
                    rwarn(f"limit exceeded @ [{cur}→{to_blk}] ⇒ dimezzo chunk ({chunk}→{max(100, chunk//2)})")
                chunk = max(100, chunk // 2)
                time.sleep(max(0.02, sleep_s))
                continue
            time.sleep(max(0.05, sleep_s))
            continue
        except ValueError:
            if verbose and not args.quiet:
                rwarn(f"ValueError @ [{cur}→{to_blk}] ⇒ riduco chunk ({chunk}→{max(100, chunk//2)})")
            if chunk <= 100:
                time.sleep(max(0.02, sleep_s))
                try:
                    part = event.get_logs(from_block=cur, to_block=min(end_blk, cur + 50))
                    logs.extend(part)
                    if progress:
                        progress.update(task, advance=51)
                    cur = cur + 51
                    continue
                except Exception:
                    time.sleep(0.1)
                    continue
            chunk = max(100, chunk // 2)
            time.sleep(max(0.02, sleep_s))
            continue

        if sleep_s > 0:
            time.sleep(sleep_s)

    if progress:
        progress.stop()

    return logs

# -------------------- Main -------------------- #

def banner_and_config(args):
    if args.quiet:
        return
    if RICH and console:
        console.print(Rule("[bold]Young Whale Finder — BSC / Pancake v3[/bold]"))
        body = (
            f"[b]RPC:[/b] {args.rpc}\n"
            f"[b]Token:[/b] {args.token}\n"
            f"[b]Pair:[/b] {args.pair or 'auto (DexScreener)'}\n"
            f"[b]Window:[/b] {args.minutes} min    [b]Min USD:[/b] {args.min_usd}\n"
            f"[b]Young days:[/b] {args.young_days}\n"
            f"[b]Chunk:[/b] {args.chunk_blocks} blk    [b]Sleep/chunk:[/b] {args.chunk_sleep}s\n"
            f"[b]Rate RPC:[/b] {args.rps_rpc} rps    [b]Rate HTTP:[/b] {args.rps_http} rps\n"
            f"[b]Verbose:[/b] {args.verbose}    [b]Quiet:[/b] {args.quiet}"
        )
        rpanel("Configurazione", body, "magenta")
    else:
        rlog("Young Whale Finder — BSC / Pancake v3")
        rlog(f"Token={args.token} | Pair={args.pair or 'auto'} | Window={args.minutes}min | MinUSD={args.min_usd}")

def final_summary(stats: Dict[str, int]):
    if args.quiet:
        return
    if RICH and console:
        t = Table(title="Riepilogo", box=box.SIMPLE_HEAVY)
        t.add_column("Metrica")
        t.add_column("Valore", justify="right")
        for k, v in stats.items():
            t.add_row(k, str(v))
        console.print(t)
    else:
        rlog("=== Riepilogo ===")
        for k, v in stats.items():
            rlog(f"{k}: {v}")

def main():
    global args
    args = parse_args()
    rpc_rl = SimpleRate(args.rps_rpc)
    http_rl = SimpleRate(args.rps_http)

    banner_and_config(args)

    # Connessione RPC
    if not args.quiet and RICH and console:
        with console.status("[bold blue]Connessione RPC & warmup POA...[/bold blue]"):
            w3 = Web3(Web3.HTTPProvider(args.rpc, request_kwargs={"timeout": 30}))
            inject_poa(w3)
            warmup_poa(w3)
            if not w3.is_connected():
                raise RuntimeError("RPC non raggiungibile.")
            latest = w3.eth.block_number
    else:
        w3 = Web3(Web3.HTTPProvider(args.rpc, request_kwargs={"timeout": 30}))
        inject_poa(w3)
        warmup_poa(w3)
        if not w3.is_connected():
            raise RuntimeError("RPC non raggiungibile.")
        latest = w3.eth.block_number

    rlog(f"Latest block: {latest}")

    # Pair + prezzo
    if not args.quiet and RICH and console:
        with console.status("[bold blue]Individuazione pair & prezzo...[/bold blue]"):
            if args.pair:
                pair_addr = args.pair
                price_usd = refresh_pair_price(pair_addr, http_rl)
            else:
                pair_addr, price_usd = pick_main_bsc_pool(args.token, http_rl)
    else:
        if args.pair:
            pair_addr = args.pair
            price_usd = refresh_pair_price(pair_addr, http_rl)
        else:
            pair_addr, price_usd = pick_main_bsc_pool(args.token, http_rl)

    rlog(f"Pair: {pair_addr}  | priceUsd(baseToken)={price_usd if price_usd is not None else 'n.d.'}")

    pool = w3.eth.contract(address=pair_addr, abi=UNIV3_POOL_ABI)

    # token side
    rpc_rl.wait()
    token0 = Web3.to_checksum_address(pool.functions.token0().call())
    rpc_rl.wait()
    token1 = Web3.to_checksum_address(pool.functions.token1().call())
    target_is_t0 = (args.token == token0)
    target_is_t1 = (args.token == token1)
    if not (target_is_t0 or target_is_t1):
        raise RuntimeError("Il token non è presente in questa pool. Passa la pool corretta con --pair.")

    dec, sym = get_token_meta(w3, args.token, rpc_rl)
    rlog(f"Token symbol: {sym} | Decimals: {dec} | Side: {'token0' if target_is_t0 else 'token1'}")

    seconds = args.minutes * 60
    from_blk = find_from_block_for_window(w3, latest, seconds, rpc_rl)
    rlog(f"Block window: {from_blk} → {latest} (~{args.minutes} min)")

    # logs con chunk adattivo + progress
    swap_event = pool.events.Swap()
    t0 = time.perf_counter()
    logs: List[LogReceipt] = get_swap_logs_adaptive_with_progress(
        swap_event, start_blk=from_blk, end_blk=latest,
        base_chunk=args.chunk_blocks, sleep_s=args.chunk_sleep, verbose=args.verbose
    )
    dt_logs = time.perf_counter() - t0
    rlog(f"Log letti: {len(logs)} in {dt_logs:.2f}s")

    if len(logs) > args.max_logs:
        logs = logs[-args.max_logs:]
        rwarn(f"Cap a max_logs={args.max_logs}: tengo gli ultimi {len(logs)} log")

    if price_usd is None:
        price_usd = refresh_pair_price(pair_addr, http_rl)
        rlog(f"priceUsd refresh: {price_usd if price_usd is not None else 'n.d.'}")

    qualifying_tx: List[Tuple[str, float, str, int]] = []
    buys_checked = 0

    # Filtra BUY >= min_usd
    for ev in logs:
        a0 = Decimal(ev["args"]["amount0"])
        a1 = Decimal(ev["args"]["amount1"])
        bought_raw = None
        if target_is_t0 and a0 < 0:
            bought_raw = -a0
        elif target_is_t1 and a1 < 0:
            bought_raw = -a1
        if bought_raw is None:
            continue

        qty = float(bought_raw) / (10 ** dec)
        if price_usd is None:
            continue
        usd_val = qty * float(price_usd)
        buys_checked += 1
        if usd_val >= args.min_usd:
            buyer = tx_from(w3, ev["transactionHash"].hex(), rpc_rl)
            qualifying_tx.append((buyer, usd_val, ev["transactionHash"].hex(), ev["blockNumber"]))
            if args.verbose and not args.quiet:
                rlog(f"BUY >= {args.min_usd}$ | {buyer} | {usd_val:.2f}$ | tx={ev['transactionHash'].hex()}")

    # Dedup per wallet (somma USD nella finestra)
    buyers: Dict[str, float] = {}
    for addr, usd, _tx, _blk in qualifying_tx:
        buyers[addr] = buyers.get(addr, 0.0) + float(usd)

    rlog(f"Buy >= soglia: {len(qualifying_tx)} | Wallet unici: {len(buyers)}")

    results = []
    young_cutoff_ts = int(time.time()) - args.young_days * 86400

    kept = 0
    skipped_old = 0
    skipped_contract = 0

    for addr, sum_usd in buyers.items():
        if is_contract(w3, addr, rpc_rl):
            skipped_contract += 1
            if args.verbose and not args.quiet:
                rwarn(f"Scarto contract: {addr}")
            continue

        if args.bscscan_key:
            first_ts = first_tx_ts_bscscan(addr, args.bscscan_key, http_rl)
            if first_ts is None:
                first_ts = first_tx_ts_rpc_heuristic(w3, addr, latest, args.young_days + 30, rpc_rl)
        else:
            first_ts = first_tx_ts_rpc_heuristic(w3, addr, latest, args.young_days + 30, rpc_rl)

        if first_ts is not None and first_ts <= young_cutoff_ts:
            skipped_old += 1
            if args.verbose and not args.quiet:
                rwarn(f"Scarto vecchio: {addr} (first_ts={first_ts})")
            continue

        bal_raw = get_wallet_balance(w3, args.token, addr, rpc_rl)
        bal_tok = float(bal_raw) / (10 ** dec)
        bal_usd = bal_tok * float(price_usd or 0.0)

        results.append({
            "address": addr,
            "balance_token": bal_tok,
            "balance_usd": bal_usd,
            "first_tx_ts": first_ts,
            "sum_buys_usd_window": sum_usd,
        })
        kept += 1
        if args.verbose and not args.quiet:
            rlog(f"KEEP: {addr} | bal={bal_tok:.6f} {sym} (~{bal_usd:.2f}$) | first_ts={first_ts} | buys_window={sum_usd:.2f}$")

    # Ordina per balance_usd desc
    results.sort(key=lambda r: r["balance_usd"], reverse=True)

    # Riepilogo visivo
    final_summary({
        "Log letti": len(logs),
        "Swap con BUY lato target": buys_checked,
        "Buy >= soglia USD": len(qualifying_tx),
        "Wallet unici (>= soglia)": len(buyers),
        "Scartati (contract)": skipped_contract,
        "Scartati (vecchi)": skipped_old,
        "Wallet tenuti": kept,
    })

    # Output JSON finale (sempre)
    print(json.dumps({
        "token": args.token,
        "pair": pair_addr,
        "symbol": sym,
        "decimals": dec,
        "price_usd": price_usd,
        "window_minutes": args.minutes,
        "min_usd": args.min_usd,
        "young_days": args.young_days,
        "results": results,
    }, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
