#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DeFi Quant Tool: token1 needed to push a V3 pool price upward (+30%, +50%, +100%)
Chain: BSC (compatible with PancakeSwap v3 / Uniswap v3 pools)

What it does
------------
- Reads on-chain data (slot0, tickSpacing, liquidity, token0/1 metadata, tickBitmap, ticks)
- Computes the current price (token1 per 1 token0)
- Finds the initialized ticks in the path up to the target ticks for +30%, +50%, +100%
- Integrates active liquidity segment-by-segment to estimate token1 needed:
  amount1 = L * (sqrtB - sqrtA) / Q96, with L constant between initialized ticks
- Outputs a summary table and (optionally) a step plot of liquidity vs price with markers

Usage
-----
python defiFunzionante.py --pool 0xYourPool --rpc https://bsc-dataseed.binance.org --plot

Notes
-----
- This script connects directly to the chain via RPC (same data you'd read from BscScan's "Read Contract").
- It only handles *upward* price moves (buying token0 with token1). For downward moves the math is symmetric.
- Precision: uses Python Decimal with high precision for exponentials near current tick.
- If long spans have L=0, you'll see a warning: expect jumps and lower confidence.

MIT License (c) 2025
"""

from decimal import Decimal, getcontext
from dataclasses import dataclass
from typing import List, Tuple, Dict, Optional
import argparse
import math
import os
from web3 import Web3
import pandas as pd
try:
    from web3.middleware import ExtraDataToPOAMiddleware  # web3.py v6
except ImportError:
    ExtraDataToPOAMiddleware = None
try:
    from web3.middleware import geth_poa_middleware        # web3.py v5
except ImportError:
    geth_poa_middleware = None
try:
    import matplotlib.pyplot as plt
    _HAS_MPL = True
except Exception:
    _HAS_MPL = False

# High precision for decimal exponentials
getcontext().prec = 80

Q96 = 2 ** 96
def _inject_poa(w3: Web3) -> None:
    """
    Injects the correct PoA middleware for BSC depending on web3.py version.
    Must be called right after creating Web3(provider).
    """
    # web3.py v6+
    if ExtraDataToPOAMiddleware is not None:
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        return
    # web3.py v5.x
    if geth_poa_middleware is not None:
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        return

UNISWAP_V3_POOL_MINI_ABI = [
    {
        "inputs": [],
        "name": "slot0",
        "outputs": [
            {"internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160"},
            {"internalType": "int24",   "name": "tick", "type": "int24"},
            {"internalType": "uint16",  "name": "observationIndex", "type": "uint16"},
            {"internalType": "uint16",  "name": "observationCardinality", "type": "uint16"},
            {"internalType": "uint16",  "name": "observationCardinalityNext", "type": "uint16"},
            {"internalType": "uint32",  "name": "feeProtocol", "type": "uint32"},  # <<<<< fix
            {"internalType": "bool",    "name": "unlocked", "type": "bool"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "tickSpacing",
        "outputs": [{"internalType": "int24", "name": "", "type": "int24"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "int16", "name": "wordPosition", "type": "int16"}],
        "name": "tickBitmap",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "int24", "name": "tick", "type": "int24"}],
        "name": "ticks",
        "outputs": [
            {"internalType": "uint128", "name": "liquidityGross", "type": "uint128"},
            {"internalType": "int128", "name": "liquidityNet", "type": "int128"},
            {"internalType": "uint256", "name": "feeGrowthOutside0X128", "type": "uint256"},
            {"internalType": "uint256", "name": "feeGrowthOutside1X128", "type": "uint256"},
            {"internalType": "int56", "name": "tickCumulativeOutside", "type": "int56"},
            {"internalType": "uint160", "name": "secondsPerLiquidityOutsideX128", "type": "uint160"},
            {"internalType": "uint32", "name": "secondsOutside", "type": "uint32"},
            {"internalType": "bool", "name": "initialized", "type": "bool"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "liquidity",
        "outputs": [{"internalType": "uint128", "name": "", "type": "uint128"}],
        "stateMutability": "view",
        "type": "function",
    },
    {"inputs": [], "name": "token0", "outputs": [{"internalType": "address", "name": "", "type": "address"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "token1", "outputs": [{"internalType": "address", "name": "", "type": "address"}], "stateMutability": "view", "type": "function"},
]

ERC20_MINI_ABI = [
    {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "payable": False, "stateMutability": "view", "type": "function"},
    {"constant": True, "inputs": [], "name": "symbol",   "outputs": [{"name": "", "type": "string"}], "payable": False, "stateMutability": "view", "type": "function"},
]

@dataclass
class TokenMeta:
    address: str
    decimals: int
    symbol: str

@dataclass
class PoolSnapshot:
    sqrtPriceX96: int
    tick: int
    tickSpacing: int
    liquidity: int
    token0: TokenMeta
    token1: TokenMeta
    block_number: int
    block_timestamp: int

def floor_div_neg(a: int, b: int) -> int:
    # Floor division toward -infinity (Python's // already does this for ints)
    return a // b

def price_from_sqrtX96(sqrtX96: int, dec0: int, dec1: int) -> Decimal:
    sp = Decimal(sqrtX96) / Decimal(Q96)
    price = sp * sp * (Decimal(10) ** Decimal(dec1 - dec0))
    return price

def tick_from_price(price: Decimal, dec0: int, dec1: int) -> int:
    # t(P) = floor( ln( P * 10^(dec0-dec1) ) / ln(1.0001) )
    base = Decimal('1.0001')
    val = price * (Decimal(10) ** Decimal(dec0 - dec1))
    t = (val.ln() / base.ln())
    return math.floor(t)

def sqrt_unscaled_from_tick_delta(sqrt_curr_unscaled: Decimal, delta_tick: int) -> Decimal:
    # sqrt(P_target) = sqrt(P_curr) * (1.0001)^(delta_tick/2)
    base = Decimal('1.0001')
    exponent = Decimal(delta_tick) / Decimal(2)
    return sqrt_curr_unscaled * (base ** exponent)

def sqrtX96_from_unscaled(sqrt_unscaled: Decimal) -> int:
    return int((sqrt_unscaled * Decimal(Q96)).to_integral_value(rounding="ROUND_FLOOR"))

def scan_initialized_ticks(w3: Web3, pool, t0: int, t_target: int, spacing: int) -> List[int]:
    # Determine compressed ticks and word range (inclusive)
    c0 = floor_div_neg(t0, spacing)
    cT = floor_div_neg(t_target, spacing)
    w0 = floor_div_neg(c0, 256)
    wT = floor_div_neg(cT, 256)
    in_range: List[int] = []
    direction = 1 if t_target >= t0 else -1

    if direction >= 0:
        word_range = range(w0, wT + 1)
    else:
        word_range = range(w0, wT - 1, -1)

    for w in word_range:
        bitmap = pool.functions.tickBitmap(w).call()
        if bitmap == 0:
            continue
        # scan bits 0..255
        for b in range(256):
            if (bitmap >> b) & 1 == 1:
                c_i = w * 256 + b
                tick_i = c_i * spacing
                # keep only ticks strictly between t0 and t_target (inclusive of target boundary if exact multiple)
                if direction >= 0:
                    if tick_i > t0 and tick_i <= t_target:
                        in_range.append(tick_i)
                else:
                    if tick_i <= t0 and tick_i > t_target:
                        in_range.append(tick_i)

    in_range = sorted(in_range) if direction >= 0 else sorted(in_range, reverse=True)
    return in_range

def fetch_liquidity_nets(w3: Web3, pool, tick_indices: List[int]) -> Dict[int, int]:
    liq_nets: Dict[int, int] = {}
    for ti in tick_indices:
        data = pool.functions.ticks(ti).call()
        liquidityNet = int(data[1])
        liq_nets[ti] = liquidityNet
    return liq_nets

def integrate_token1_to_target(
    snapshot: PoolSnapshot,
    w3: Web3,
    pool,
    tick_target: int,
    do_collect_segments: bool = True,
) -> Tuple[Decimal, int, List[Tuple[int, int, int]], List[int]]:
    """
    Return:
      - token1_needed (in *human* units, i.e., divided by 10^dec1) as Decimal
      - segments_count
      - segments_detail: list of tuples (tick_from, tick_to, L_used) for each integrated segment
      - zero_L_segments_ticks: list of starting ticks where L==0 occurred
    """
    t0 = snapshot.tick
    spacing = snapshot.tickSpacing
    direction_up = tick_target >= t0
    if not direction_up:
        raise ValueError("This helper only handles upward moves (price increase).")

    # find initialized ticks between (t0, tick_target]
    ticks_in_range = scan_initialized_ticks(w3, pool, t0, tick_target, spacing)
    liq_nets = fetch_liquidity_nets(w3, pool, ticks_in_range)

    sqrt_curr_unscaled = Decimal(snapshot.sqrtPriceX96) / Decimal(Q96)

    # Prepare the ordered boundary ticks to traverse (initialized ones + final target marker as a pseudo-tick)
    boundary_ticks = ticks_in_range.copy()

    # Track liquidity across crossings
    L = int(snapshot.liquidity)

    token1_raw_total = 0  # in raw token1 units (no decimals)
    segments_detail: List[Tuple[int, int, int]] = []  # (tick_from, tick_to, L_used)
    zero_L_segments: List[int] = []

    # Build the sequence of "stops": initialized ticks, then final target
    all_stops: List[Tuple[str, int]] = [("tick", ti) for ti in boundary_ticks] + [("target", tick_target)]

    prev_sqrtX96 = snapshot.sqrtPriceX96
    prev_tick_marker = t0

    for kind, stop_tick in all_stops:
        # compute sqrt at this stop (tick boundary or final target)
        delta_tick = stop_tick - t0
        sqrt_stop_unscaled = sqrt_unscaled_from_tick_delta(sqrt_curr_unscaled, delta_tick)
        sqrt_stop_X96 = sqrtX96_from_unscaled(sqrt_stop_unscaled)

        # amount for segment [prev_sqrt, sqrt_stop)
        if L == 0:
            zero_L_segments.append(prev_tick_marker)

        delta_sqrtX96 = sqrt_stop_X96 - prev_sqrtX96
        if delta_sqrtX96 < 0:
            raise RuntimeError("Unexpected negative delta_sqrtX96 while moving upward.")

        # amount1 = L * (sqrtB - sqrtA) / Q96
        seg_amount1_raw = (int(L) * int(delta_sqrtX96)) // Q96
        token1_raw_total += seg_amount1_raw

        if do_collect_segments:
            segments_detail.append((prev_tick_marker, stop_tick, int(L)))

        # Cross the initialized tick (if this stop is a tick), update L
        if kind == "tick":
            L = L + int(liq_nets[stop_tick])

        # advance
        prev_sqrtX96 = sqrt_stop_X96
        prev_tick_marker = stop_tick

    token1_human = Decimal(token1_raw_total) / (Decimal(10) ** Decimal(snapshot.token1.decimals))
    return token1_human, len(segments_detail), segments_detail, zero_L_segments

ERC20_MINI_ABI_BYTES32 = [
    {"constant": True, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "bytes32"}], "payable": False, "stateMutability": "view", "type": "function"},
    {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "payable": False, "stateMutability": "view", "type": "function"},
]

def _read_token_meta(w3: Web3, addr: str) -> TokenMeta:
    c_str = w3.eth.contract(address=addr, abi=ERC20_MINI_ABI)
    c_b32 = w3.eth.contract(address=addr, abi=ERC20_MINI_ABI_BYTES32)
    decimals = int(c_str.functions.decimals().call())
    try:
        symbol = c_str.functions.symbol().call()
        if isinstance(symbol, (bytes, bytearray)):
            symbol = symbol.decode('utf-8', 'ignore').strip('\x00')
    except Exception:
        raw = c_b32.functions.symbol().call()
        symbol = raw.decode('utf-8', 'ignore').strip('\x00') if isinstance(raw, (bytes, bytearray)) else str(raw)
    return TokenMeta(address=addr, decimals=decimals, symbol=symbol or "TOKEN")


def read_pool_snapshot(w3: Web3, pool_addr: str) -> PoolSnapshot:
    pool = w3.eth.contract(address=Web3.to_checksum_address(pool_addr), abi=UNISWAP_V3_POOL_MINI_ABI)
    sqrtPriceX96, tick, *_rest = pool.functions.slot0().call()
    tickSpacing = int(pool.functions.tickSpacing().call())
    liquidity = int(pool.functions.liquidity().call())
    token0_addr = pool.functions.token0().call()
    token1_addr = pool.functions.token1().call()

    tok0_meta = _read_token_meta(w3, token0_addr)
    tok1_meta = _read_token_meta(w3, token1_addr)


    latest_block = w3.eth.get_block("latest")

    snapshot = PoolSnapshot(
        sqrtPriceX96=int(sqrtPriceX96),
        tick=int(tick),
        tickSpacing=int(tickSpacing),
        liquidity=int(liquidity),
        token0=tok0_meta,
        token1=tok1_meta,
        block_number=int(latest_block.number),
        block_timestamp=int(latest_block.timestamp),
    )
    return snapshot

def compute_targets(
    snapshot: PoolSnapshot,
    w3: Web3,
    pool,
    factors: List[Decimal] = [Decimal('1.5'), Decimal('2.0'), Decimal('3.0')],
):
    results = []
    # Current price (token1 per 1 token0)
    P0 = price_from_sqrtX96(snapshot.sqrtPriceX96, snapshot.token0.decimals, snapshot.token1.decimals)

    for f in factors:
        P_target = P0 * f
        t_target = tick_from_price(P_target, snapshot.token0.decimals, snapshot.token1.decimals)

        token1_needed, segments_count, segments_detail, zeroL = integrate_token1_to_target(
            snapshot, w3, pool, t_target, do_collect_segments=True
        )
        results.append({
            "target": f"+{int((f-1)*100)}%",
            "price_target": f"{P_target:.18f}",
            "tick_target": t_target,
            "token1_needed": f"{token1_needed:.18f} {snapshot.token1.symbol}",
            "segments": segments_count,
            "tick_min..tick_max": f"{min(snapshot.tick, t_target)}..{max(snapshot.tick, t_target)}",
            "_segments_detail": segments_detail,
            "_zeroL_starts": zeroL,
        })
    return P0, results

def plot_liquidity_distribution(
    snapshot: PoolSnapshot,
    segments_detail: List[Tuple[int, int, int]],
    targets_prices: Dict[str, Decimal],
    show: bool = True,
    save_path: Optional[str] = None,
):
    if not _HAS_MPL:
        print("matplotlib non disponibile: salta il grafico. Installa `matplotlib` per abilitarlo.")
        return

    # Build step arrays of price vs liquidity
    dec0, dec1 = snapshot.token0.decimals, snapshot.token1.decimals
    # helper to map a tick to price with decimals
    def price_at_tick(t: int) -> Decimal:
        return (Decimal('1.0001') ** Decimal(t)) * (Decimal(10) ** Decimal(dec1 - dec0))

    xs = []  # prices
    ys = []  # liquidity
    for (t_from, t_to, L_used) in segments_detail:
        p_from = price_at_tick(t_from)
        p_to = price_at_tick(t_to)
        # step: maintain L from p_from to p_to
        xs.extend([float(p_from), float(p_to)])
        ys.extend([L_used, L_used])

    import matplotlib.pyplot as plt  # safe now
    plt.figure()
    plt.step(xs, ys, where='post')
    # vertical lines at P0, P30, P50, P100
    for label, p in targets_prices.items():
        plt.axvline(float(p), linestyle='--')
        # avoid text overlap at 0 liquidity
        top = max(ys) if ys else 0
        plt.text(float(p), top, label, rotation=90, va='bottom')
    plt.xlabel(f"Price {snapshot.token1.symbol} per 1 {snapshot.token0.symbol}")
    plt.ylabel("Active Liquidity (raw units)")
    plt.title("Active Liquidity vs Price (step)")
    if save_path:
        plt.savefig(save_path, bbox_inches="tight")
    if show:
        plt.show()

def main():
    parser = argparse.ArgumentParser(description="Estimate token1 needed to push a V3 pool price upward by +30%, +50%, +100%.")
    parser.add_argument("--pool", required=True, help="Pool address (e.g., 0xaeaD6bd31dd66Eb3A6216aAF271D0E661585b0b1)")
    parser.add_argument("--rpc", default=None, help="BSC RPC URL. If omitted, uses a public default.")
    parser.add_argument("--plot", action="store_true", help="Show a simple liquidity vs price plot with target markers.")
    args = parser.parse_args()

    rpc_url = args.rpc or os.environ.get("BSC_RPC") or "https://bsc-dataseed.binance.org"
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 30}))
    _inject_poa(w3)  # <<< aggiungi SUBITO dopo aver creato Web3

    if not w3.is_connected():
        raise RuntimeError("Failed to connect to RPC. Provide a working BSC RPC via --rpc or set BSC_RPC env var.")


    pool_addr = Web3.to_checksum_address(args.pool)
    pool = w3.eth.contract(address=pool_addr, abi=UNISWAP_V3_POOL_MINI_ABI)

    snapshot = read_pool_snapshot(w3, pool_addr)

    # Compute targets
    P0, rows = compute_targets(snapshot, w3, pool)

    # Build summary DataFrame
    df = pd.DataFrame([
        {
            "target": r["target"],
            "price_target": r["price_target"],
            "tick_target": r["tick_target"],
            "token1_needed": r["token1_needed"],
            "segments": r["segments"],
            "tick_min..tick_max": r["tick_min..tick_max"],
        }
        for r in rows
    ])

    print("\n=== Pool Snapshot ===")
    print(f"Block: {snapshot.block_number} @ {snapshot.block_timestamp}")
    print(f"Pool tickSpacing: {snapshot.tickSpacing}")
    print(f"Current tick: {snapshot.tick}")
    print(f"Current sqrtPriceX96: {snapshot.sqrtPriceX96}")
    print(f"Current price ({snapshot.token1.symbol} per 1 {snapshot.token0.symbol}): {P0:.18f}")
    print(f"Liquidity (raw): {snapshot.liquidity}")
    print(f"token0: {snapshot.token0.symbol} ({snapshot.token0.address}), decimals={snapshot.token0.decimals}")
    print(f"token1: {snapshot.token1.symbol} ({snapshot.token1.address}), decimals={snapshot.token1.decimals}")

    # Warning if any segment had L==0
    any_zeroL = any(bool(r["_zeroL_starts"]) for r in rows)
    if any_zeroL:
        print("\n[ATTENZIONE] Tratti con L=0 rilevati lungo il percorso: possibile salto di prezzo e stima meno affidabile.")

    print("\n=== Risultati ===")
    print(df.to_string(index=False))

    # Optional plot: reuse segments from the widest (+100%) scenario for the curve
    if args.plot:
        targets_prices = {
            "P0": P0,
            rows[0]["target"]: Decimal(rows[0]["price_target"]),
            rows[1]["target"]: Decimal(rows[1]["price_target"]),
            rows[2]["target"]: Decimal(rows[2]["price_target"]),
        }
        segments_100 = rows[2]["_segments_detail"]
        plot_liquidity_distribution(snapshot, segments_100, targets_prices, show=True, save_path=None)

if __name__ == "__main__":
    # Example CLI:
    # python defi_price_push.py --pool 0xaeaD6bd31dd66Eb3A6216aAF271D0E661585b0b1 --rpc https://bsc-dataseed.binance.org --plot
    main()
