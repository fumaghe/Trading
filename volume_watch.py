#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Volume Watch — run consigliato ogni 5 minuti
- Calcola Vol(30m) esatto sommando i campioni DexScreener `volume.m5` persistiti su disco.
- Calcola Δ% tra ultimi 30' e 30' precedenti (in %) e stampa un unico messaggio tabellare.
- Fallback: se `m5` non è disponibile/insufficiente, mostra ~Vol30m ≈ h1/2 e Δ% n.d.
- Output HTML tabellare unico: state/telegram/msg_volwatch.html (invio Telegram opzionale, singolo messaggio)
- Stato per pair: state/volwatch/{pair}.json  (mantiene una history di campioni m5 ~75')
- Salva e riusa la lista TOP in: state/top_pairs.json (fallback se test4.py fallisce)

In parallelo (opzionale):
- Scanner CEX listings (PERPS/SPOT) su Binance e Bybit, segnala le nuove coin dall'ultimo run:
  salva stato in state/cex_listings.json e appende la sezione al messaggio HTML.
"""

from __future__ import annotations
import argparse
import json
import os
import re
import subprocess
import sys
import time
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from html import escape as htmlesc

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
)
HEADERS = {"Accept": "application/json", "User-Agent": UA}
HTTP_TIMEOUT = 30

# Chain fissa BSC come nello script originale
DEX_PAIRS_URL = "https://api.dexscreener.com/latest/dex/pairs/bsc/{pair}"
TOP_HEADER = "— Tutti e 3 i parametri (TOP) —"
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

SCRIPT_DIR = Path(__file__).resolve().parent

# Finestra e retention per il buffer m5
WIN_30M_SEC = 30 * 60
RETENTION_SEC = 75 * 60  # ~1h15 per sicurezza

# ---- Endpoints CEX (per lo scanner opzionale) ----
BINANCE_FUTURES_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_SPOT_INFO    = "https://api.binance.com/api/v3/exchangeInfo"
BYBIT_INSTRUMENTS    = "https://api.bybit.com/v5/market/instruments-info"

# ---------- Utils ---------- #

def ensure_dir(p: str):
    Path(p).mkdir(parents=True, exist_ok=True)

def format_usd_int(x: Any) -> str:
    try:
        return f"{int(round(float(x))):,}$".replace(",", ".")
    except Exception:
        return "n.d."

def format_pct_signed(x: Optional[float]) -> str:
    if x is None:
        return "n.d."
    sign = "+" if x > 0 else ""
    try:
        return f"{sign}{float(x):.2f}%"
    except Exception:
        return "n.d."

def now_iso_local() -> str:
    # timestamp locale (il runner ha TZ=Europe/Rome da workflow)
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M")

# --- Telegram (singolo messaggio) ---

def send_telegram_single(html_text: str,
                         token: str,
                         chat_id: str,
                         parse_mode: str = "HTML") -> bool:
    api = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        r = requests.post(api, data={
            "chat_id": chat_id,
            "text": html_text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }, timeout=30)
        r.raise_for_status()
        return bool((r.json() or {}).get("ok"))
    except Exception as e:
        try:
            print(f"[Telegram send error] {getattr(e, 'response', None) and e.response.text}", file=sys.stderr)
        except Exception:
            pass
        return False

# ---------- Screener bridge (opzionale) ---------- #

def run_test4_and_capture(python_bin: str, min_liq: int, workers: int,
                          dominance: float, max_tickers_scan: int,
                          rps_cg: float, rps_ds: float, funnel_show: int,
                          skip_unchanged_days: int) -> str:
    test4_path = SCRIPT_DIR / "test4.py"
    cmd = [
        python_bin, str(test4_path),
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

def parse_top_only(stdout: str) -> List[Dict[str, Any]]:
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
            ([A-Z0-9._-]+)\s*[·•\-]\s*       # symbol (più permissivo)
            (.*?)\s*[·•\-]\s*
            LP:\s*([\d\.\$]+)\s*[·•\-]\s*
            Pool:\s*(0x[a-fA-F0-9]{40})\s*$""",
            line, flags=re.X,
        )
        if m:
            symbol = m.group(1).upper()
            name = m.group(2).strip()
            pair = m.group(4).lower()
            entries.append({"symbol": symbol, "name": name, "pair": pair})
    return entries

# ---------- DexScreener ---------- #

def fetch_pair(pair: str) -> Optional[Dict[str, Any]]:
    try:
        url = DEX_PAIRS_URL.format(pair=pair)
        r = requests.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        j = r.json()
        p = (j.get("pairs") or [None])[0] or None
        return p
    except Exception:
        return None

def extract_volumes(p: Dict[str, Any]) -> Dict[str, Optional[float]]:
    vol = (p.get("volume") or {}) if p else {}
    def _f(x):
        try:
            return float(x) if x is not None else None
        except Exception:
            return None
    h1  = _f(vol.get("h1"))
    h6  = _f(vol.get("h6"))
    h24 = _f(vol.get("h24"))
    m5  = _f(vol.get("m5"))  # può non essere presente su alcune pair
    if h1 is None:
        if h6 is not None:
            h1 = h6 / 6.0
        elif h24 is not None:
            h1 = h24 / 24.0
    return {"m5": m5, "h1": h1, "h6": h6, "h24": h24}

# ---------- State (m5 rolling buffer) ---------- #

def load_prev_state(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

def save_state(path: Path, data: Dict[str, Any]):
    ensure_dir(str(path.parent))
    try:
        path.write_text(json.dumps(data), encoding="utf-8")
    except Exception:
        pass

def trim_history(hist: List[Dict[str, Any]], now_ts: int) -> List[Dict[str, Any]]:
    out = []
    for it in hist or []:
        try:
            ts = int(it.get("ts", 0))
            v = float(it.get("vol_m5"))
            if v >= 0 and now_ts - ts <= RETENTION_SEC:
                out.append({"ts": ts, "vol_m5": v})
        except Exception:
            continue
    out.sort(key=lambda x: x["ts"])
    return out

def sum_window(hist: List[Dict[str, Any]], start_ts: int, end_ts: int) -> Optional[float]:
    """Somma i campioni m5 con timestamp in (start_ts, end_ts]. (half-open per evitare doppio conteggio)"""
    if start_ts >= end_ts:
        return 0.0
    vals = [h["vol_m5"] for h in hist if start_ts < h["ts"] <= end_ts]
    if not vals:
        return None
    return float(sum(vals))

def count_window(hist: List[Dict[str, Any]], start_ts: int, end_ts: int) -> int:
    """Conta campioni m5 in (start_ts, end_ts]."""
    return sum(1 for h in hist if start_ts < h["ts"] <= end_ts)

def calc_vol30m_from_m5(prev_hist: List[Dict[str, Any]], now_ts: int, vol_m5: Optional[float]):
    """
    Aggiorna la history con il campione corrente e calcola:
    - vol30_now: somma m5 negli ultimi 30 minuti
    - vol30_prev: somma m5 nei 30 minuti precedenti
    - cov_count: n° campioni negli ultimi 30' (copertura)
    """
    hist = trim_history(prev_hist or [], now_ts)
    if vol_m5 is not None:
        hist.append({"ts": now_ts, "vol_m5": float(vol_m5)})

    start_now = now_ts - WIN_30M_SEC
    end_now   = now_ts
    start_prev = now_ts - 2*WIN_30M_SEC
    end_prev   = now_ts - WIN_30M_SEC

    vol30_now  = sum_window(hist, start_now, end_now)
    vol30_prev = sum_window(hist, start_prev, end_prev)
    cov_count  = count_window(hist, start_now, end_now)  # 6 = copertura piena

    return hist, vol30_now, vol30_prev, cov_count

# ---------- Message (tabellare unico) ---------- #

def build_table_message(rows: List[Dict[str, Any]], note: str = "", extra_sections_html: str = "") -> str:
    """
    rows: list di dict con chiavi:
      symbol, name, pair, vol_h1, vol30m, vol30m_prev, delta30_pct
    """
    # Ordina per Δ% decrescente (None va in coda)
    rows.sort(key=lambda r: (r.get("delta30_pct") if r.get("delta30_pct") is not None else -1e18), reverse=True)

    header = []
    header.append("⏱️ <b>VOLUME WATCH</b> — Vol(30m) esatto • Δ% ultimi 30’ vs 30’ precedenti")
    header.append(f"<i>Aggiornato:</i> {htmlesc(now_iso_local())}")
    if note:
        header.append(htmlesc(note))
    header.append("")

    # Tabella compatta: #, SYMBOL, Now30m, Δ%
    pre = []
    pre.append(f"{'#':>2}  {'SYMBOL':8} {'Now30m':>12} {'Δ%':>9}")
    pre.append("-"*40)
    for i, r in enumerate(rows, start=1):
        now30 = r.get("vol30m")
        vol1h = r.get("vol_h1")
        now_is_est = bool(r.get("now_is_estimate"))
        now30_s = (
            ("~" + format_usd_int(now30)) if (now30 is not None and now_is_est)
            else (format_usd_int(now30) if now30 is not None
                  else ("~" + format_usd_int((vol1h or 0)/2.0) if vol1h is not None else "n.d."))
        )
        dpct_s = format_pct_signed(r.get("delta30_pct")) if now30 is not None else "n.d."
        sym = (r.get("symbol") or "")[:8]
        pre.append(f"{i:>2}. {sym:<8} {now30_s:>12} {dpct_s:>9}")

    parts = []
    parts.append("\n".join(header))
    parts.append("<pre>" + "\n".join(pre) + "</pre>")

    if extra_sections_html:
        parts.append(extra_sections_html.strip())

    return "\n\n".join(parts)

# ---------- CEX scanner (NEW LISTINGS) ---------- #

def _binance_perp_bases() -> Dict[str, Dict[str, Any]]:
    try:
        r = requests.get(BINANCE_FUTURES_INFO, timeout=HTTP_TIMEOUT, headers=HEADERS)
        r.raise_for_status()
        data = r.json() or {}
        out = {}
        for s in data.get("symbols", []):
            if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING":
                base = (s.get("baseAsset") or "").upper()
                sym  = (s.get("symbol") or "").upper()
                if base:
                    out[base] = {"symbol": sym}
        return out
    except Exception:
        return {}

def _binance_spot_bases() -> Dict[str, Dict[str, Any]]:
    try:
        r = requests.get(BINANCE_SPOT_INFO, timeout=HTTP_TIMEOUT, headers=HEADERS)
        r.raise_for_status()
        data = r.json() or {}
        out = {}
        for s in data.get("symbols", []):
            if s.get("status") == "TRADING":
                base = (s.get("baseAsset") or "").upper()
                sym  = (s.get("symbol") or "").upper()
                if base:
                    out.setdefault(base, {"symbols": set()})
                    out[base]["symbols"].add(sym)
        # normalizza set -> list
        for k in list(out.keys()):
            out[k]["symbols"] = sorted(list(out[k]["symbols"]))
        return out
    except Exception:
        return {}

def _bybit_linear_bases() -> Dict[str, Dict[str, Any]]:
    out = {}
    try:
        cursor = None
        while True:
            params = {"category": "linear", "limit": 1000}
            if cursor: params["cursor"] = cursor
            r = requests.get(BYBIT_INSTRUMENTS, params=params, timeout=HTTP_TIMEOUT, headers=HEADERS)
            r.raise_for_status()
            data = r.json() or {}
            lst = (data.get("result") or {}).get("list") or []
            for s in lst:
                if s.get("status") == "Trading" and s.get("contractType") in ("LinearPerpetual", "LinearFutures"):
                    symbol = (s.get("symbol") or "").upper()      # es. BTCUSDT
                    base = re.sub(r"(USDT|USDC)$", "", symbol)
                    if base:
                        out[base] = {"symbol": symbol}
            cursor = (data.get("result") or {}).get("nextPageCursor")
            if not cursor:
                break
    except Exception:
        return out
    return out

def _bybit_spot_bases() -> Dict[str, Dict[str, Any]]:
    out = {}
    try:
        cursor = None
        while True:
            params = {"category": "spot", "limit": 1000}
            if cursor: params["cursor"] = cursor
            r = requests.get(BYBIT_INSTRUMENTS, params=params, timeout=HTTP_TIMEOUT, headers=HEADERS)
            r.raise_for_status()
            data = r.json() or {}
            lst = (data.get("result") or {}).get("list") or []
            for s in lst:
                if s.get("status") == "Trading":
                    symbol = (s.get("symbol") or "").upper()  # es. BTCUSDT
                    base = re.sub(r"(USDT|USDC)$", "", symbol)
                    if base:
                        out.setdefault(base, {"symbols": set()})
                        out[base]["symbols"].add(symbol)
            cursor = (data.get("result") or {}).get("nextPageCursor")
            if not cursor:
                break
        for k in list(out.keys()):
            out[k]["symbols"] = sorted(list(out[k]["symbols"]))
    except Exception:
        return out
    return out

def _load_cex_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"binance": {"perps": {"bases": [], "first_seen": {}},
                            "spot":  {"bases": [], "first_seen": {}}},
                "bybit":   {"perps": {"bases": [], "first_seen": {}},
                            "spot":  {"bases": [], "first_seen": {}}}}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"binance": {"perps": {"bases": [], "first_seen": {}},
                            "spot":  {"bases": [], "first_seen": {}}},
                "bybit":   {"perps": {"bases": [], "first_seen": {}},
                            "spot":  {"bases": [], "first_seen": {}}}}

def _save_cex_state(path: Path, state: Dict[str, Any]):
    ensure_dir(str(path.parent))
    try:
        path.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass

def scan_cex_new_listings(markets: str = "perps") -> Tuple[str, int]:
    """
    markets: 'perps' | 'spot' | 'both'
    Ritorna (html_fragment, new_count)
    """
    state_path = Path("state/cex_listings.json")
    state = _load_cex_state(state_path)

    want_perps = markets in ("perps", "both")
    want_spot  = markets in ("spot", "both")

    new_items: List[str] = []
    now_iso = now_iso_local()

    # --- PERPS ---
    if want_perps:
        # Binance
        cur_bnz = _binance_perp_bases()
        prev_bnz_bases = set(state["binance"]["perps"]["bases"])
        cur_bnz_bases  = set(cur_bnz.keys())
        add_bnz = sorted(list(cur_bnz_bases - prev_bnz_bases))
        for base in add_bnz:
            state["binance"]["perps"]["first_seen"][base] = now_iso
            sym = cur_bnz.get(base, {}).get("symbol", "")
            new_items.append(f"• Binance PERPS: <b>{base}</b> ({htmlesc(sym)})")

        state["binance"]["perps"]["bases"] = sorted(list(cur_bnz_bases))

        # Bybit
        cur_byb = _bybit_linear_bases()
        prev_byb_bases = set(state["bybit"]["perps"]["bases"])
        cur_byb_bases  = set(cur_byb.keys())
        add_byb = sorted(list(cur_byb_bases - prev_byb_bases))
        for base in add_byb:
            state["bybit"]["perps"]["first_seen"][base] = now_iso
            sym = cur_byb.get(base, {}).get("symbol", "")
            new_items.append(f"• Bybit PERPS: <b>{base}</b> ({htmlesc(sym)})")

        state["bybit"]["perps"]["bases"] = sorted(list(cur_byb_bases))

    # --- SPOT ---
    if want_spot:
        # Binance
        cur_bnz = _binance_spot_bases()
        prev_bnz_bases = set(state["binance"]["spot"]["bases"])
        cur_bnz_bases  = set(cur_bnz.keys())
        add_bnz = sorted(list(cur_bnz_bases - prev_bnz_bases))
        for base in add_bnz:
            state["binance"]["spot"]["first_seen"][base] = now_iso
            syms = ", ".join(cur_bnz.get(base, {}).get("symbols", [])[:3])
            more = "" if len(cur_bnz.get(base, {}).get("symbols", [])) <= 3 else "…"
            new_items.append(f"• Binance SPOT: <b>{base}</b> ({htmlesc(syms)}{more})")
        state["binance"]["spot"]["bases"] = sorted(list(cur_bnz_bases))

        # Bybit
        cur_byb = _bybit_spot_bases()
        prev_byb_bases = set(state["bybit"]["spot"]["bases"])
        cur_byb_bases  = set(cur_byb.keys())
        add_byb = sorted(list(cur_byb_bases - prev_byb_bases))
        for base in add_byb:
            state["bybit"]["spot"]["first_seen"][base] = now_iso
            syms = ", ".join(cur_byb.get(base, {}).get("symbols", [])[:3])
            more = "" if len(cur_byb.get(base, {}).get("symbols", [])) <= 3 else "…"
            new_items.append(f"• Bybit SPOT: <b>{base}</b> ({htmlesc(syms)}{more})")
        state["bybit"]["spot"]["bases"] = sorted(list(cur_byb_bases))

    # Salva stato
    _save_cex_state(state_path, state)

    if not new_items:
        return "", 0

    section = []
    section.append("<hr>")
    section.append("🆕 <b>Nuove listing CEX</b> (rilevate nel run corrente)")
    section.append("<ul>")
    for it in new_items:
        section.append(f"<li>{it}</li>")
    section.append("</ul>")
    return "\n".join(section), len(new_items)

# ---------- Main ---------- #

def main():
    ap = argparse.ArgumentParser(description="Volume watcher — Vol(30m) esatto da m5 + Δ% vs 30’ precedenti (tabellare) + CEX listings (opz.)")
    ap.add_argument("--mode", choices=["from-screener", "from-file"], default="from-screener",
                    help="Origine lista pair: 'from-screener' rilancia test4.py, 'from-file' legge JSON")
    ap.add_argument("--pairs-file", type=str, default="state/top_pairs.json",
                    help="File JSON con [{'symbol','name','pair'}...]")
    # Parametri per test4.py quando mode=from-screener
    ap.add_argument("--python-bin", default=sys.executable)
    ap.add_argument("--min-liq", type=int, default=200000)
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--dominance", type=float, default=0.30)
    ap.add_argument("--max-tickers-scan", type=int, default=40)
    ap.add_argument("--rps-cg", type=float, default=0.5)
    ap.add_argument("--rps-ds", type=float, default=2.0)
    ap.add_argument("--funnel-show", type=int, default=100)
    ap.add_argument("--skip-unchanged-days", type=int, default=0)
    # Filtro/qualità calcolo Vol(30m) e Δ%
    ap.add_argument("--min-cov", type=int, default=0,
                    help="Min # di campioni m5 richiesti negli ultimi 30' (0..6). Se non soddisfatto: usa ~h1/2 e Δ% n.d.")
    ap.add_argument("--threshold-x", type=float, default=0.0,
                    help="Se > 0, mostra solo le pair con |Δ%| >= soglia (in punti percentuali). 0 = disabilitato.")
    ap.add_argument("--delta-mode", choices=["strict", "approx"], default="approx",
                    help="Calcolo Δ%: strict=solo m5 completi; approx=stima con scaling/h1")
    # Scanner CEX (NEW LISTINGS)
    ap.add_argument("--cex-watch", action="store_true",
                    help="Se presente, esegue scanner CEX per nuove listing (Binance/Bybit).")
    ap.add_argument("--cex-markets", choices=["perps", "spot", "both"], default="perps",
                    help="Mercati da scansionare per nuove listing CEX.")
    # Invio Telegram
    ap.add_argument("--send-telegram", action="store_true", help="Se presente, invia il messaggio a Telegram (singolo)")
    ap.add_argument("--tg-parse-mode", default="HTML",
                    choices=["HTML", "MarkdownV2", "Markdown"], help="Parse mode Telegram")
    ap.add_argument("--tg-token", default=None, help="Override TG_BOT_TOKEN (altrimenti legge da env)")
    ap.add_argument("--tg-chat", default=None, help="Override TG_CHAT_ID (altrimenti legge da env)")
    args = ap.parse_args()

    # Sanity bounds
    min_cov = max(0, min(int(args.min_cov), 6))  # 0..6
    threshold_x = max(0.0, float(args.threshold_x))

    # --- Avvio scanner CEX in parallelo (se abilitato) ---
    extra_section_holder = {"html": "", "n": 0}
    cex_thread = None
    if args.cex_watch:
        def _run_cex():
            try:
                html, n = scan_cex_new_listings(args.cex_markets)
                extra_section_holder["html"] = html
                extra_section_holder["n"] = n
            except Exception as e:
                # Non bloccare il run principale
                print(f"[CEX scan error] {e}", file=sys.stderr)
        cex_thread = threading.Thread(target=_run_cex, daemon=True)
        cex_thread.start()

    # 1) Carica lista pair
    pairs: List[Dict[str, Any]] = []
    if args.mode == "from-file":
        try:
            pairs_path = Path(args.pairs_file)
            data = json.loads(pairs_path.read_text(encoding="utf-8"))
        except Exception:
            data = []
        pairs = [{"symbol": d.get("symbol"), "name": d.get("name"), "pair": d.get("pair")} for d in (data or [])]
    else:
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
            pairs = parse_top_only(stdout)
            # Salva la lista TOP per fallback
            ensure_dir("state")
            Path("state/top_pairs.json").write_text(json.dumps(pairs, ensure_ascii=False), encoding="utf-8")
        except subprocess.CalledProcessError as e:
            print("ERROR: test4.py failed", file=sys.stderr)
            try:
                print(e.stdout); print(e.stderr, file=sys.stderr)
            except Exception:
                pass
            # Fallback al file salvato
            try:
                data = json.loads(Path(args.pairs_file).read_text(encoding="utf-8"))
                pairs = [{"symbol": d.get("symbol"), "name": d.get("name"), "pair": d.get("pair")} for d in (data or [])]
                print(f"Fallback: caricata lista TOP da {args.pairs_file} ({len(pairs)} pairs).", file=sys.stderr)
            except Exception:
                pairs = []

    if not pairs:
        # Attendi comunque il thread CEX, così se ci sono nuove listing le vedi lo stesso
        if cex_thread: cex_thread.join(timeout=10)
        extra_html = extra_section_holder["html"]
        msg = "⚠️ Nessuna pair TOP da monitorare."
        if extra_html:
            msg = msg + "\n\n" + extra_html
        ensure_dir("state/telegram")
        Path("state/telegram/msg_volwatch.html").write_text(msg, encoding="utf-8")
        print(msg)
        return

    # 2) Per-pair: fetch, calcola Vol(30m) da m5, Δ% vs prev, salva stato
    rows: List[Dict[str, Any]] = []
    state_dir = Path("state/volwatch")
    ensure_dir(str(state_dir))
    now_ts = int(time.time())

    for pinfo in pairs:
        sym = pinfo["symbol"]; name = pinfo["name"]; pair = pinfo["pair"]
        data = fetch_pair(pair) or {}
        vols = extract_volumes(data)
        vol_h1 = vols.get("h1")
        vol_m5 = vols.get("m5")

        state_path = state_dir / f"{pair}.json"
        prev = load_prev_state(state_path) or {}
        prev_hist = prev.get("m5_history") or []

        hist, vol30_now, vol30_prev, cov_count = calc_vol30m_from_m5(prev_hist, now_ts, vol_m5)

        # approx strict handling
        delta30_pct = None
        if cov_count < min_cov:
            vol30_now = None
            vol30_prev = None
            delta30_pct = None
        else:
            if vol30_now is not None and vol30_prev is not None:
                try:
                    delta_abs = float(vol30_now) - float(vol30_prev)
                    if vol30_prev != 0:
                        delta30_pct = (delta_abs / float(vol30_prev)) * 100.0
                except Exception:
                    delta30_pct = None

        save_state(state_path, {
            "ts": now_ts, "vol_h1": vol_h1, "m5_history": hist,
        })

        rows.append({
            "symbol": sym,
            "name": name,
            "pair": pair,
            "vol_h1": vol_h1,
            "vol30m": vol30_now,        # ultimi 30'
            "vol30m_prev": vol30_prev,  # 30' precedenti
            "delta30_pct": delta30_pct, # differenza in %
            "cov_count": cov_count,
            "now_is_estimate": False,
        })

    # filtro Δ%
    rows_for_table = rows
    if threshold_x > 0:
        filtered = [r for r in rows if (r.get("delta30_pct") is not None and abs(r["delta30_pct"]) >= threshold_x)]
        if filtered:
            rows_for_table = filtered

    # Attendi lo scanner CEX, ma non più di 10s (non deve bloccare il run)
    if cex_thread:
        cex_thread.join(timeout=10)
    extra_html = extra_section_holder["html"]

    # Nota header
    note_bits = []
    if threshold_x > 0: note_bits.append(f"Filtro: |Δ%| ≥ {threshold_x:.2f}")
    if min_cov > 0:     note_bits.append(f"Min copertura m5 ≥ {min_cov}/6 campioni")
    if args.cex_watch:  note_bits.append(f"CEX scan: {args.cex_markets}")
    note = " • ".join(note_bits)

    # 3) Costruisci messaggio HTML tabellare e salva
    ensure_dir("state/telegram")
    html = build_table_message(rows_for_table, note=note, extra_sections_html=extra_html)
    out_path = Path("state/telegram/msg_volwatch.html")
    out_path.write_text(html, encoding="utf-8")
    print(f"OK: scritto {out_path} ({len(html)} chars)")

    # 4) Invio Telegram (opzionale, singolo)
    if args.send_telegram:
        token = args.tg_token or os.environ.get("TG_BOT_TOKEN", "")
        chat  = args.tg_chat  or os.environ.get("TG_CHAT_ID", "")
        if not token or not chat:
            print("Telegram disabled: TG_BOT_TOKEN/TG_CHAT_ID mancanti.", file=sys.stderr)
        else:
            ok = send_telegram_single(html, token, chat, parse_mode=args.tg_parse_mode)
            print(f"Telegram send: {'OK' if ok else 'FAIL'}")

if __name__ == "__main__":
    main()
