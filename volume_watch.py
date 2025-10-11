#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Volume Watch — ogni 30 minuti
- Prende le pool TOP (da test4.py o da un file JSON) e chiama DexScreener.
- Usa volume.h1 come metrica robusta; calcola Δ% vs 30m fa (persistito su disco).
- Mostra tabellone e, per ogni coin, un blocco con:
  • Nome cliccabile (DexScreener)
  • Pool address con link DexScreener + address in <code>
  • Token BASE e QUOTE con link BscScan, address short
  • Vol1h, ~Vol30m (≈ h1/2), Δ% con freccia
Output: state/telegram/msg_volwatch.html  (e invio Telegram opzionale)
Stato per pair: state/volwatch/{pair}.json
"""

from __future__ import annotations
import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from html import escape as htmlesc

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
)
HEADERS = {"Accept": "application/json", "User-Agent": UA}
HTTP_TIMEOUT = 30

DEX_PAIRS_URL = "https://api.dexscreener.com/latest/dex/pairs/bsc/{pair}"
TOP_HEADER = "— Tutti e 3 i parametri (TOP) —"
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

SCRIPT_DIR = Path(__file__).resolve().parent  # per path robusti

# ---------- Utils ---------- #

def ensure_dir(p: str):
    Path(p).mkdir(parents=True, exist_ok=True)

def format_usd_int(x: Any) -> str:
    try:
        return f"{int(round(float(x))):,}$".replace(",", ".")
    except Exception:
        return "n.d."

def short_addr(addr: Optional[str]) -> str:
    a = (addr or "").lower()
    if not a.startswith("0x") or len(a) < 10:
        return a or "n.a."
    return f"{a[:6]}…{a[-4:]}"

def now_iso_local() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M")

# --- Telegram helpers (chunked) ---

def chunk_by_blocks(text: str, max_chars: int = 3800, split_marker: str = "\n\n• ") -> list[str]:
    """Divide il messaggio in chunk <= max_chars cercando di spezzare tra i blocchi per-coin (prefisso '• ')."""
    if split_marker in text:
        head, rest = text.split(split_marker, 1)
        blocks = [head] + [split_marker.strip() + b for b in rest.split(split_marker)]
    else:
        blocks = [text]
    parts, buf = [], ""
    for blk in blocks:
        if not blk:
            continue
        if len(blk) > max_chars:  # taglio di sicurezza se un blocco è enorme
            if buf:
                parts.append(buf)
                buf = ""
            for i in range(0, len(blk), max_chars):
                parts.append(blk[i:i+max_chars])
            continue
        if len(buf) + len(blk) + 2 > max_chars:
            if buf:
                parts.append(buf)
            buf = blk
        else:
            buf = (buf + "\n\n" + blk) if buf else blk
    if buf:
        parts.append(buf)
    return parts

def send_telegram_chunked(html_text: str,
                          token: str,
                          chat_id: str,
                          max_chars: int = 3800,
                          sleep_s: float = 0.8,
                          parse_mode: str = "HTML") -> bool:
    api = f"https://api.telegram.org/bot{token}/sendMessage"
    parts = chunk_by_blocks(html_text, max_chars=max_chars, split_marker="\n\n• ")
    ok_all = True
    for i, p in enumerate(parts, 1):
        r = requests.post(api, data={
            "chat_id": chat_id,
            "text": p,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        })
        try:
            r.raise_for_status()
            ok = bool((r.json() or {}).get("ok"))
        except Exception:
            ok = False
        if not ok:
            ok_all = False
            try:
                print(f"[Send fail part {i}] {r.text}", file=sys.stderr)
            except Exception:
                print(f"[Send fail part {i}] HTTP {r.status_code}", file=sys.stderr)
        time.sleep(sleep_s)
    return ok_all

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
            ([A-Z0-9]+)\s*[·•\-]\s*
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
    # Proxy se manca h1
    if h1 is None:
        if h6 is not None:
            h1 = h6 / 6.0
        elif h24 is not None:
            h1 = h24 / 24.0
    return {"h1": h1, "h6": h6, "h24": h24}

# ---------- State ---------- #

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

# ---------- Message ---------- #

def build_message(rows: List[Dict[str, Any]]) -> str:
    """rows: list di dict con chiavi:
       symbol, name, pair, url, base_addr, base_sym, quote_addr, quote_sym, vol_h1, delta_pct
    """
    # Ordina per Δ% desc
    rows.sort(key=lambda r: (r["delta_pct"] if r["delta_pct"] is not None else -1e18), reverse=True)

    # Tabellone
    header = []
    header.append(f"⏱️ <b>VOLUME WATCH</b> — ΔVol(1h) vs 30m fa  •  ~Vol(30m) ≈ Vol(1h)/2")
    header.append(f"<i>Aggiornato:</i> {htmlesc(now_iso_local())}\n")

    pre = []
    pre.append(f"{'#':>2}  {'SYMBOL':8} {'Vol1h':>12} {'~Vol30m':>12} {'Δ1h vs 30m':>12}")
    pre.append("-"*56)
    for i, r in enumerate(rows, start=1):
        vol1h = format_usd_int(r["vol_h1"]) if r["vol_h1"] is not None else "n.d."
        vol30 = format_usd_int(r["vol_h1"]/2.0) if r["vol_h1"] is not None else "n.d."
        if r["delta_pct"] is None:
            dp = "    n.d."
        else:
            dp = f"{r['delta_pct']:>9.2f}%"
        sym = (r["symbol"] or "")[:8]
        pre.append(f"{i:>2}. {sym:<8} {vol1h:>12} {vol30:>12} {dp:>12}")

    # Dettagli per coin (link + address)
    blocks = []
    for r in rows:
        url = r.get("url") or f"https://dexscreener.com/bsc/{r['pair']}"
        pair = r["pair"]
        sym = r["symbol"] or "SYM"
        name = r["name"] or "—"
        base_sym = r.get("base_sym") or "BASE"
        quote_sym = r.get("quote_sym") or "QUOTE"
        base_addr = (r.get("base_addr") or "").lower()
        quote_addr = (r.get("quote_addr") or "").lower()

        # freccia per Δ
        if r["delta_pct"] is None:
            delta_label = "n.d."
            arrow = "➖"
        else:
            delta = r["delta_pct"]
            arrow = "🔺" if delta > 0 else ("🔻" if delta < 0 else "➖")
            delta_label = f"{delta:.2f}%"
        vol1h = format_usd_int(r["vol_h1"]) if r["vol_h1"] is not None else "n.d."
        vol30 = format_usd_int(r["vol_h1"]/2.0) if r["vol_h1"] is not None else "n.d."

        blocks.append(
            "\n".join([
                f"• <a href=\"{htmlesc(url)}\"><b>{htmlesc(sym)}</b> — {htmlesc(name)}</a>",
                f"  {arrow} ΔVol(1h): <b>{htmlesc(delta_label)}</b>   |   Vol1h: <b>{htmlesc(vol1h)}</b>   |   ~30m: <b>{htmlesc(vol30)}</b>",
                f"  Pool: <a href=\"https://dexscreener.com/bsc/{htmlesc(pair)}\">DexScreener</a> • "
                f"<a href=\"https://bscscan.com/address/{htmlesc(pair)}\">BscScan</a>",
                f"  <code>{htmlesc(pair)}</code>",
                f"  Tokens: "
                f"{htmlesc(base_sym)} <a href=\"https://bscscan.com/token/{htmlesc(base_addr)}\">{htmlesc(short_addr(base_addr))}</a>  /  "
                f"{htmlesc(quote_sym)} <a href=\"https://bscscan.com/token/{htmlesc(quote_addr)}\">{htmlesc(short_addr(quote_addr))}</a>",
            ])
        )

    # Assembla messaggio
    parts = []
    parts.append("\n".join(header))
    parts.append("<pre>" + "\n".join(pre) + "</pre>")
    parts.append("\n".join(blocks))
    parts.append("\n<i>Note:</i> ΔVol(1h) vs 30m fa = momentum. ~Vol(30m) è una proxy semplice (Vol1h/2).")
    msg = "\n\n".join(parts)
    return msg

# ---------- Main ---------- #

def main():
    ap = argparse.ArgumentParser(description="Volume watcher — Δ% volume1h vs 30m fa (messaggio HTML con link & address)")
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
    # Invio Telegram
    ap.add_argument("--send-telegram", action="store_true", help="Se presente, invia il messaggio a Telegram in chunk")
    ap.add_argument("--max-chars", type=int, default=3800, help="Max caratteri per chunk Telegram")
    ap.add_argument("--tg-sleep", type=float, default=0.8, help="Pausa tra i chunk Telegram")
    ap.add_argument("--tg-parse-mode", default="HTML",
                    choices=["HTML", "MarkdownV2", "Markdown"], help="Parse mode Telegram")
    ap.add_argument("--tg-token", default=None, help="Override TG_BOT_TOKEN (altrimenti legge da env)")
    ap.add_argument("--tg-chat", default=None, help="Override TG_CHAT_ID (altrimenti legge da env)")
    args = ap.parse_args()

    # 1) Carica lista pair
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
        except subprocess.CalledProcessError as e:
            print("ERROR: test4.py failed", file=sys.stderr)
            print(e.stdout); print(e.stderr, file=sys.stderr)
            sys.exit(1)

    if not pairs:
        msg = "⚠️ Nessuna pair TOP da monitorare."
        ensure_dir("state/telegram")
        Path("state/telegram/msg_volwatch.html").write_text(msg, encoding="utf-8")
        print(msg)
        return

    # 2) Per-pair: fetch, read volumes, delta vs 30m, salva stato
    rows: List[Dict[str, Any]] = []
    state_dir = Path("state/volwatch")
    ensure_dir(str(state_dir))

    for pinfo in pairs:
        sym = pinfo["symbol"]; name = pinfo["name"]; pair = pinfo["pair"]
        data = fetch_pair(pair)
        vols = extract_volumes(data) if data else {"h1": None}
        vol_h1 = vols.get("h1")

        # metadati per link
        url = (data or {}).get("url") or f"https://dexscreener.com/bsc/{pair}"
        base = (data or {}).get("baseToken") or {}
        quote = (data or {}).get("quoteToken") or {}
        base_addr = (base.get("address") or "").lower()
        quote_addr = (quote.get("address") or "").lower()
        base_sym = (base.get("symbol") or "" ).upper() or "BASE"
        quote_sym = (quote.get("symbol") or "").upper() or "QUOTE"

        state_path = state_dir / f"{pair}.json"
        prev = load_prev_state(state_path)

        delta_pct = None
        if prev and prev.get("vol_h1") is not None and vol_h1 is not None:
            prev_h1 = float(prev["vol_h1"])
            now_h1 = float(vol_h1)
            if prev_h1 > 0:
                delta_pct = (now_h1 - prev_h1) / prev_h1 * 100.0

        save_state(state_path, {"ts": int(time.time()), "vol_h1": vol_h1})

        rows.append({
            "symbol": sym,
            "name": name,
            "pair": pair,
            "url": url,
            "base_addr": base_addr,
            "base_sym": base_sym,
            "quote_addr": quote_addr,
            "quote_sym": quote_sym,
            "vol_h1": vol_h1,
            "delta_pct": delta_pct,
        })

    # 3) Costruisci messaggio HTML e salva
    ensure_dir("state/telegram")
    html = build_message(rows)
    out_path = Path("state/telegram/msg_volwatch.html")
    out_path.write_text(html, encoding="utf-8")
    print(f"OK: scritto {out_path} ({len(html)} chars)")

    # 4) Invio Telegram (opzionale)
    if args.send_telegram:
        token = args.tg_token or os.environ.get("TG_BOT_TOKEN", "")
        chat  = args.tg_chat  or os.environ.get("TG_CHAT_ID", "")
        if not token or not chat:
            print("Telegram disabled: TG_BOT_TOKEN/TG_CHAT_ID mancanti.", file=sys.stderr)
        else:
            ok = send_telegram_chunked(html, token, chat,
                                       max_chars=args.max_chars,
                                       sleep_s=args.tg_sleep,
                                       parse_mode=args.tg_parse_mode)
            print(f"Telegram send: {'OK' if ok else 'PARTIAL/FAIL'}")

if __name__ == "__main__":
    main()
