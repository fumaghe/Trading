#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Volume Watch (spike 1h vs 1h precedente, trigger ≥ X)
- Ogni 5 minuti:
  * Legge lista TOP (da test4.py o da file) e per ogni pair interroga DexScreener.
  * Mantiene uno stato per pair: state/volwatch/{pair}.json con campioni m5 (volume 5m).
  * Calcola Vol(1h) come somma degli ultimi 12 campioni m5 e Vol(1h_prev) come i 12 precedenti.
  * Se Vol(1h) ≥ X * Vol(1h_prev) (default X=10) e copertura sufficiente, triggera l’alert.
- Output:
  * Genera un unico messaggio HTML in state/telegram/msg_volwatch.html con SOLO le coin triggerate.
  * Invia Telegram (singolo messaggio) solo se c’è almeno un trigger.
- Fix inclusi:
  * Timestamp allineato ai bucket 5m; dedup per ts; backoff per 429/5xx; HTML escape; retention 150'.
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

CHAIN_SLUG = "bsc"
DEX_PAIRS_URL = f"https://api.dexscreener.com/latest/dex/pairs/{CHAIN_SLUG}" + "/{pair}"
TOP_HEADER = "— Tutti e 3 i parametri (TOP) —"
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

SCRIPT_DIR = Path(__file__).resolve().parent

# Bucket e retention
BUCKET_SEC = 300         # 5m
HOUR_SEC   = 3600
WIN_1H_SEC = HOUR_SEC
RETENTION_SEC = 150 * 60  # 150' ~ 2.5h per coprire 2 ore + margine

# ---------- Utils ---------- #

def ensure_dir(p: str):
    Path(p).mkdir(parents=True, exist_ok=True)

def now_iso_local() -> str:
    # runner con TZ=Europe/Rome consigliato
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M")

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
            ([A-Z0-9._-]+)\s*[·•\-]\s*       # symbol
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

# ---------- DexScreener (HTTP con backoff) ---------- #

def fetch_pair(pair: str) -> Optional[Dict[str, Any]]:
    url = DEX_PAIRS_URL.format(pair=pair)
    backoff = 1.0
    for _ in range(5):
        try:
            r = requests.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff); backoff = min(backoff*2, 16); continue
            r.raise_for_status()
            j = r.json()
            return (j.get("pairs") or [None])[0] or None
        except Exception:
            time.sleep(backoff); backoff = min(backoff*2, 16)
    return None

def extract_m5(p: Dict[str, Any]) -> Optional[float]:
    try:
        vol = (p.get("volume") or {}) if p else {}
        v = vol.get("m5")
        return float(v) if v is not None else None
    except Exception:
        return None

# ---------- State (m5 rolling buffer) ---------- #

def load_prev_state(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

def atomic_write(path: Path, content: str):
    ensure_dir(str(path.parent))
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)

def save_state(path: Path, data: Dict[str, Any]):
    try:
        atomic_write(path, json.dumps(data, ensure_ascii=False))
    except Exception:
        pass

def trim_history(hist: List[Dict[str, Any]], now_ts: int) -> List[Dict[str, Any]]:
    # dedup per ts (last-wins) + retention
    uniq: Dict[int, float] = {}
    for it in hist or []:
        try:
            ts = int(it.get("ts", 0))
            v = float(it.get("vol_m5"))
            if v >= 0 and now_ts - ts <= RETENTION_SEC:
                uniq[ts] = v
        except Exception:
            continue
    items = [{"ts": ts, "vol_m5": v} for ts, v in uniq.items()]
    items.sort(key=lambda x: x["ts"])
    return items

def sum_window(hist: List[Dict[str, Any]], start_ts: int, end_ts: int) -> Optional[float]:
    """Somma i campioni m5 in (start_ts, end_ts]."""
    if start_ts >= end_ts:
        return 0.0
    vals = [h["vol_m5"] for h in hist if start_ts < h["ts"] <= end_ts]
    if not vals:
        return None
    return float(sum(vals))

def count_window(hist: List[Dict[str, Any]], start_ts: int, end_ts: int) -> int:
    return sum(1 for h in hist if start_ts < h["ts"] <= end_ts)

def calc_h1_from_m5(prev_hist: List[Dict[str, Any]], now_ts: int, vol_m5: Optional[float],
                    min_cov: int = 10):
    """
    Aggiorna history con il campione corrente (bucket 5m) e calcola:
    - h1_now: somma m5 negli ultimi 60' (12 bucket)
    - h1_prev: somma m5 nei 60' precedenti
    - cov_now, cov_prev: n° campioni nelle finestre
    Applica un requisito di copertura minima (default 10/12).
    """
    hist = trim_history(prev_hist or [], now_ts)

    # append/aggiorna campione corrente (dedup per ts)
    if vol_m5 is not None:
        if not hist or hist[-1]["ts"] != now_ts:
            hist.append({"ts": now_ts, "vol_m5": float(vol_m5)})
        else:
            hist[-1]["vol_m5"] = float(vol_m5)

    start_now  = now_ts - WIN_1H_SEC
    end_now    = now_ts
    start_prev = now_ts - 2 * WIN_1H_SEC
    end_prev   = now_ts - WIN_1H_SEC

    h1_now  = sum_window(hist, start_now, end_now)
    h1_prev = sum_window(hist, start_prev, end_prev)
    cov_now  = count_window(hist, start_now, end_now)
    cov_prev = count_window(hist, start_prev, end_prev)

    # Copertura minima
    if cov_now < min_cov or cov_prev < min_cov:
        h1_now, h1_prev = None, None

    return hist, h1_now, h1_prev, cov_now, cov_prev

# ---------- Message (solo trigger) ---------- #

def format_money(x: Optional[float]) -> str:
    if x is None:
        return "n.d."
    try:
        return f"{int(round(float(x))):,}$".replace(",", ".")
    except Exception:
        return "n.d."

def build_spike_message(spikes: List[Dict[str, Any]], threshold_x: float) -> str:
    """
    spikes: list con chiavi:
      symbol, name, pair, h1_now, h1_prev, ratio, delta_pct
    """
    header = []
    header.append(f"🚨 <b>VOLUME SPIKE</b> — 1h ≥ {threshold_x:g}× della 1h precedente")
    header.append(f"<i>Aggiornato:</i> {htmlesc(now_iso_local())}\n")

    parts = ["\n".join(header)]

    for r in spikes:
        sym  = htmlesc(r.get("symbol") or "")
        name = htmlesc(r.get("name") or "")
        pair = (r.get("pair") or "").lower()
        link = f"https://dexscreener.com/{CHAIN_SLUG}/{pair}"
        h1n  = format_money(r.get("h1_now"))
        h1p  = format_money(r.get("h1_prev"))
        ratio = r.get("ratio")
        delta = r.get("delta_pct")
        ratio_s = f"{ratio:.2f}×" if ratio is not None else "n.d."
        delta_s = f"+{delta:.2f}%" if (delta is not None and delta >= 0) else (f"{delta:.2f}%" if delta is not None else "n.d.")

        block = []
        block.append(f"• <b>{sym}</b> — {name}")
        block.append(f"Pair: <code>{htmlesc(pair)}</code>")
        block.append(f'DexScreener: <a href="{link}">{htmlesc(link)}</a>')
        block.append(f"Vol 1h: {h1n}  |  Prev 1h: {h1p}")
        block.append(f"Ratio: <b>{ratio_s}</b>  |  Δ: {delta_s}")
        parts.append("\n".join(block) + "\n")

    return "\n".join(parts).strip()

# ---------- Main ---------- #

def main():
    ap = argparse.ArgumentParser(
        description="Volume watcher — Spike 1h vs 1h precedente (trigger ≥ X), messaggio HTML con solo i trigger."
    )
    ap.add_argument("--mode", choices=["from-screener", "from-file"], default="from-screener",
                    help="Origine lista pair: 'from-screener' rilancia test4.py, 'from-file' legge JSON")
    ap.add_argument("--pairs-file", type=str, default="state/top_pairs.json",
                    help="File JSON con [{'symbol','name','pair'}...]")

    # Parametri per test4.py (mode=from-screener)
    ap.add_argument("--python-bin", default=sys.executable)
    ap.add_argument("--min-liq", type=int, default=200000)
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--dominance", type=float, default=0.30)
    ap.add_argument("--max-tickers-scan", type=int, default=40)
    ap.add_argument("--rps-cg", type=float, default=0.5)
    ap.add_argument("--rps-ds", type=float, default=2.0)
    ap.add_argument("--funnel-show", type=int, default=100)
    ap.add_argument("--skip-unchanged-days", type=int, default=0)

    # Trigger & copertura
    ap.add_argument("--threshold-x", type=float, default=10.0, help="Soglia X per il trigger (Vol1h ≥ X × Vol1h_prev)")
    ap.add_argument("--min-cov", type=int, default=10, help="Copertura minima (campioni m5) per ciascuna 1h [0..12]")

    # Invio Telegram
    ap.add_argument("--send-telegram", action="store_true", help="Se presente, invia il messaggio a Telegram se ci sono trigger")
    ap.add_argument("--tg-token", default=None, help="Override TG_BOT_TOKEN (altrimenti legge da env)")
    ap.add_argument("--tg-chat", default=None, help="Override TG_CHAT_ID (altrimenti legge da env)")

    args = ap.parse_args()

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
            atomic_write(Path("state/top_pairs.json"), json.dumps(pairs, ensure_ascii=False))
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
        msg = "⚠️ Nessuna pair TOP da monitorare."
        ensure_dir("state/telegram")
        atomic_write(Path("state/telegram/msg_volwatch.html"), msg)
        print(msg)
        return

    # 2) Per-pair: fetch, aggiorna stato m5, calcola h1 vs h1_prev e filtra trigger
    spikes: List[Dict[str, Any]] = []
    state_dir = Path("state/volwatch")
    ensure_dir(str(state_dir))

    # ancora il timestamp al bucket 5m
    now_ts = int(time.time())
    now_ts = (now_ts // BUCKET_SEC) * BUCKET_SEC

    for pinfo in pairs:
        sym = pinfo["symbol"]; name = pinfo["name"]; pair = (pinfo["pair"] or "").lower()
        data = fetch_pair(pair) or {}
        vol_m5 = extract_m5(data)

        state_path = state_dir / f"{pair}.json"
        prev = load_prev_state(state_path) or {}
        prev_hist = prev.get("m5_history") or []

        hist, h1_now, h1_prev, cov_now, cov_prev = calc_h1_from_m5(prev_hist, now_ts, vol_m5, min_cov=args.min_cov)

        # salva nuovo stato
        save_state(state_path, {"ts": now_ts, "m5_history": hist})

        # trigger (richiede copertura)
        if h1_now is not None and h1_prev is not None and h1_prev > 0:
            ratio = float(h1_now) / float(h1_prev)
            if ratio >= float(args.threshold_x):
                delta_pct = (float(h1_now) - float(h1_prev)) / float(h1_prev) * 100.0
                spikes.append({
                    "symbol": sym,
                    "name": name,
                    "pair": pair,
                    "h1_now": h1_now,
                    "h1_prev": h1_prev,
                    "ratio": ratio,
                    "delta_pct": delta_pct,
                    "cov_now": cov_now,
                    "cov_prev": cov_prev,
                })

    # Ordina per ratio decrescente
    spikes.sort(key=lambda r: r.get("ratio", 0.0), reverse=True)

    # 3) Costruisci messaggio HTML e salva
    ensure_dir("state/telegram")
    if spikes:
        html = build_spike_message(spikes, threshold_x=args.threshold_x)
    else:
        html = f"ℹ️ Nessun spike ≥ {args.threshold_x:g}× (Vol 1h vs 1h precedente). Aggiornato: {htmlesc(now_iso_local())}"

    out_path = Path("state/telegram/msg_volwatch.html")
    atomic_write(out_path, html)
    print(f"OK: scritto {out_path} ({len(html)} chars) — triggers: {len(spikes)}")

    # 4) Invio Telegram solo se ci sono trigger
    if args.send_telegram and spikes:
        token = args.tg_token or os.environ.get("TG_BOT_TOKEN", "")
        chat  = args.tg_chat  or os.environ.get("TG_CHAT_ID", "")
        if not token or not chat:
            print("Telegram disabled: TG_BOT_TOKEN/TG_CHAT_ID mancanti.", file=sys.stderr)
        else:
            ok = send_telegram_single(html, token, chat, parse_mode="HTML")
            print(f"Telegram send: {'OK' if ok else 'FAIL'}")

if __name__ == "__main__":
    main()
