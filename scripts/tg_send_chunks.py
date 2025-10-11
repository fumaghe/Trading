#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Invia un messaggio HTML lungo a Telegram in più chunk <4096 char.
Usa TG_BOT_TOKEN e TG_CHAT_ID dagli environment secrets GitHub Actions.

Esempio:
python scripts/tg_send_chunks.py --file state/telegram/msg_volwatch.html
"""

from __future__ import annotations
import argparse
import os
import sys
import time
import requests

def chunk_by_blocks(text: str, max_chars: int, split_marker: str) -> list[str]:
    """Spezza mantenendo blocchi (header, tabellone, '• ...')."""
    if split_marker in text:
        head, rest = text.split(split_marker, 1)
        blocks = [head] + [split_marker.strip() + b for b in rest.split(split_marker)]
    else:
        blocks = [text]

    parts: list[str] = []
    buf = ""
    for blk in blocks:
        if not blk:
            continue
        # se il blocco singolo già supera max_chars, taglia grezzo per sicurezza
        if len(blk) > max_chars:
            if buf:
                parts.append(buf)
                buf = ""
            start = 0
            while start < len(blk):
                parts.append(blk[start:start + max_chars])
                start += max_chars
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

def main():
    ap = argparse.ArgumentParser(description="Telegram sender (chunked)")
    ap.add_argument("--file", required=True, help="File HTML da inviare (p.es. state/telegram/msg_volwatch.html)")
    ap.add_argument("--max-chars", type=int, default=3800, help="Limite per chunk (default: 3800 < 4096)")
    ap.add_argument("--sleep", type=float, default=0.8, help="Pausa tra i messaggi")
    ap.add_argument("--split-marker", default="\n\n• ", help="Separatore blocchi per-coin (default: '\\n\\n• ')")
    ap.add_argument("--parse-mode", default="HTML", choices=["HTML", "MarkdownV2", "Markdown"])
    args = ap.parse_args()

    token = os.environ.get("TG_BOT_TOKEN", "")
    chat  = os.environ.get("TG_CHAT_ID", "")
    if not token or not chat:
        print("Missing TG_BOT_TOKEN or TG_CHAT_ID; nothing to send.", file=sys.stderr)
        return 0

    try:
        with open(args.file, encoding="utf-8") as f:
            text = f.read()
    except Exception as e:
        print(f"Cannot read file: {args.file} ({e})", file=sys.stderr)
        return 0

    text = (text or "").strip()
    if not text:
        print("Empty message; nothing to send.", file=sys.stderr)
        return 0

    parts = chunk_by_blocks(text, max_chars=args.max_chars, split_marker=args.split_marker)
    api = f"https://api.telegram.org/bot{token}/sendMessage"

    ok_all = True
    for i, p in enumerate(parts, 1):
        r = requests.post(api, data={
            "chat_id": chat,
            "text": p,
            "parse_mode": args.parse_mode,
            "disable_web_page_preview": True,
        })
        ok = False
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
        time.sleep(args.sleep)

    return 0 if ok_all else 1

if __name__ == "__main__":
    sys.exit(main())
