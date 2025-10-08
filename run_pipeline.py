#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
Orchestratore: esegue lo screener, legge i winners, lancia la stima price-push per ogni pair,
e stampa SOLO la lista winners (S1+S2+S3) con i token1 necessari per arrivare alle soglie richieste.

Esempio (PowerShell):
python ./run_pipeline.py --screener-cmd "python ./test4.py --seed-from perps --workers 12 --min-liq 200000 --max-tickers-scan 40 --dominance 0.30 --skip-unchanged-days 0 --rps-cg 0.5 --rps-ds 2.0 --funnel-show 100 --require-v3 --json-out ./winners.json" --winners-json ./winners.json --rpc https://bsc-dataseed.binance.org --factors 1.5,2.0,3.5 --max-coins 25
"""

import argparse
import json
import os
import subprocess
from decimal import Decimal
import sys

# importa la funzione libreria dal tool di stima (V3)
import defi_price_push as dpp


def run_cmd_capture(cmd: str) -> str:
    """Esegue un comando shell e ritorna stdout (non solleva su exit!=0)."""
    env = os.environ.copy()
    # Forza UTF-8 nel processo figlio e scoraggia output "ricco" (Rich) quando non è un TTY.
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    env["TERM"] = "dumb"
    env["RICH_FORCE_TERMINAL"] = "false"
    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=True,
        text=True,
        env=env,
    )
    out, _ = p.communicate()
    return out or ""


def format_number(x: str) -> str:
    # x è una stringa decimale (es "12345.678900000000000000")
    try:
        d = Decimal(x)
        return f"{d:,.6f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return x


def main():
    # Forza UTF-8 anche per la stampa locale (Windows 3.7+)
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except Exception:
            pass

    ap = argparse.ArgumentParser()
    ap.add_argument("--screener-cmd", required=True, help="Comando completo per eseguire lo screener")
    ap.add_argument("--winners-json", required=True, help="Path del JSON scritto dallo screener (--json-out)")
    ap.add_argument("--rpc", default=os.environ.get("BSC_RPC", "https://bsc-dataseed.binance.org"))
    ap.add_argument("--factors", default="1.3,1.5,2.0", help="Fattori target separati da virgola (es. 1.5,2.0,3.5)")
    ap.add_argument("--max-coins", type=int, default=25, help="Limite coin da stimare; 0 o negativo = nessun limite")
    ap.add_argument("--max-telegram-chars", type=int, default=3900, help="0 o negativo = non troncare (occhio a Telegram)")
    ap.add_argument("--out-file", type=str, default=None, help="Se impostato, salva l'output COMPLETO su file (UTF-8)")
    args = ap.parse_args()

    # 1) Esegui screener e cattura stdout (serve solo per generare il JSON)
    screener_out = run_cmd_capture(args.screener_cmd)

    # 2) Carica winners.json; se manca, mostra output screener per debug e termina
    if not os.path.exists(args.winners_json):
        print("=== Screener output (stdout+stderr) ===")
        print(screener_out.strip())
        raise SystemExit(
            f"winners JSON non trovato: {args.winners_json}  "
            f"(verifica che lo screener supporti --json-out e che il path sia corretto)"
        )

    with open(args.winners_json, "r", encoding="utf-8") as f:
        payload = json.load(f)
    winners = payload.get("winners") or []

    # 3) Filtra SOLTANTO Pancake V3 per sicurezza (anche se nello screener usi --require-v3)
    winners_v3 = []
    for w in winners:
        pair = (w.get("pair") or {})
        dexid = (pair.get("dexId") or "").lower()
        chain = (pair.get("chainId") or "").lower()
        if "pancake" in dexid and "v3" in dexid and chain == "bsc":
            winners_v3.append(w)

    # 4) Prepara fattori
    factors = [Decimal(x.strip()) for x in args.factors.split(",") if x.strip()]

    # 5) Per ogni winner V3, lancia analyze_pool e costruisci SOLO il blocco "price push"
    lines = []
    count = 0
    total_v3 = len(winners_v3)

    for w in winners_v3:
        if args.max_coins and args.max_coins > 0 and count >= args.max_coins:
            lines.append(f"...(+{total_v3 - args.max_coins} altre)")
            break

        sym = (w.get("symbol") or "").upper()
        name = w.get("name") or w.get("id") or ""
        pair_addr = ((w.get("pair") or {}).get("pairAddress") or "").lower()
        dexid = ((w.get("pair") or {}).get("dexId") or "")

        if not pair_addr:
            lines.append(f"{sym} · {name} · nessun pair address")
            continue

        try:
            res = dpp.analyze_pool(pair_addr, args.rpc, factors=factors)
            t0 = res["token0"]["symbol"]
            t1 = res["token1"]["symbol"]
            # Mappa target -> token1_needed
            perfs = []
            for row in res["targets"]:
                tgt = row["target"]                  # es: "+50%"
                need = row["token1_needed"]         # es: "123.000000000000000000 USDT"
                perfs.append(f"{tgt}: {need}")
            header = f"{sym} · {name} · Pool: {pair_addr[:8]}…{pair_addr[-6:]} · [{dexid}] · P0({t1}/1 {t0})={format_number(res['price0'])}"
            lines.append(header)
            for p in perfs:
                lines.append("  - " + p)
            count += 1
        except Exception as e:
            lines.append(f"{sym} · {name} · [{dexid}] · errore: {e}")

    # 6) SOLO Price Push (niente funnel/screener). Opzionalmente salva full text su file.
    out = []
    out.append(f"— Price Push (token1 necessari) —  [coin elaborate: {count}/{total_v3}]")
    out.append("\n".join(lines))

    final_text_full = "\n".join(out)

    if args.out_file:
        try:
            with open(args.out_file, "w", encoding="utf-8") as f:
                f.write(final_text_full)
        except Exception as e:
            print(f"[WARN] Impossibile scrivere out_file '{args.out_file}': {e}")

    final_text = final_text_full
    if args.max_telegram_chars and args.max_telegram_chars > 0:
        if len(final_text) > args.max_telegram_chars:
            final_text = final_text[:args.max_telegram_chars] + "\n\n...(troncato)"

    print(final_text)


if __name__ == "__main__":
    main()
