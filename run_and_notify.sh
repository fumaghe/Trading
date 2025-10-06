#!/usr/bin/env bash
set -euo pipefail

# === CONFIG TELEGRAM (test) ===
TG_TOKEN="8429976502:AAGApe-HkCmmgEEcbwzEqPZnZzwBsyyobFM"   # il tuo token di test
TG_CHAT_ID="302113556"                                     # il tuo chat_id

# === COMANDO SCREENER (il tuo) ===
CMD=(python test3.py --seed-from perps --workers 12 --min-liq 200000 --max-tickers-scan 40 --dominance 0.30 --skip-unchanged-days 0 --rps-cg 0.5 --rps-ds 2.0 --funnel-show 100)

# === ESECUZIONE ===
echo ">> Running screener..."
set +e
OUTPUT="$("${CMD[@]}" 2>&1)"
CODE=$?
set -e

HEADER="✅ Screener OK"
[ $CODE -ne 0 ] && HEADER="❌ Screener ERROR ($CODE)"

# Telegram limita ~4096 caratteri → tronchiamo in modo sicuro
MAX=3900
if [ ${#OUTPUT} -gt $MAX ]; then
  OUTPUT="${OUTPUT:0:$MAX}\n\n…(troncato)"
fi

MESSAGE="${HEADER}\n\n${OUTPUT}"

# === INVIO A TELEGRAM ===
curl -sS "https://api.telegram.org/bot${TG_TOKEN}/sendMessage" \
  -d chat_id="${TG_CHAT_ID}" \
  --data-urlencode text="${MESSAGE}" \
  >/dev/null

echo ">> Messaggio inviato a Telegram."
