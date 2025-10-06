# Crypto Screener — README

Screener giornaliero (CEX/DEX) per filtrare coin che rispettano **4 criteri**:

1) **Top CEX spot = Bitget** (dai tickers CoinGecko).  
2) **Perpetual su entrambi**: Binance Futures (USDⓈ-M) **e** Bybit (linear).  
3) **BSC presente** e **main LP** (più liquidità) **su PancakeSwap** (preferibilmente **V3**).  
4) **Pancake V3**: **zero/poca** liquidità *sopra* una soglia di prezzo (es. **+200%**).

Implementa: **stage-gating**, **cache multilivello**, **lazy subgraph**, **euristiche Bitget**, **parallelismo con rate-limit** e **indice locale** per saltare coin stabili tra i run.

---

## Requisiti

- Python 3.9+
- Librerie:
  ```bash
  pip install requests tenacity rich
  ```


## Struttura Progetto
crypto_screen_v2.py
config.yaml                 # (opzionale) configurazione consigliata
.cache/                     # cache http
  perps_binance.json
  perps_bybit.json
  coingecko/<coin_id>.json
  dexscreener/<bsc_address>.json
  subgraph/<pool_address>.json
state/
  index.json                # indice locale con ultimo esito per coin
run.log                     # (opzionale) log del cron


## Esecuzione
#### Comandi rapidi

#### Run quotidiano (profilo “daily”)
python crypto_screen_v2.py --pages 2 --per-page 250 --workers 10 --min-liq 200000 --price-mult 3.0 --max-ticks-above 0 --max-tickers-scan 15 --dominance 0.60 --ttl-cg 172800 --ttl-perps 604800 --ttl-ds 86400 --ttl-sg 86400 --dex-liq-delta 0.40 --skip-unchanged-days 3 --cache-root .cache --state-root state --rps-cg 1.0 --rps-ds 2.0 --rps-sg 1.0


#### Run veloce di verifica (più aggressivo)
python crypto_screen_v2.py --pages 1 --per-page 250 --workers 14 --min-liq 300000 --price-mult 3.0 --max-ticks-above 0 --max-tickers-scan 10 --dominance 0.65 --ttl-cg 172800 --ttl-perps 604800 --ttl-ds 86400 --ttl-sg 86400 --dex-liq-delta 0.5 --skip-unchanged-days 5 --rps-cg 1.2 --rps-ds 3.0 --rps-sg 1.2


#### Strict V3 (escludi LP non v3)
python crypto_screen_v2.py --pages 2 --per-page 250 --workers 12 --min-liq 200000 --require-v3 --price-mult 3.0 --max-ticks-above 0


#### Analizza solo alcune coin
python crypto_screen_v2.py --ids deagentai,aster,aia --workers 8 --min-liq 150000 --price-mult 3.0 --max-ticks-above 0


#### Forza refresh liste perps
rm -f .cache/perps_binance.json .cache/perps_bybit.json
#Cancella a mano se non va la cartella o il file
python crypto_screen_v2.py --pages 2 --per-page 250 --workers 10


Windows Task Scheduler
Program/script: python.exe
Argomenti: C:\path\crypto_screen_v2.py --pages 2 --per-page 250 --workers 10 --min-liq 200000 --price-mult 3.0 --max-ticks-above 0
Start in: C:\path\

| Flag                           | Descrizione                                         |
| ------------------------------ | --------------------------------------------------- |
| `--pages`, `--per-page`        | Quante coin dal seed CoinGecko.                     |
| `--ids`                        | Analizza solo specifiche coin (bypassa `pages`).    |
| `--workers`                    | Thread concorrenti.                                 |
| `--min-liq`                    | Liquidità USD minima del main LP Pancake.           |
| `--price-mult`                 | Soglia prezzo (3.0 = +200%).                        |
| `--max-ticks-above`            | Numero massimo di tick **sopra soglia** (0 = zero). |
| `--require-v3`                 | Scarta LP Pancake non v3.                           |
| `--max-tickers-scan`           | Quanti tickers spot leggere (euristica).            |
| `--dominance`                  | Dominanza Bitget per short-circuit (0–1).           |
| `--ttl-*`                      | TTL cache CG/Perps/DS/Subgraph.                     |
| `--dex-liq-delta`              | Δ liquidità che forza ricalcolo ticks.              |
| `--skip-unchanged-days`        | Giorni di “silenzio” per coin stabili.              |
| `--rps-*`                      | Rate limit per sorgente (richieste/sec).            |
| `--quiet`/`--no-verbose`       | Livello di log.                                     |
| `--cache-root`, `--state-root` | Cartelle di cache e stato.                          |



## Se si rompe qualcosa
Contattare fumaghe
oppure

### Best practice di performance
Aumenta --min-liq per tagliare presto i candidati.
Usa --require-v3 se ti serve solo v3 → niente subgraph su v2.
Non azzerare la cache: dal 2° run in poi taglia gran parte dei tempi.
Orario notturno: riduce 429/403.
Tieni --workers 8–16 e taratura --rps-* conservativa.

### Troubleshooting
429/418 / 403: riduci --rps-*, aumenta TTL, alza --skip-unchanged-days.
Poche coin passano: guarda state/index.json → status motivazionale.
Falsi KO Bitget: alza --max-tickers-scan (es. 25) o abbassa --dominance (es. 0.5).
Subgraph lento: alza --dex-liq-delta (es. 0.6) e --min-liq.
Verifica manuale: usa --ids su 2–3 coin note e confronta.

### Note sulle fonti
CoinGecko: tickers spot + mapping contract (no key).
Binance Futures / Bybit v5: elenchi perpetual (no key).
DexScreener: pairs per token (liquidità, dexId) (no key).
Pancake v3 Subgraph: slot0 e ticks per pool v3 (no key).

# Comando che userei giornalmente
python crypto_screen_v2.py --pages 2 --per-page 250 --workers 12 --min-liq 200000 --price-mult 3.0 --max-ticks-above 0 --max-tickers-scan 15 --dominance 0.60 --ttl-cg 172800 --ttl-perps 604800 --ttl-ds 86400 --ttl-sg 86400 --dex-liq-delta 0.40 --skip-unchanged-days 3 --rps-cg 1.0 --rps-ds 2.0 --rps-sg 1.0


alternativa
python crypto_screen_v2.py --pages 2 --per-page 250 --workers 8 --min-liq 300000 --price-mult 3.0 --max-ticks-above 0 --max-tickers-scan 25 --dominance 0.35 --ttl-cg 172800 --ttl-perps 604800 --ttl-ds 86400 --ttl-sg 86400 --dex-liq-delta 0.40 --skip-unchanged-days 3 --rps-cg 0.6 --rps-ds 2.0 --rps-sg 0.6


alternativa2
python crypto_screen_v2.py --pages 6 --per-page 250 --workers 8 --min-liq 300000 --price-mult 3.0 --max-ticks-above 0 --max-tickers-scan 25 --dominance 0.35 --ttl-cg 172800 --ttl-perps 604800 --ttl-ds 86400 --ttl-sg 86400 --rps-cg 0.6 --rps-ds 2.0 --rps-sg 0.6


# Test 3
Seed “perps-first”

Scarica l’elenco dei perpetual attivi da Binance Futures e Bybit.

Prende l’intersezione dei base symbol (solo le coin che hanno il perp su entrambi).

Mappa ogni symbol al relativo CoinGecko id usando /coins/list?include_platform=true (preferisce chi ha BSC nei platforms).
(In alternativa puoi usare --seed-from cg per partire dai markets di CoinGecko e poi filtrare sui perps.)

Pipeline a 3 stage (con stop anticipato)

S1 – Perps su entrambi: ricontrollo di sicurezza binance∩bybit. Se non passa → scarta.

S2 – Top spot = Bitget: legge i tickers CoinGecko, filtra out “stale/anomaly” e trust non green/yellow, poi
valuta i primi N (--max-tickers-scan) con short-circuit se Bitget domina oltre --dominance.

S3 – BSC + Pancake main LP: dalla scheda CG prende l’address BSC, interroga DexScreener, cerca la pair BSC con liquidità maggiore su Pancake, richiede liq >= --min-liq e opzionalmente solo v3 (--require-v3).
Se passa, finisce in “Tutti e 3 i parametri”.

Robustezza & performance

Cache su disco (.cache/): perps (7g), coin CG (48h), DexScreener (24h).

Indice stato (state/index.json): memorizza esito e last_checked per saltare coin invariate per --skip-unchanged-days.

Rate-limit con jitter per evitare 429; thread-pool controllato con --workers.

Filtri qualità sui tickers CG (stale/anomaly/trust).

Log Rich: pannelli di riepilogo, progress bar, tabelle Funnel (S1, S1+S2, S1+S2+S3) e due liste finali:

“Tutti e 3 i parametri” (Bitget top + Pancake LP su BSC, dopo essere passata da perps)

“Almeno 2 (S1+S2)” per visibilità su cosa manca solo dello step LP.