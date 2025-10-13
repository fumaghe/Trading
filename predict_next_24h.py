# predict_next_24h.py
# Forecast a 5 minuti per le prossime 24h dato un pairAddress DexScreener.
# Re-usa le funzioni del tuo build_dataset.py per:
# - selezione simili (select_similar)
# - fetch OHLCV 5m (gt_fetch_ohlcv_5m)
#
# Output:
# - CSV con 288 righe (5m x 24h) in data/predictions/<POOL_XXXX>/forecast_5m_<YYYYMMDD_HHMM>.csv
# - stampa qualità modello su validation (MAE/RMSE/MAPE)

import os
import sys
import math
import argparse
import warnings
from datetime import datetime, timezone, timedelta
from typing import Tuple, List

import numpy as np
import pandas as pd

# librerie ML (con fallback)
Model = None
try:
    import lightgbm as lgb
    Model = "lgb"
except Exception:
    try:
        import xgboost as xgb
        Model = "xgb"
    except Exception:
        from sklearn.ensemble import RandomForestRegressor
        Model = "rf"

# importa dal tuo builder (stesso folder)
from build_dataset import (
    select_similar,
    ds_get_pair,
    gt_fetch_ohlcv_5m,
    DEFAULT_PCS_DEX_IDS,
    NETWORK,
)

warnings.filterwarnings("ignore", category=UserWarning)

# ------------ utils ------------
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)
    return path

def to_iso(ts: int):
    return datetime.utcfromtimestamp(ts).replace(tzinfo=timezone.utc).isoformat()

def time_features(ts: pd.Series) -> pd.DataFrame:
    # feature cicliche giornaliere
    dt = pd.to_datetime(ts, unit="s", utc=True)
    minute_of_day = dt.dt.hour * 60 + dt.dt.minute
    rad = 2 * np.pi * (minute_of_day / (24 * 60))
    return pd.DataFrame({
        "sin_day": np.sin(rad),
        "cos_day": np.cos(rad),
        "dow": dt.dt.weekday,
    }, index=ts.index)

def add_lags_rolls(df: pd.DataFrame, col="close", lag_list=(1,2,3,6,12,24,36,72,144), rolls=(3,12,36,144)):
    out = df.copy()
    for L in lag_list:
        out[f"{col}_lag{L}"] = out[col].shift(L)
    for R in rolls:
        out[f"{col}_sma{R}"] = out[col].rolling(R).mean()
        out[f"{col}_std{R}"] = out[col].rolling(R).std()
    # returns (log)
    out["ret"] = np.log(out[col]).diff()
    for L in (1,2,3,6,12,24,36,72,144):
        out[f"ret_lag{L}"] = out["ret"].shift(L)
    for R in (3,12,36,144):
        out[f"ret_sma{R}"] = out["ret"].rolling(R).mean()
        out[f"ret_std{R}"] = out["ret"].rolling(R).std()
    return out

def mae(y, yhat): return float(np.mean(np.abs(y - yhat)))
def rmse(y, yhat): return float(np.sqrt(np.mean((y - yhat)**2)))
def mape(y, yhat): return float(np.mean(np.abs((y - yhat) / np.clip(y, 1e-8, None)))) * 100.0

# ------------ dataset building ------------
def fetch_target_and_exo(target_pair: str, chain: str, days: int, sim_df: pd.DataFrame, top_k_exo: int = 10) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Ritorna:
      - df_tgt: OHLCV del target (5m)
      - df_exo: DataFrame con colonne ['ts', 'exo_med_ret'] allineate alla griglia del target
    """
    # target
    df_tgt = gt_fetch_ohlcv_5m(target_pair, network=chain, days=days, include_empty=True)
    if df_tgt is None or df_tgt.empty:
        raise RuntimeError("Nessun OHLCV per il target.")
    df_tgt = df_tgt.sort_values("ts").drop_duplicates("ts")

    # exogenous: median return di top K simili (escludi il target se presente)
    exo_series = []
    for addr in sim_df["pairAddress"].head(top_k_exo):
        if addr.lower() == target_pair.lower():
            continue
        try:
            df = gt_fetch_ohlcv_5m(addr, network=chain, days=days, include_empty=True)
            if df is None or df.empty:
                continue
            df = df[["ts","close"]].drop_duplicates("ts").sort_values("ts")
            df["ret"] = np.log(df["close"]).diff()
            exo_series.append(df[["ts","ret"]].rename(columns={"ret":f"r_{addr[:6]}"}))
        except Exception:
            continue

    if not exo_series:
        # nessun exo disponibile → colonna zero
        df_exo = pd.DataFrame({"ts": df_tgt["ts"].values, "exo_med_ret": 0.0})
        return df_tgt, df_exo

    # merge su ts (outer), poi median across series
    df_exo = None
    for s in exo_series:
        if df_exo is None: df_exo = s.copy()
        else: df_exo = pd.merge(df_exo, s, on="ts", how="outer")
    df_exo = df_exo.sort_values("ts")
    # allinea alla griglia target (reindex con forward-fill)
    df_exo = df_exo.set_index("ts").reindex(df_tgt["ts"]).ffill().bfill().reset_index().rename(columns={"index":"ts"})
    df_exo["exo_med_ret"] = df_exo.drop(columns=["ts"]).median(axis=1, skipna=True).fillna(0.0)
    df_exo = df_exo[["ts","exo_med_ret"]]
    return df_tgt, df_exo

def make_supervised(df_tgt: pd.DataFrame, df_exo: pd.DataFrame) -> pd.DataFrame:
    df = df_tgt.merge(df_exo, on="ts", how="left")
    df["exo_med_ret"] = df["exo_med_ret"].fillna(0.0)
    # feature base sul close
    df = add_lags_rolls(df, col="close")
    # aggiungi lags dell'exo
    for L in (1,2,3,6,12,24,36,72,144):
        df[f"exo_lag{L}"] = df["exo_med_ret"].shift(L)
    # time features
    tf = time_features(df["ts"])
    df = pd.concat([df.reset_index(drop=True), tf.reset_index(drop=True)], axis=1)
    # target da predire: prossimo close (t+1 step)
    df["y"] = df["close"].shift(-1)
    df = df.dropna().reset_index(drop=True)
    return df

# ------------ modeling ------------
def fit_model(df: pd.DataFrame):
    # train/val split: ultime 2 settimane (2*7*24*12 = 4032 step ~ 2 settimane a 5m) per validazione se disponibili
    val_len = min(4032, max(576, int(len(df)*0.15)))  # almeno 2 giorni, fino a 2 settimane
    Xcols = [c for c in df.columns if c not in ("ts","open","high","low","close","volume","y")]
    X_train = df.iloc[:-val_len][Xcols].values
    y_train = df.iloc[:-val_len]["y"].values
    X_val   = df.iloc[-val_len:][Xcols].values
    y_val   = df.iloc[-val_len:]["y"].values

    if Model == "lgb":
        import lightgbm as lgb
        model = lgb.LGBMRegressor(
            n_estimators=1200,
            learning_rate=0.03,
            max_depth=-1,
            num_leaves=64,
            subsample=0.9,
            colsample_bytree=0.9,
            reg_alpha=0.1,
            reg_lambda=1.0,
            random_state=42
        )
        # Niente 'verbose' nella fit; uso callback per silenziare il log
        model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            eval_metric="l2",
            callbacks=[lgb.log_evaluation(period=0)]
        )
    elif Model == "xgb":
        import xgboost as xgb
        model = xgb.XGBRegressor(
            n_estimators=1200,
            learning_rate=0.03,
            max_depth=8,
            subsample=0.9,
            colsample_bytree=0.9,
            reg_alpha=0.1,
            reg_lambda=1.0,
            random_state=42,
            tree_method="hist"
        )
        model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
    else:
        from sklearn.ensemble import RandomForestRegressor
        model = RandomForestRegressor(
            n_estimators=600,
            max_depth=None,
            min_samples_leaf=2,
            n_jobs=-1,
            random_state=42
        )
        model.fit(X_train, y_train)

    # qualità su validation
    yhat_val = model.predict(X_val)
    metrics = {
        "val_mae": mae(y_val, yhat_val),
        "val_rmse": rmse(y_val, yhat_val),
        "val_mape": mape(y_val, yhat_val),
        "n_train": int(len(X_train)),
        "n_val": int(len(X_val)),
        "model": Model
    }
    return model, Xcols, metrics

def recursive_forecast(df_full: pd.DataFrame, model, Xcols: List[str], horizon_steps: int = 288) -> pd.DataFrame:
    """
    Esegue una forecast ricorsiva di 'horizon_steps' a 5 minuti.
    Usa l'ultima riga di df_full come stato di partenza e aggiorna feature ad ogni step.
    """
    df = df_full.copy().reset_index(drop=True)
    last_ts = int(df.loc[df.index[-1], "ts"])
    step = 300  # 5 minuti

    rows = []
    for i in range(1, horizon_steps+1):
        next_ts = last_ts + step

        # aggiorna exo assumendo 'exo_med_ret' costante vs ultimo noto
        exo = float(df.loc[df.index[-1], "exo_med_ret"])

        # fai spazio alla nuova riga "placeholder"
        new = {
            "ts": next_ts,
            "open": np.nan, "high": np.nan, "low": np.nan,
            "close": float(df.loc[df.index[-1], "y"]),  # usa la last y come "close corrente" per features
            "volume": 0.0,
            "exo_med_ret": exo
        }
        df = pd.concat([df, pd.DataFrame([new])], ignore_index=True)

        # ricalcola features
        df_feat = add_lags_rolls(df, col="close")
        for L in (1,2,3,6,12,24,36,72,144):
            df_feat[f"exo_lag{L}"] = df_feat["exo_med_ret"].shift(L)
        tf = time_features(df_feat["ts"])
        df_feat = pd.concat([df_feat.reset_index(drop=True), tf.reset_index(drop=True)], axis=1)

        # compila riga corrente (X) e predici y (close t+1)
        cur = df_feat.iloc[-1:].copy()
        for req in Xcols:
            if req not in cur.columns:
                cur[req] = 0.0
        X = cur[Xcols].fillna(method="ffill").fillna(method="bfill").fillna(0.0).values
        yhat = float(model.predict(X)[0])

        # inserisci la y prevista
        df.loc[df.index[-1], "y"] = yhat
        df.loc[df.index[-1], "close"] = max(yhat, 0.0)

        rows.append({
            "ts": next_ts,
            "time_iso": to_iso(next_ts),
            "yhat": max(yhat, 0.0)
        })

        last_ts = next_ts

    fc = pd.DataFrame(rows)
    return fc

# ------------ main pipeline ------------
def run_pipeline(
    target_pair: str,
    chain: str,
    days: int,
    count_sim: int,
    relax: float,
    no_quote_filter: bool,
    dex_ids: List[str],
    allow_any_dex: bool,
    outdir: str,
    horizon_minutes: int = 24*60,
    step_minutes: int = 5,
    top_k_exo: int = 10,
    dedup_base: bool = True,
    explain: bool = False,
    diag: bool = False
):
    # 1) target e simili
    _ = ds_get_pair(chain, target_pair)  # sanity
    df_sim = select_similar(
        chain=chain,
        target_pair_addr=target_pair,
        count=count_sim,
        relax=relax,
        dex_ids=dex_ids,
        allow_any_dex=allow_any_dex,
        quotes_allow=None,
        no_quote_filter=no_quote_filter,
        anchors=None,
        source_mode="both",
        dedup_base=dedup_base,
        explain=explain,
        diag=diag
    )

    # 2) fetch dati
    df_tgt, df_exo = fetch_target_and_exo(target_pair, chain, days, df_sim, top_k_exo=top_k_exo)

    # 3) supervised dataset
    df_sup = make_supervised(df_tgt, df_exo)

    # 4) fit modello
    model, Xcols, metrics = fit_model(df_sup)

    # 5) forecast ricorsiva
    steps = int(horizon_minutes // step_minutes)
    fc = recursive_forecast(df_sup, model, Xcols, horizon_steps=steps)

    # 6) intervallo di confidenza semplice (basato su residui val)
    val_len = metrics["n_val"]
    Xcols2 = [c for c in df_sup.columns if c not in ("ts","open","high","low","close","volume","y")]
    y_val = df_sup.iloc[-val_len:]["y"].values
    yhat_val = model.predict(df_sup.iloc[-val_len:][Xcols2].values)
    resid_std = float(np.std(y_val - yhat_val))
    fc["yhat_lower"] = np.clip(fc["yhat"] - 1.96 * resid_std, 0.0, None)
    fc["yhat_upper"] = fc["yhat"] + 1.96 * resid_std

    # 7) salvataggio
    subdir = ensure_dir(os.path.join(outdir, chain, f"POOL_{target_pair[:6]}"))
    ts_str = datetime.utcnow().strftime("%Y%m%d_%H%M")
    out_csv = os.path.join(subdir, f"forecast_5m_{ts_str}.csv")
    meta_txt = os.path.join(subdir, "model_card.txt")

    fc.to_csv(out_csv, index=False)
    with open(meta_txt, "w", encoding="utf-8") as f:
        f.write(f"pair: {target_pair}\n")
        f.write(f"chain: {chain}\n")
        f.write(f"model: {metrics['model']}\n")
        f.write(f"n_train: {metrics['n_train']}\n")
        f.write(f"n_val: {metrics['n_val']}\n")
        f.write(f"val_mae: {metrics['val_mae']:.6f}\n")
        f.write(f"val_rmse: {metrics['val_rmse']:.6f}\n")
        f.write(f"val_mape: {metrics['val_mape']:.4f}%\n")
        f.write(f"relax: {relax}\n")
        f.write(f"no_quote_filter: {no_quote_filter}\n")
        f.write(f"count_sim: {count_sim}\n")
        f.write(f"top_k_exo: {top_k_exo}\n")
        f.write(f"generated_utc: {datetime.utcnow().isoformat()}Z\n")

    print("\n=== MODEL VALIDATION ===")
    print(f"Model: {metrics['model']} | Ntrain={metrics['n_train']} Nval={metrics['n_val']}")
    print(f"MAE={metrics['val_mae']:.6f}  RMSE={metrics['val_rmse']:.6f}  MAPE={metrics['val_mape']:.4f}%")

    print("\n=== FORECAST (head) ===")
    print(fc.head(10).to_string(index=False))
    print(f"\nSaved forecast to: {out_csv}")
    print(f"Saved model card to: {meta_txt}")

    return out_csv

# ------------ CLI ------------
def parse_args():
    ap = argparse.ArgumentParser(description="Forecast 5-min x 24h per un pair DexScreener (BSC).")
    ap.add_argument("--target_pair", required=True, help="pairAddress DexScreener (es: 0xf0a9...)")
    ap.add_argument("--days", type=int, default=60, help="Storico da usare (giorni)")
    ap.add_argument("--count_sim", type=int, default=60, help="Quanti pool simili usare per scegliere exo")
    ap.add_argument("--relax", type=float, default=2.0, help="Ampiezza filtri per simili (>=1.0)")
    ap.add_argument("--no_quote_filter", action="store_true", help="Disattiva filtro sul quote")
    ap.add_argument("--dex_ids", default=",".join(DEFAULT_PCS_DEX_IDS), help="dexIds separati da virgola")
    ap.add_argument("--allow_any_dex", action="store_true", help="Accetta qualsiasi dexId")
    ap.add_argument("--top_k_exo", type=int, default=10, help="Quanti simili usare per la mediana exo")
    ap.add_argument("--outdir", default="data/predictions", help="Cartella output")
    ap.add_argument("--chain", default=NETWORK, help="Rete (default: bsc)")
    ap.add_argument("--horizon_minutes", type=int, default=24*60, help="Orizzonte in minuti (default 1440)")
    ap.add_argument("--step_minutes", type=int, default=5, help="Passo in minuti (default 5)")
    ap.add_argument("--explain", action="store_true")
    ap.add_argument("--diag", action="store_true")
    return ap.parse_args()

def main():
    args = parse_args()
    dex_ids = [d.strip().lower() for d in args.dex_ids.split(",") if d.strip()] or None

    run_pipeline(
        target_pair=(args.target_pair or "").lower(),
        chain=args.chain,
        days=args.days,
        count_sim=args.count_sim,
        relax=max(args.relax, 1.0),
        no_quote_filter=bool(args.no_quote_filter),
        dex_ids=dex_ids,
        allow_any_dex=bool(args.allow_any_dex),
        outdir=args.outdir,
        horizon_minutes=args.horizon_minutes,
        step_minutes=args.step_minutes,
        top_k_exo=args.top_k_exo,
        dedup_base=True,
        explain=args.explain,
        diag=args.diag
    )

if __name__ == "__main__":
    main()
