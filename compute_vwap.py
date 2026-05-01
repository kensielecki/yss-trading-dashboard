import os
import sys
import warnings
import pytz
import pandas as pd
import yfinance as yf
from datetime import datetime

warnings.filterwarnings("ignore")

ET = pytz.timezone("America/New_York")
TICKER = "YSS"
DISPLAY_PATH  = "output/_display_minute_bars.tsv"   # settled + today
ARCHIVE_PATH  = "output/_archive_minute_bars.tsv"   # fallback


def main():
    today     = datetime.now(ET).strftime("%y%m%d")
    today_date = datetime.now(ET).date()

    # ── Read 1-min bar dataset ────────────────────────────────────────────
    src = DISPLAY_PATH if os.path.exists(DISPLAY_PATH) else ARCHIVE_PATH
    if not os.path.exists(src):
        print(f"No bar data found ({DISPLAY_PATH} or {ARCHIVE_PATH}).", file=sys.stderr)
        sys.exit(1)

    print(f"Reading {src}")
    df = pd.read_csv(src, sep="\t")
    df["timestamp_et"] = pd.to_datetime(df["timestamp_et"], utc=True).dt.tz_convert(ET)
    df = df.sort_values("timestamp_et").reset_index(drop=True)
    df = df[df["volume"] > 0]

    if "source" not in df.columns:
        df["source"] = "yfinance_1m"

    df["date"] = df["timestamp_et"].dt.date
    df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
    df["tp_vol"] = df["typical_price"] * df["volume"]

    # ── Daily summary from 1-min bars (VWAP needs the bar-level data) ─────
    daily = (
        df.groupby("date", sort=True)
        .agg(
            open_1m=("open",   "first"),
            close_1m=("close", "last"),
            volume_1m=("volume", "sum"),
            tp_vol_sum=("tp_vol", "sum"),
            vol_sum=("volume", "sum"),
        )
        .reset_index()
    )
    daily["daily_vwap"] = (daily["tp_vol_sum"] / daily["vol_sum"]).round(4)

    hf = (
        df.groupby("date")["source"]
        .apply(lambda s: not all(v == "yfinance_hourly_backfill" for v in s))
        .reset_index(name="high_fidelity")
    )
    daily = daily.merge(hf, on="date")

    # ── Rolling 10-day VWAP as of each session close ──────────────────────
    # For each session date D, compute VWAP using all bars in the archive up
    # to and including D. This shows how the 10-day VWAP has shifted over time.
    session_dates = sorted(df["date"].unique())
    rolling_vwap_map = {}
    for d in session_dates:
        df_up_to = df[df["date"] <= d]
        rolling_vwap_map[d] = round(
            df_up_to["tp_vol"].sum() / df_up_to["volume"].sum(), 4
        )
    daily["rolling_vwap_10d"] = daily["date"].map(rolling_vwap_map)

    # ── Fetch official daily bars (interval=1d) ───────────────────────────
    # These match the Yahoo Finance historical data page exactly.
    # Used to override settled session open/close/volume for display accuracy.
    official = {}
    try:
        raw_daily = yf.Ticker(TICKER).history(
            period="15d", interval="1d", prepost=False, auto_adjust=False
        )
        raw_daily.index = raw_daily.index.tz_convert(ET)
        for idx, row in raw_daily.iterrows():
            official[idx.date()] = {
                "open":   round(float(row["Open"]),  4),
                "close":  round(float(row["Close"]), 4),
                "volume": int(row["Volume"]),
                "high":   round(float(row["High"]),  4),
                "low":    round(float(row["Low"]),   4),
            }
        print(f"Fetched official daily bars for {len(official)} sessions")
    except Exception as e:
        print(f"Daily bar fetch failed ({e}) — using 1-min bar open/close/volume for all sessions")

    # Override settled sessions (T-1 and older) with official daily values.
    # Today's row keeps live 1-min data so the intraday display stays current.
    settled_dates = {d for d in official if d < today_date}

    def pick(row, field, fallback):
        d = row["date"]
        return official[d][field] if d in settled_dates and d in official else fallback

    daily["open"]   = daily.apply(lambda r: pick(r, "open",   r["open_1m"]),   axis=1)
    daily["close"]  = daily.apply(lambda r: pick(r, "close",  r["close_1m"]),  axis=1)
    daily["volume"] = daily.apply(lambda r: pick(r, "volume", r["volume_1m"]), axis=1)
    daily["pct_change"] = (daily["close"].pct_change() * 100).round(4)

    summary_path = f"output/{today}_daily_summary.tsv"
    daily[
        ["date", "open", "close", "pct_change", "volume", "daily_vwap", "rolling_vwap_10d", "high_fidelity"]
    ].to_csv(
        summary_path, sep="\t", index=False, encoding="utf-8",
        lineterminator="\n", float_format="%.4f"
    )
    print(f"Wrote daily summary → {summary_path}")

    # ── Running intraday VWAP (used by chart) ─────────────────────────────
    df["cum_tp_vol"]   = df.groupby("date")["tp_vol"].cumsum()
    df["cum_vol"]      = df.groupby("date")["volume"].cumsum()
    df["running_vwap"] = (df["cum_tp_vol"] / df["cum_vol"]).round(4)

    running = df[["timestamp_et", "close", "running_vwap", "volume"]].copy()
    running["timestamp_et"] = df["timestamp_et"].map(lambda x: x.isoformat())

    running_path = f"output/{today}_running_vwap.tsv"
    running.to_csv(
        running_path, sep="\t", index=False, encoding="utf-8",
        lineterminator="\n", float_format="%.4f"
    )
    print(f"Wrote running VWAP → {running_path}")

    # ── Headline metrics ──────────────────────────────────────────────────
    vwap_10d   = round(df["tp_vol"].sum() / df["volume"].sum(), 4)
    last_row   = df.iloc[-1]
    last_price = round(float(last_row["close"]), 4)
    last_ts    = last_row["timestamp_et"].isoformat()

    # Use official high/low where available for 10d range
    if official:
        off_highs = [v["high"] for d, v in official.items() if d in set(df["date"])]
        off_lows  = [v["low"]  for d, v in official.items() if d in set(df["date"])]
        high_10d  = round(max(off_highs) if off_highs else float(df["high"].max()), 4)
        low_10d   = round(min(off_lows)  if off_lows  else float(df["low"].min()),  4)
    else:
        high_10d = round(float(df["high"].max()), 4)
        low_10d  = round(float(df["low"].min()),  4)

    avg_vol    = round(float(daily["volume"].mean()), 0)
    latest_pct = round(float(daily["pct_change"].iloc[-1]), 4) if len(daily) >= 2 else 0.0

    pd.DataFrame([{
        "last_price":        last_price,
        "last_updated":      last_ts,
        "vwap_10d":          vwap_10d,
        "high_10d":          high_10d,
        "low_10d":           low_10d,
        "avg_volume_10d":    avg_vol,
        "latest_pct_change": latest_pct,
    }]).to_csv(
        f"output/{today}_headline_metrics.tsv", sep="\t", index=False,
        encoding="utf-8", lineterminator="\n", float_format="%.4f"
    )
    print(f"Wrote headline metrics → output/{today}_headline_metrics.tsv")


if __name__ == "__main__":
    main()
