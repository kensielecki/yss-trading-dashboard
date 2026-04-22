import os
import sys
import pytz
import pandas as pd
from datetime import datetime

ET = pytz.timezone("America/New_York")
ARCHIVE_PATH = "output/_archive_minute_bars.tsv"


def main():
    today = datetime.now(ET).strftime("%y%m%d")

    if not os.path.exists(ARCHIVE_PATH):
        print(f"Archive not found: {ARCHIVE_PATH}", file=sys.stderr)
        sys.exit(1)

    print(f"Reading {ARCHIVE_PATH}")
    df = pd.read_csv(ARCHIVE_PATH, sep="\t")
    df["timestamp_et"] = pd.to_datetime(df["timestamp_et"], utc=True).dt.tz_convert(ET)
    df = df.sort_values("timestamp_et").reset_index(drop=True)
    df = df[df["volume"] > 0]

    # Ensure source column exists (backwards compat with old archives)
    if "source" not in df.columns:
        df["source"] = "yfinance_1m"

    df["date"] = df["timestamp_et"].dt.date
    df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
    df["tp_vol"] = df["typical_price"] * df["volume"]

    # ── Daily summary ──────────────────────────────────────────────────────
    daily = (
        df.groupby("date", sort=True)
        .agg(
            open=("open", "first"),
            close=("close", "last"),
            volume=("volume", "sum"),
            tp_vol_sum=("tp_vol", "sum"),
            vol_sum=("volume", "sum"),
        )
        .reset_index()
    )
    daily["daily_vwap"] = (daily["tp_vol_sum"] / daily["vol_sum"]).round(4)
    daily["pct_change"] = (daily["close"].pct_change() * 100).round(4)

    # high_fidelity: True if session has any non-hourly-backfill bars
    hf = (
        df.groupby("date")["source"]
        .apply(lambda s: not all(v == "yfinance_hourly_backfill" for v in s))
        .reset_index(name="high_fidelity")
    )
    daily = daily.merge(hf, on="date")

    summary_path = f"output/{today}_daily_summary.tsv"
    daily[
        ["date", "open", "close", "pct_change", "volume", "daily_vwap", "high_fidelity"]
    ].to_csv(
        summary_path, sep="\t", index=False, encoding="utf-8",
        lineterminator="\n", float_format="%.4f"
    )
    print(f"Wrote daily summary → {summary_path}")

    # ── Running intraday VWAP (used by render_page for chart) ─────────────
    df["cum_tp_vol"] = df.groupby("date")["tp_vol"].cumsum()
    df["cum_vol"]    = df.groupby("date")["volume"].cumsum()
    df["running_vwap"] = (df["cum_tp_vol"] / df["cum_vol"]).round(4)

    running = df[["timestamp_et", "close", "running_vwap", "volume"]].copy()
    running["timestamp_et"] = df["timestamp_et"].map(lambda x: x.isoformat())

    running_path = f"output/{today}_running_vwap.tsv"
    running.to_csv(
        running_path, sep="\t", index=False, encoding="utf-8",
        lineterminator="\n", float_format="%.4f"
    )
    print(f"Wrote running VWAP → {running_path}")

    # ── Headline metrics ───────────────────────────────────────────────────
    vwap_10d   = round(df["tp_vol"].sum() / df["volume"].sum(), 4)
    last_row   = df.iloc[-1]
    last_price = round(float(last_row["close"]), 4)
    last_ts    = last_row["timestamp_et"].isoformat()
    high_10d   = round(float(df["high"].max()), 4)
    low_10d    = round(float(df["low"].min()), 4)
    avg_vol    = round(float(daily["volume"].mean()), 0)
    latest_pct = round(float(daily["pct_change"].iloc[-1]), 4) if len(daily) >= 2 else 0.0

    pd.DataFrame([{
        "last_price":         last_price,
        "last_updated":       last_ts,
        "vwap_10d":           vwap_10d,
        "high_10d":           high_10d,
        "low_10d":            low_10d,
        "avg_volume_10d":     avg_vol,
        "latest_pct_change":  latest_pct,
    }]).to_csv(
        f"output/{today}_headline_metrics.tsv", sep="\t", index=False,
        encoding="utf-8", lineterminator="\n", float_format="%.4f"
    )
    print(f"Wrote headline metrics → output/{today}_headline_metrics.tsv")


if __name__ == "__main__":
    main()
