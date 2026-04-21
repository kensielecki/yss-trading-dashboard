import os
import sys
import pytz
import yfinance as yf
import pandas as pd
from datetime import datetime

TICKER = "YSS"
ET = pytz.timezone("America/New_York")


def is_trading_day():
    return datetime.now(ET).weekday() < 5  # Mon–Fri only


def main():
    if not is_trading_day():
        print(f"Not a trading day ({datetime.now(ET).strftime('%A')}). Pipeline no-op.")
        sys.exit(0)

    today = datetime.now(ET).strftime("%y%m%d")
    os.makedirs("output", exist_ok=True)

    print(f"Fetching 10-day 1-min bars for {TICKER} via yfinance…")
    # Yahoo Finance caps 1-min history at ~8 calendar days per request;
    # period="5d" reliably returns the last 5 trading sessions.
    df = yf.Ticker(TICKER).history(
        period="5d", interval="1m", prepost=False, auto_adjust=False
    )

    if df.empty:
        print(f"No data returned for {TICKER}. Market may be closed or data unavailable.")
        sys.exit(0)

    # Normalise index to ET
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC").tz_convert(ET)
    else:
        df.index = df.index.tz_convert(ET)

    out = pd.DataFrame({
        "timestamp_et": df.index.map(lambda x: x.isoformat()),
        "open":         df["Open"].round(6),
        "high":         df["High"].round(6),
        "low":          df["Low"].round(6),
        "close":        df["Close"].round(6),
        "volume":       df["Volume"].astype(int),
        "source":       "yfinance",
    })

    path = f"output/{today}_minute_bars.tsv"
    out.to_csv(path, sep="\t", index=False, encoding="utf-8", lineterminator="\n")
    print(f"Wrote {len(out):,} bars → {path}")


if __name__ == "__main__":
    main()
