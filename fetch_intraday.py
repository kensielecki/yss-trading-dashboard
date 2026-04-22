import os
import sys
import pytz
import yfinance as yf
import pandas as pd
import holidays
from datetime import datetime, timedelta, date

TICKER = "YSS"
ET = pytz.timezone("America/New_York")
ARCHIVE_PATH = "output/_archive_minute_bars.tsv"


def _nyse_holidays(start_year=2024, end_year=2028):
    h = {}
    for y in range(start_year, end_year + 1):
        h.update(holidays.NYSE(years=y))
    return h


def get_last_n_trading_sessions(n=10):
    nyse_h = _nyse_holidays()
    today = datetime.now(ET).date()
    sessions = []
    d = today
    while len(sessions) < n:
        if d.weekday() < 5 and d not in nyse_h:
            sessions.append(d)
        d -= timedelta(days=1)
    return sorted(sessions)


def is_trading_day():
    return datetime.now(ET).weekday() < 5


def main():
    if not is_trading_day():
        print(f"Not a trading day ({datetime.now(ET).strftime('%A')}). Pipeline no-op.")
        sys.exit(0)

    today = datetime.now(ET).strftime("%y%m%d")
    os.makedirs("output", exist_ok=True)

    # ── Load existing archive ──────────────────────────────────────────────
    if os.path.exists(ARCHIVE_PATH):
        archive = pd.read_csv(ARCHIVE_PATH, sep="\t")
        archive["timestamp_et"] = (
            pd.to_datetime(archive["timestamp_et"], utc=True).dt.tz_convert(ET)
        )
        print(f"Loaded archive: {len(archive):,} bars from {ARCHIVE_PATH}")
    else:
        archive = pd.DataFrame(
            columns=["timestamp_et", "open", "high", "low", "close", "volume", "source"]
        )
        print("No archive found — starting fresh.")

    # ── Fetch new 1-min bars (8d preferred, 7d fallback) ──────────────────
    new_bars = pd.DataFrame()
    for period in ("8d", "7d"):
        try:
            raw = yf.Ticker(TICKER).history(
                period=period, interval="1m", prepost=False, auto_adjust=False
            )
            if not raw.empty:
                print(f"Fetched {len(raw):,} 1-min bars using period={period}")
                if raw.index.tz is None:
                    raw.index = raw.index.tz_localize("UTC").tz_convert(ET)
                else:
                    raw.index = raw.index.tz_convert(ET)
                new_bars = pd.DataFrame({
                    "timestamp_et": raw.index,
                    "open":         raw["Open"].round(6),
                    "high":         raw["High"].round(6),
                    "low":          raw["Low"].round(6),
                    "close":        raw["Close"].round(6),
                    "volume":       raw["Volume"].astype(int),
                    "source":       "yfinance_1m",
                })
                break
            print(f"period={period} returned empty; trying next…")
        except Exception as e:
            print(f"period={period} failed ({e}); trying next…")

    if new_bars.empty:
        print(f"No 1-min data returned for {TICKER}. Will use archive only.")

    # ── Merge: new bars override archive for overlapping timestamps ────────
    frames = [df for df in [archive, new_bars] if not df.empty]
    if not frames:
        print("Archive and fetch both empty — nothing to write.")
        sys.exit(0)

    combined = pd.concat(frames, ignore_index=True)
    # new_bars appended last → keep="last" prefers new fetch on duplicate timestamps
    combined = combined.drop_duplicates(subset=["timestamp_et"], keep="last")
    combined = combined.sort_values("timestamp_et").reset_index(drop=True)

    # ── Prune to last 10 trading sessions ─────────────────────────────────
    sessions_10 = get_last_n_trading_sessions(10)
    sessions_set = set(sessions_10)
    combined["date"] = combined["timestamp_et"].dt.date
    combined = combined[combined["date"].isin(sessions_set)].copy()
    print(f"Sessions in archive after pruning: {sorted(combined['date'].unique())}")

    # ── Hourly backfill for sessions not yet represented ──────────────────
    covered = set(combined["date"].unique())
    missing = sessions_set - covered

    if missing:
        print(f"Missing sessions → fetching hourly backfill: {sorted(missing)}")
        try:
            h_raw = yf.Ticker(TICKER).history(
                period="10d", interval="1h", prepost=False, auto_adjust=False
            )
            if not h_raw.empty:
                if h_raw.index.tz is None:
                    h_raw.index = h_raw.index.tz_localize("UTC").tz_convert(ET)
                else:
                    h_raw.index = h_raw.index.tz_convert(ET)
                hourly = pd.DataFrame({
                    "timestamp_et": h_raw.index,
                    "open":         h_raw["Open"].round(6),
                    "high":         h_raw["High"].round(6),
                    "low":          h_raw["Low"].round(6),
                    "close":        h_raw["Close"].round(6),
                    "volume":       h_raw["Volume"].astype(int),
                    "source":       "yfinance_hourly_backfill",
                })
                hourly["date"] = hourly["timestamp_et"].dt.date
                hourly = hourly[hourly["date"].isin(missing)]
                filled = len(hourly["date"].unique())
                print(f"Hourly backfill: {len(hourly):,} bars covering {filled} session(s)")
                combined = pd.concat([combined, hourly], ignore_index=True)
                combined = combined.sort_values("timestamp_et").reset_index(drop=True)
            else:
                print("Hourly fetch returned empty — backfill skipped.")
        except Exception as e:
            print(f"Hourly backfill failed: {e}")
    else:
        print("All 10 sessions covered — no hourly backfill needed.")

    # ── Write archive (canonical, no date prefix, overwritten each run) ────
    out = combined.drop(columns=["date"]).copy()
    out["timestamp_et"] = out["timestamp_et"].map(lambda x: x.isoformat())
    out.to_csv(ARCHIVE_PATH, sep="\t", index=False, encoding="utf-8", lineterminator="\n")
    print(f"Wrote archive: {len(out):,} bars → {ARCHIVE_PATH}")

    # ── Write date-stamped snapshot (debugging reference) ─────────────────
    snap_path = f"output/{today}_minute_bars.tsv"
    out.to_csv(snap_path, sep="\t", index=False, encoding="utf-8", lineterminator="\n")
    print(f"Wrote snapshot → {snap_path}")


if __name__ == "__main__":
    main()
