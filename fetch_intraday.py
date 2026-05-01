import os
import sys
import pytz
import yfinance as yf
import pandas as pd
import holidays
from datetime import datetime, timedelta

TICKER = "YSS"
ET = pytz.timezone("America/New_York")
ARCHIVE_PATH = "output/_archive_minute_bars.tsv"   # settled sessions only (T-1+)
DISPLAY_PATH = "output/_display_minute_bars.tsv"   # settled + today (read by compute_vwap)


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


def log_t2_t5_corrections(old_df, new_df, today_date):
    """Compare T-2 to T-5 volumes between old archive and fresh fetch; log any corrections."""
    if old_df.empty or new_df.empty:
        return

    settled_dates = sorted(d for d in new_df["date"].unique() if d < today_date)
    # T-1 is index -1, T-2 is -2, T-5 is -5
    check_dates = settled_dates[-5:-1] if len(settled_dates) >= 2 else []

    found_any = False
    for sess_date in check_dates:
        old_vol = int(old_df[old_df["date"] == sess_date]["volume"].sum()) if not old_df.empty else 0
        new_vol = int(new_df[new_df["date"] == sess_date]["volume"].sum())
        if old_vol > 0:
            pct = (new_vol - old_vol) / old_vol * 100
            if abs(pct) >= 1.0:
                if not found_any:
                    print("1-min volume corrections detected (T-2 to T-5):")
                    found_any = True
                print(f"  {sess_date}: {old_vol:,} → {new_vol:,} ({pct:+.1f}%)")

    if not found_any:
        print("No significant 1-min volume corrections for T-2 to T-5.")


def main():
    if not is_trading_day():
        print(f"Not a trading day ({datetime.now(ET).strftime('%A')}). Pipeline no-op.")
        sys.exit(0)

    today_str  = datetime.now(ET).strftime("%y%m%d")
    today_date = datetime.now(ET).date()
    os.makedirs("output", exist_ok=True)

    # ── Load existing archive ─────────────────────────────────────────────
    old_archive = pd.DataFrame()
    if os.path.exists(ARCHIVE_PATH):
        old_archive = pd.read_csv(ARCHIVE_PATH, sep="\t")
        old_archive["timestamp_et"] = (
            pd.to_datetime(old_archive["timestamp_et"], utc=True).dt.tz_convert(ET)
        )
        old_archive["date"] = old_archive["timestamp_et"].dt.date
        print(f"Loaded archive: {len(old_archive):,} bars from {ARCHIVE_PATH}")
    else:
        print("No archive found — starting fresh.")

    # ── Fetch new 1-min bars (8d preferred, 7d fallback) ─────────────────
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

    # ── Merge: new bars override archive for overlapping timestamps ───────
    archive_out = old_archive.drop(columns=["date"], errors="ignore") if not old_archive.empty else pd.DataFrame()
    frames = [df for df in [archive_out, new_bars] if not df.empty]
    if not frames:
        print("Archive and fetch both empty — nothing to write.")
        sys.exit(0)

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["timestamp_et"], keep="last")
    combined = combined.sort_values("timestamp_et").reset_index(drop=True)
    combined["date"] = combined["timestamp_et"].dt.date

    # ── Log T-2 to T-5 corrections ────────────────────────────────────────
    log_t2_t5_corrections(old_archive, combined, today_date)

    # ── Prune to last 10 trading sessions ─────────────────────────────────
    sessions_10 = get_last_n_trading_sessions(10)
    sessions_set = set(sessions_10)
    combined = combined[combined["date"].isin(sessions_set)].copy()
    print(f"Sessions in dataset: {sorted(combined['date'].unique())}")

    # ── Hourly backfill for sessions not yet represented ─────────────────
    covered = set(combined["date"].unique())
    missing = sessions_set - covered

    if missing:
        print(f"Missing sessions → hourly backfill: {sorted(missing)}")
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
                combined["date"] = combined["timestamp_et"].dt.date
                combined = combined.sort_values("timestamp_et").reset_index(drop=True)
            else:
                print("Hourly fetch returned empty — backfill skipped.")
        except Exception as e:
            print(f"Hourly backfill failed: {e}")
    else:
        print("All 10 sessions covered — no hourly backfill needed.")

    # ── Split: settled (T-1 and older) vs today ───────────────────────────
    settled   = combined[combined["date"] < today_date].copy()
    today_bars = combined[combined["date"] == today_date].copy()
    print(f"Settled bars: {len(settled):,} | Today's bars: {len(today_bars):,}")

    # ── Write settled-only archive (clean, no intraday spikes) ────────────
    out_archive = settled.drop(columns=["date"]).copy()
    out_archive["timestamp_et"] = out_archive["timestamp_et"].map(lambda x: x.isoformat())
    out_archive.to_csv(
        ARCHIVE_PATH, sep="\t", index=False, encoding="utf-8", lineterminator="\n"
    )
    print(f"Wrote settled archive: {len(out_archive):,} bars → {ARCHIVE_PATH}")

    # ── Write display dataset (settled + today) for compute_vwap ──────────
    display = pd.concat([settled, today_bars], ignore_index=True).sort_values("timestamp_et")
    out_display = display.drop(columns=["date"]).copy()
    out_display["timestamp_et"] = out_display["timestamp_et"].map(lambda x: x.isoformat())
    out_display.to_csv(
        DISPLAY_PATH, sep="\t", index=False, encoding="utf-8", lineterminator="\n"
    )
    print(f"Wrote display dataset: {len(out_display):,} bars → {DISPLAY_PATH}")

    # ── Write date-stamped snapshot (debugging reference) ─────────────────
    snap_path = f"output/{today_str}_minute_bars.tsv"
    out_display.to_csv(
        snap_path, sep="\t", index=False, encoding="utf-8", lineterminator="\n"
    )
    print(f"Wrote snapshot → {snap_path}")


if __name__ == "__main__":
    main()
