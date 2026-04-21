# YSS Trading Dashboard

Automated trading dashboard for York Space Systems (NYSE: YSS). Refreshed three times per trading day via GitHub Actions and published to GitHub Pages at:
**https://kensielecki.github.io/yss-trading-dashboard/**

## What it does

1. Fetches the last 5 sessions of 1-minute bars for YSS from Yahoo Finance (via yfinance).
2. Computes per-day VWAP, running intraday VWAP (resets at 09:30 ET), and a 5-day aggregate VWAP scalar.
3. Renders a static HTML dashboard (`docs/index.html`) with a Plotly price+VWAP chart, four metric cards, and a daily summary table.
4. Commits updated `output/` TSVs and `docs/index.html` back to `main`; GitHub Pages serves the result automatically.

## Data source

**Primary:** [yfinance](https://github.com/ranaroussi/yfinance) — an unofficial Yahoo Finance scraper. No API key required.

**Known limitation:** yfinance is unofficial and occasionally breaks when Yahoo changes their endpoints. If the pipeline fails consistently, upgrade options are:

- **(a) Alpha Vantage** — paid plans from ~$25/mo. Swap a key into `ALPHAVANTAGE_API_KEY` env var and rewire `fetch_intraday.py` to call `TIME_SERIES_INTRADAY`.
- **(b) Polygon.io** — reliable paid data with a generous free tier for EOD; intraday requires a paid plan.
- **(c) Finnhub** — free tier includes basic intraday data for US equities.

## Local setup

```bash
git clone https://github.com/kensielecki/yss-trading-dashboard.git
cd yss-trading-dashboard
pip install -r requirements.txt
bash run_pipeline.sh
open docs/index.html
```

## Manual run

```bash
bash run_pipeline.sh
```

Logs are written to `output/logs/YYMMDD_HHMM_pipeline.log`. Logs older than 8 weeks are pruned automatically by `prune_logs.py`.

## Script reference

| Script | Purpose |
|---|---|
| `fetch_intraday.py` | Fetch 5-day 1-min bars; write `output/YYMMDD_minute_bars.tsv` |
| `compute_vwap.py` | Compute daily VWAP, running VWAP, headline metrics; write 3 TSVs |
| `render_page.py` | Read TSVs; render `docs/index.html` via Plotly |
| `run_pipeline.sh` | Orchestrate all three scripts; log to `output/logs/` |
| `prune_logs.py` | Delete logs older than 8 weeks |

## GitHub Pages enablement

1. Push the `main` branch (must include `docs/index.html`).
2. In the GitHub repo go to **Settings → Pages**.
3. Under **Source** select **Deploy from a branch**.
4. Set **Branch** to `main`, folder to `/docs`, then click **Save**.
5. The site will be live at `https://kensielecki.github.io/yss-trading-dashboard/` within ~60 seconds.

## Triggering a manual workflow run

1. In the repo go to **Actions → Refresh YSS Data**.
2. Click **Run workflow → Run workflow** (green button).
3. Wait ~60–90 seconds. The workflow fetches data, updates TSVs, rebuilds `docs/index.html`, commits, and pushes. GitHub Pages deploys automatically from the pushed commit.

## Refresh schedule

The workflow fires Mon–Fri at approximately:

| Target ET time | Purpose |
|---|---|
| 9:45 AM | Captures opening 15 min of trading |
| 12:30 PM | Midday snapshot |
| 4:15 PM | Post-close final bars |

Six cron entries cover both EDT (UTC-4) and EST (UTC-5) to handle Daylight Saving Time transitions automatically. The Python pipeline's weekday check ensures a clean no-op on weekends.

## Output file layout

```
output/
  YYMMDD_minute_bars.tsv        # raw 1-min OHLCV + source column
  YYMMDD_daily_summary.tsv      # date, open, close, pct_change, volume, daily_vwap
  YYMMDD_running_vwap.tsv       # per-bar running VWAP time series
  YYMMDD_headline_metrics.tsv   # single-row summary (last price, 10d VWAP, etc.)
  logs/
    YYMMDD_HHMM_pipeline.log
```

All timestamps are ISO 8601 in `America/New_York` (ET). TSVs are UTF-8, tab-separated, Unix line endings.

## Known limitations

- yfinance is unofficial and may break without notice.
- Yahoo Finance caps 1-min intraday data at ~8 calendar days per request; the pipeline fetches the last 5 trading sessions (`period="5d"`).
- Upgrading to a paid data source (Alpha Vantage, Polygon.io) would unlock deeper history.
- Historical data before the yfinance window is not stored; re-running extends the TSV in place.
- Holiday detection beyond weekday filtering is not implemented; the pipeline runs on federal holidays but simply re-fetches the same trailing data.
