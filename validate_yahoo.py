"""
Scrape Yahoo Finance history page for YSS and compare against yfinance daily bars.
Writes output/validation_result.json.

Thresholds:
  open / close : >3% difference → warning
  volume       : >5% difference → warning

To acknowledge a warning and clear it from the dashboard, ask Claude Code to add
the relevant entry to output/validation_overrides.json.
"""

import json
import os
import sys
import warnings
import pytz
import yfinance as yf
from datetime import datetime

warnings.filterwarnings("ignore")

ET              = pytz.timezone("America/New_York")
TICKER          = "YSS"
YAHOO_URL       = f"https://finance.yahoo.com/quote/{TICKER}/history/"
RESULT_PATH     = "output/validation_result.json"
OVERRIDE_PATH   = "output/validation_overrides.json"
PRICE_THRESHOLD = 0.03   # 3%
VOL_THRESHOLD   = 0.05   # 5%


def scrape_yahoo_history():
    """Return list of {date, open, close, volume} dicts from Yahoo Finance history page."""
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        print("playwright not installed — skipping scrape.")
        return None, "playwright not installed"

    results = []
    error   = None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx     = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        )
        page = ctx.new_page()
        try:
            page.goto(YAHOO_URL, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_timeout(3_000)

            # Dismiss cookie/consent banners if present
            for sel in ['button[name="agree"]', 'button:has-text("Accept all")',
                        'button:has-text("Accept")']:
                try:
                    page.click(sel, timeout=2_000)
                    page.wait_for_timeout(1_000)
                    break
                except Exception:
                    pass

            page.wait_for_selector("table tbody tr", timeout=15_000)
            rows = page.query_selector_all("table tbody tr")

            for row in rows:
                cells = row.query_selector_all("td")
                if len(cells) < 7:
                    continue
                texts = [c.inner_text().strip() for c in cells]

                # Skip dividend / split rows (second cell won't parse as a number)
                try:
                    float(texts[1].replace(",", ""))
                except ValueError:
                    continue

                try:
                    dt = datetime.strptime(texts[0], "%b %d, %Y").date()

                    def to_num(s):
                        return float(s.replace(",", "").replace("$", ""))

                    results.append({
                        "date":   str(dt),
                        "open":   round(to_num(texts[1]), 2),
                        "close":  round(to_num(texts[4]), 2),
                        "volume": int(to_num(texts[6])),
                    })
                except (ValueError, IndexError):
                    continue

        except PWTimeout as exc:
            error = f"Playwright timeout: {exc}"
            print(error)
        except Exception as exc:
            error = str(exc)
            print(f"Scrape error: {exc}")
        finally:
            browser.close()

    return results, error


def get_yfinance_daily():
    """Return {date_str: {open, close, volume}} from yfinance daily endpoint."""
    raw = yf.Ticker(TICKER).history(
        period="15d", interval="1d", prepost=False, auto_adjust=False
    )
    raw.index = raw.index.tz_convert(ET)
    result = {}
    for idx, row in raw.iterrows():
        result[str(idx.date())] = {
            "open":   round(float(row["Open"]),  2),
            "close":  round(float(row["Close"]), 2),
            "volume": int(row["Volume"]),
        }
    return result


def main():
    os.makedirs("output", exist_ok=True)
    run_at = datetime.now(ET).isoformat()

    print("Scraping Yahoo Finance history page…")
    scraped_rows, scrape_error = scrape_yahoo_history()

    if scraped_rows is None:
        payload = {
            "run_at":        run_at,
            "scrape_success": False,
            "rows_scraped":  0,
            "error":         scrape_error or "unknown",
            "discrepancies": [],
        }
        with open(RESULT_PATH, "w") as f:
            json.dump(payload, f, indent=2)
        print(f"Scrape failed — wrote empty result to {RESULT_PATH}")
        sys.exit(0)

    print(f"Scraped {len(scraped_rows)} rows")

    print("Fetching yfinance daily bars for comparison…")
    yf_data = get_yfinance_daily()

    discrepancies = []
    checks = [
        ("open",   PRICE_THRESHOLD),
        ("close",  PRICE_THRESHOLD),
        ("volume", VOL_THRESHOLD),
    ]

    for row in scraped_rows[:10]:
        date_str = row["date"]
        if date_str not in yf_data:
            continue
        yf_row = yf_data[date_str]

        for field, threshold in checks:
            scraped_val = row[field]
            yf_val      = yf_row[field]
            if scraped_val == 0:
                continue
            pct_diff = abs(yf_val - scraped_val) / scraped_val
            if pct_diff > threshold:
                entry = {
                    "date":        date_str,
                    "field":       field,
                    "yfinance_val": yf_val,
                    "scraped_val": scraped_val,
                    "pct_diff":    round((yf_val - scraped_val) / scraped_val * 100, 2),
                }
                discrepancies.append(entry)
                print(
                    f"  DISCREPANCY {date_str} {field}: "
                    f"yfinance={yf_val} vs scraped={scraped_val} "
                    f"({pct_diff * 100:.1f}%)"
                )

    payload = {
        "run_at":         run_at,
        "scrape_success": scrape_error is None,
        "rows_scraped":   len(scraped_rows),
        "error":          scrape_error,
        "discrepancies":  discrepancies,
    }
    with open(RESULT_PATH, "w") as f:
        json.dump(payload, f, indent=2)

    if discrepancies:
        print(f"{len(discrepancies)} discrepancy(ies) found — dashboard will show warning.")
    else:
        print("All values within thresholds — no warnings.")


if __name__ == "__main__":
    main()
