import glob
import json
import os
import sys
import pytz
import pandas as pd
import holidays
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots
from dateutil import parser as dtparser
from datetime import datetime

ET = pytz.timezone("America/New_York")


def latest_file(pattern):
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"No file matching {pattern}", file=sys.stderr)
        sys.exit(1)
    return files[-1]


def fmt_price(val):
    return f"${float(val):.2f}" if pd.notna(val) else "—"


def fmt_vol(val):
    return f"{int(val):,}" if pd.notna(val) else "—"


def get_market_holidays(start_year=2025, end_year=2027):
    h = {}
    for y in range(start_year, end_year + 1):
        h.update(holidays.NYSE(years=y))
    return sorted(d.strftime("%Y-%m-%d") for d in h)


def load_validation_warning():
    """Return HTML warning banner string, or empty string if no active discrepancies."""
    result_path   = "output/validation_result.json"
    override_path = "output/validation_overrides.json"

    if not os.path.exists(result_path):
        return ""

    with open(result_path) as f:
        result = json.load(f)

    discrepancies = result.get("discrepancies", [])
    if not discrepancies:
        return ""

    ack_keys = set()
    if os.path.exists(override_path):
        with open(override_path) as f:
            overrides = json.load(f).get("overrides", [])
        ack_keys = {(o["date"], o["field"]) for o in overrides}

    active = [d for d in discrepancies if (d["date"], d["field"]) not in ack_keys]
    if not active:
        return ""

    items = "\n".join(
        f'<li><strong>{d["date"]}</strong> &mdash; {d["field"]}: '
        f'yfinance {d["yfinance_val"]:,.2f} vs Yahoo Finance {d["scraped_val"]:,.2f} '
        f'({d["pct_diff"]:+.1f}%)</li>'
        for d in active
    )
    return f"""
  <div class="warning-banner">
    <strong>&#9888; Data validation warning</strong> &mdash;
    yfinance and Yahoo Finance website differ beyond acceptable threshold:
    <ul>{items}</ul>
    <span class="warning-sub">To clear this warning, ask Claude Code to acknowledge the
    discrepancy (it will update <code>output/validation_overrides.json</code>).</span>
  </div>"""


def main():
    summary_df  = pd.read_csv(latest_file("output/*_daily_summary.tsv"),   sep="\t")
    running_df  = pd.read_csv(latest_file("output/*_running_vwap.tsv"),    sep="\t")
    headline_df = pd.read_csv(latest_file("output/*_headline_metrics.tsv"), sep="\t")

    running_df["timestamp_et"] = (
        pd.to_datetime(running_df["timestamp_et"], utc=True).dt.tz_convert(ET)
    )

    if "high_fidelity" in summary_df.columns:
        summary_df["high_fidelity"] = (
            summary_df["high_fidelity"].astype(str).str.lower() == "true"
        )
    else:
        summary_df["high_fidelity"] = True

    m = headline_df.iloc[0]
    last_price  = float(m["last_price"])
    vwap_10d    = float(m["vwap_10d"])
    high_10d    = float(m["high_10d"])
    low_10d     = float(m["low_10d"])
    avg_vol_10d = int(m["avg_volume_10d"])
    last_updated_raw = str(m["last_updated"])

    def fmt_ts(dt):
        h    = dt.hour % 12 or 12
        ampm = "AM" if dt.hour < 12 else "PM"
        return f"{h}:{dt.minute:02d} {ampm} ET, {dt.strftime('%b %d, %Y')}"

    try:
        lu = dtparser.parse(last_updated_raw)
        if lu.tzinfo is None:
            lu = ET.localize(lu)
        data_through_fmt = fmt_ts(lu.astimezone(ET))
    except Exception:
        data_through_fmt = last_updated_raw

    page_refreshed_fmt = fmt_ts(datetime.now(ET))
    market_holidays    = get_market_holidays()

    # ── Session metadata ──────────────────────────────────────────────────
    running_df["_date"] = running_df["timestamp_et"].dt.date
    session_dates = sorted(running_df["_date"].unique())

    _open_by_date = (
        summary_df.assign(_date=pd.to_datetime(summary_df["date"]).dt.date)
        .set_index("_date")["open"]
        .to_dict()
    )
    _rolling_vwap_by_date = (
        summary_df.assign(_date=pd.to_datetime(summary_df["date"]).dt.date)
        .set_index("_date")["rolling_vwap_10d"]
        .to_dict()
        if "rolling_vwap_10d" in summary_df.columns else {}
    )

    # ── Plotly chart ──────────────────────────────────────────────────────
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Price line
    fig.add_trace(
        go.Scatter(
            x=running_df["timestamp_et"],
            y=running_df["close"],
            mode="lines",
            name="Close",
            line=dict(color="#1a73e8", width=1.5),
            hovertemplate="%{x|%b %d %H:%M}<br>Close: $%{y:.2f}<extra></extra>",
        ),
        secondary_y=False,
    )

    # Volume bars
    fig.add_trace(
        go.Bar(
            x=running_df["timestamp_et"],
            y=running_df["volume"],
            name="Volume",
            marker_color="#bbd4f5",
            opacity=0.7,
            hovertemplate="%{x|%b %d %H:%M}<br>Vol: %{y:,.0f}<extra></extra>",
        ),
        secondary_y=True,
    )

    # 10-day aggregate VWAP horizontal line
    fig.add_hline(
        y=vwap_10d,
        line_dash="solid",
        line_color="#D85A30",
        line_width=2,
        annotation_text=f"10-day VWAP ${vwap_10d:.2f}",
        annotation_position="top right",
        annotation_font=dict(size=11, color="#D85A30"),
    )

    # Per-session rolling 10-day VWAP segments
    # Each segment spans that session's time range at the 10-day VWAP level
    # as it stood at that session's close — shows how the 10-day VWAP drifts over time.
    first_rolling_trace = True
    for date in session_dates:
        vwap = _rolling_vwap_by_date.get(date)
        if vwap is None or pd.isna(vwap):
            continue
        sess = running_df[running_df["_date"] == date]
        fig.add_trace(
            go.Scatter(
                x=[sess["timestamp_et"].iloc[0], sess["timestamp_et"].iloc[-1]],
                y=[float(vwap), float(vwap)],
                mode="lines",
                name="10-day VWAP (at close)" if first_rolling_trace else None,
                showlegend=first_rolling_trace,
                line=dict(color="#E8892A", width=2.5),
                hovertemplate=(
                    f"%{{x|%b %d}}<br>10-day VWAP at close: ${float(vwap):.2f}<extra></extra>"
                ),
            ),
            secondary_y=False,
        )
        first_rolling_trace = False

    # Alternating session background shading (odd-indexed sessions)
    for i, date in enumerate(session_dates):
        if i % 2 == 0:
            continue
        sess = running_df[running_df["_date"] == date]
        fig.add_vrect(
            x0=sess["timestamp_et"].iloc[0],
            x1=sess["timestamp_et"].iloc[-1],
            fillcolor="rgba(136, 135, 128, 0.06)",
            layer="below",
            line_width=0,
        )

    # Session open / close markers
    open_xs, open_ys, close_xs, close_ys = [], [], [], []
    for date in session_dates:
        sess        = running_df[running_df["_date"] == date]
        open_price  = _open_by_date.get(date)
        if open_price is not None:
            open_xs.append(sess["timestamp_et"].iloc[0])
            open_ys.append(float(open_price))
        close_xs.append(sess["timestamp_et"].iloc[-1])
        close_ys.append(float(sess["close"].iloc[-1]))

    fig.add_trace(
        go.Scatter(
            x=open_xs, y=open_ys,
            mode="markers",
            marker=dict(color="#3B6D11", size=8),
            showlegend=False,
            hovertemplate="%{x|%b %d %H:%M}<br>Open: $%{y:.2f}<extra></extra>",
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=close_xs, y=close_ys,
            mode="markers",
            marker=dict(color="#A32D2D", size=8),
            showlegend=False,
            hovertemplate="%{x|%b %d %H:%M}<br>Close: $%{y:.2f}<extra></extra>",
        ),
        secondary_y=False,
    )

    fig.update_layout(
        height=460,
        margin=dict(l=8, r=8, t=32, b=8),
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
        font=dict(
            family="-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
            size=12,
            color="#333333",
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        hovermode="x unified",
        xaxis=dict(gridcolor="#f0f0f0", showgrid=True, zeroline=False),
        yaxis=dict(
            title="Price (USD)",
            gridcolor="#f0f0f0",
            tickprefix="$",
            zeroline=False,
        ),
        yaxis2=dict(
            title="Volume",
            gridcolor="#f0f0f0",
            showgrid=False,
            zeroline=False,
        ),
    )

    fig.update_xaxes(
        rangebreaks=[
            dict(bounds=["sat", "mon"]),
            dict(values=market_holidays),
            dict(bounds=[16, 9.5], pattern="hour"),
        ]
    )

    chart_html = pio.to_html(
        fig, include_plotlyjs="cdn", full_html=False,
        config={"displayModeBar": False, "responsive": True},
    )

    # ── Trading table rows ────────────────────────────────────────────────
    any_low_fidelity = not summary_df["high_fidelity"].all()
    table_rows = ""
    for _, row in summary_df.iterrows():
        pct = row["pct_change"]
        if pd.isna(pct):
            pct_str, pct_cls = "—", ""
        elif float(pct) >= 0:
            pct_str, pct_cls = f"+{float(pct):.2f}%", "positive"
        else:
            pct_str, pct_cls = f"{float(pct):.2f}%", "negative"

        is_hf    = bool(row["high_fidelity"])
        vwap_str = fmt_price(row["daily_vwap"])
        if not is_hf:
            vwap_str = f'{vwap_str} <span class="hf-mark">*</span>'

        table_rows += f"""
          <tr>
            <td>{row['date']}</td>
            <td>{fmt_price(row['open'])}</td>
            <td>{fmt_price(row['close'])}</td>
            <td class="{pct_cls}">{pct_str}</td>
            <td>{fmt_vol(row['volume'])}</td>
            <td>{vwap_str}</td>
          </tr>"""

    table_footnote = ""
    if any_low_fidelity:
        table_footnote = (
            '<p class="footnote" style="margin-top:10px;">'
            "* Daily VWAP approximated from hourly bars (initial backfill). "
            "Replaced by minute-bar VWAP as archive accumulates over ~3 trading days."
            "</p>"
        )

    # ── Validation warning banner ─────────────────────────────────────────
    warning_banner = load_validation_warning()

    # ── Full HTML ─────────────────────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>YSS — York Space Systems Trading Dashboard</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      background: #f4f5f7;
      color: #1a1a1a;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
                   "Helvetica Neue", Arial, sans-serif;
      font-size: 15px;
      line-height: 1.5;
      padding: 0 20px 56px;
      max-width: 1100px;
      margin: 0 auto;
    }}
    a {{ color: #1a73e8; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}

    header {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 22px 0 14px;
      border-bottom: 2px solid #dde1e7;
      flex-wrap: wrap;
      gap: 8px;
    }}
    .header-left {{ display: flex; align-items: center; gap: 10px; }}
    header h1 {{ font-size: 21px; font-weight: 700; color: #0d0d0d; }}
    .badge {{
      background: #1a73e8; color: #fff;
      font-size: 11px; font-weight: 700;
      padding: 3px 9px; border-radius: 4px;
      letter-spacing: 0.08em;
    }}
    .timestamps {{ text-align: right; line-height: 1.55; }}
    .ts-data {{ font-size: 13px; color: #555; }}
    .ts-refresh {{ font-size: 12px; color: #aaa; }}

    .warning-banner {{
      background: #fff8e6;
      border: 1px solid #f5c518;
      border-left: 4px solid #f5c518;
      border-radius: 8px;
      padding: 14px 18px;
      margin: 16px 0 4px;
      font-size: 13.5px;
    }}
    .warning-banner ul {{ margin: 8px 0 8px 20px; }}
    .warning-banner li {{ margin: 4px 0; }}
    .warning-sub {{ font-size: 11.5px; color: #888; }}

    .metrics {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
      margin: 20px 0;
    }}
    .card {{
      background: #fff;
      border: 1px solid #dde1e7;
      border-radius: 10px;
      padding: 16px 18px;
    }}
    .card-label {{
      font-size: 10.5px; font-weight: 700;
      text-transform: uppercase; letter-spacing: 0.07em;
      color: #888; margin-bottom: 6px;
    }}
    .card-value {{ font-size: 26px; font-weight: 700; color: #0d0d0d; line-height: 1.1; }}
    .card-sub {{ font-size: 11.5px; color: #aaa; margin-top: 4px; }}

    .chart-wrap {{
      background: #fff; border: 1px solid #dde1e7;
      border-radius: 10px; padding: 14px 14px 6px;
      margin-bottom: 18px;
    }}
    .section-label {{
      font-size: 11px; font-weight: 700;
      text-transform: uppercase; letter-spacing: 0.07em;
      color: #888; margin-bottom: 8px;
    }}
    .chart-footnote {{
      font-size: 11px; color: #aaa;
      margin-top: 6px; padding: 0 2px;
    }}

    .table-wrap {{
      background: #fff; border: 1px solid #dde1e7;
      border-radius: 10px; overflow: hidden;
    }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13.5px; }}
    th {{
      background: #f8f9fb; text-align: left;
      padding: 10px 16px;
      font-size: 10.5px; font-weight: 700;
      text-transform: uppercase; letter-spacing: 0.07em;
      color: #666; border-bottom: 1px solid #dde1e7;
      white-space: nowrap;
    }}
    td {{ padding: 9px 16px; border-bottom: 1px solid #f0f2f5; white-space: nowrap; }}
    tbody tr:last-child td {{ border-bottom: none; }}
    tbody tr:hover td {{ background: #f8f9fb; }}
    .positive {{ color: #1a7f37; font-weight: 600; }}
    .negative {{ color: #cf1322; font-weight: 600; }}
    .hf-mark {{ color: #D85A30; font-size: 11px; font-weight: 700; }}
    tfoot tr td {{
      background: #f0f2f5; font-weight: 700;
      border-top: 2px solid #dde1e7; padding: 11px 16px;
      color: #0d0d0d;
    }}

    .footnote {{
      font-size: 11.5px; color: #aaa;
      margin-top: 18px; line-height: 1.7;
    }}
    footer {{
      margin-top: 32px; padding-top: 16px;
      border-top: 1px solid #dde1e7;
      font-size: 12px; color: #bbb;
      display: flex; justify-content: space-between;
      flex-wrap: wrap; gap: 8px;
    }}

    @media (max-width: 540px) {{
      .card-value {{ font-size: 20px; }}
      th, td {{ padding: 8px 10px; }}
    }}
  </style>
</head>
<body>

  <header>
    <div class="header-left">
      <h1>York Space Systems</h1>
      <span class="badge">NYSE: YSS</span>
    </div>
    <div class="timestamps">
      <div class="ts-data">Data through: {data_through_fmt}</div>
      <div class="ts-refresh">Page refreshed: {page_refreshed_fmt}</div>
    </div>
  </header>
  {warning_banner}
  <div class="metrics">
    <div class="card">
      <div class="card-label">Last Price</div>
      <div class="card-value">${last_price:.2f}</div>
      <div class="card-sub">USD per share</div>
    </div>
    <div class="card">
      <div class="card-label">10-Day VWAP</div>
      <div class="card-value">${vwap_10d:.2f}</div>
      <div class="card-sub">Volume-weighted avg price</div>
    </div>
    <div class="card">
      <div class="card-label">10-Day Range</div>
      <div class="card-value">${low_10d:.2f}&#8202;–&#8202;${high_10d:.2f}</div>
      <div class="card-sub">Low – High</div>
    </div>
    <div class="card">
      <div class="card-label">10-Day Avg Volume</div>
      <div class="card-value">{avg_vol_10d:,}</div>
      <div class="card-sub">Shares per session</div>
    </div>
  </div>

  <div class="chart-wrap">
    <div class="section-label">Price &amp; VWAP — 10 sessions</div>
    {chart_html}
    <p class="chart-footnote">Solid price line shows 1-minute bars where available; hourly bars on initial backfill sessions (marked in table).</p>
    <p class="chart-footnote">Green dots: session opens &nbsp;|&nbsp; Red dots: session closes &nbsp;|&nbsp; Orange segments: rolling 10-day VWAP as of each session close &nbsp;|&nbsp; Orange line: current 10-day VWAP.</p>
  </div>

  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Date</th><th>Open</th><th>Close</th>
          <th>% Change</th><th>Volume</th><th>Daily VWAP</th>
        </tr>
      </thead>
      <tbody>{table_rows}
      </tbody>
      <tfoot>
        <tr>
          <td colspan="5" style="text-align:right;font-size:10.5px;
              text-transform:uppercase;letter-spacing:.07em;
              color:#666;font-weight:600;">10-Day VWAP</td>
          <td>${vwap_10d:.2f}</td>
        </tr>
      </tfoot>
    </table>
  </div>
  {table_footnote}

  <p class="footnote">
    <strong>Methodology:</strong> VWAP&nbsp;=&nbsp;&Sigma;(Typical&nbsp;Price&nbsp;&times;&nbsp;Volume)&nbsp;/&nbsp;&Sigma;(Volume),
    where Typical&nbsp;Price&nbsp;=&nbsp;(High&nbsp;+&nbsp;Low&nbsp;+&nbsp;Close)&nbsp;/&nbsp;3 per 1-minute bar.
    Daily VWAP shown as orange segment per session; 10-day VWAP is the aggregate across all bars in the rolling archive.
    Open/close/volume in the table use Yahoo Finance reconciled daily bars; VWAP uses 1-minute intraday bars.
    All timestamps America/New_York&nbsp;(ET).
  </p>

  <footer>
    <span>YSS Trading Dashboard &middot; auto-refreshed Mon&ndash;Fri via GitHub Actions</span>
    <a href="https://kensielecki.github.io">&larr; kensielecki.github.io</a>
  </footer>

</body>
</html>
"""

    os.makedirs("docs", exist_ok=True)
    with open("docs/index.html", "w", encoding="utf-8", newline="\n") as f:
        f.write(html)
    print("Wrote docs/index.html")


if __name__ == "__main__":
    main()
