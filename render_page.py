import glob
import os
import sys
import pytz
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots
from dateutil import parser as dtparser

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


def main():
    summary_df  = pd.read_csv(latest_file("output/*_daily_summary.tsv"), sep="\t")
    running_df  = pd.read_csv(latest_file("output/*_running_vwap.tsv"),   sep="\t")
    headline_df = pd.read_csv(latest_file("output/*_headline_metrics.tsv"), sep="\t")

    running_df["timestamp_et"] = (
        pd.to_datetime(running_df["timestamp_et"], utc=True).dt.tz_convert(ET)
    )

    m = headline_df.iloc[0]
    last_price     = float(m["last_price"])
    vwap_5d        = float(m["vwap_5d"])
    high_5d        = float(m["high_5d"])
    low_5d         = float(m["low_5d"])
    avg_vol_5d     = int(m["avg_volume_5d"])
    last_updated_raw = str(m["last_updated"])

    try:
        lu = dtparser.parse(last_updated_raw)
        h    = lu.hour % 12 or 12
        ampm = "AM" if lu.hour < 12 else "PM"
        last_updated_fmt = f"{h}:{lu.minute:02d} {ampm} ET, {lu.strftime('%b %d, %Y')}"
    except Exception:
        last_updated_fmt = last_updated_raw

    # ── Plotly chart (price + VWAP on primary y; volume bars on secondary y) ─
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Scatter(
            x=running_df["timestamp_et"],
            y=running_df["close"],
            mode="lines",
            name="Close",
            line=dict(color="#1a73e8", width=1.5),
            hovertemplate="%{x|%H:%M}<br>Close: $%{y:.2f}<extra></extra>",
        ),
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(
            x=running_df["timestamp_et"],
            y=running_df["running_vwap"],
            mode="lines",
            name="Intraday VWAP",
            line=dict(color="#e8710a", width=1.5, dash="dot"),
            hovertemplate="%{x|%H:%M}<br>VWAP: $%{y:.2f}<extra></extra>",
        ),
        secondary_y=False,
    )

    fig.add_trace(
        go.Bar(
            x=running_df["timestamp_et"],
            y=running_df["volume"],
            name="Volume",
            marker_color="#bbd4f5",
            opacity=0.7,
            hovertemplate="%{x|%H:%M}<br>Vol: %{y:,.0f}<extra></extra>",
        ),
        secondary_y=True,
    )

    fig.add_hline(
        y=vwap_5d,
        line_dash="dash",
        line_color="#999999",
        line_width=1,
        annotation_text=f"5d VWAP ${vwap_5d:.2f}",
        annotation_position="top right",
        annotation_font=dict(size=11, color="#666666"),
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

    chart_html = pio.to_html(
        fig, include_plotlyjs="cdn", full_html=False,
        config={"displayModeBar": False, "responsive": True},
    )

    # ── Trading table rows ────────────────────────────────────────────────
    table_rows = ""
    for _, row in summary_df.iterrows():
        pct = row["pct_change"]
        if pd.isna(pct):
            pct_str, pct_cls = "—", ""
        elif float(pct) >= 0:
            pct_str, pct_cls = f"+{float(pct):.2f}%", "positive"
        else:
            pct_str, pct_cls = f"{float(pct):.2f}%", "negative"

        table_rows += f"""
          <tr>
            <td>{row['date']}</td>
            <td>{fmt_price(row['open'])}</td>
            <td>{fmt_price(row['close'])}</td>
            <td class="{pct_cls}">{pct_str}</td>
            <td>{fmt_vol(row['volume'])}</td>
            <td>{fmt_price(row['daily_vwap'])}</td>
          </tr>"""

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
    .updated {{ font-size: 13px; color: #888; }}

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
    <span class="updated">Updated {last_updated_fmt}</span>
  </header>

  <div class="metrics">
    <div class="card">
      <div class="card-label">Last Price</div>
      <div class="card-value">${last_price:.2f}</div>
      <div class="card-sub">USD per share</div>
    </div>
    <div class="card">
      <div class="card-label">5-Day VWAP</div>
      <div class="card-value">${vwap_5d:.2f}</div>
      <div class="card-sub">Volume-weighted avg price</div>
    </div>
    <div class="card">
      <div class="card-label">5-Day Range</div>
      <div class="card-value">${low_5d:.2f}&#8202;–&#8202;${high_5d:.2f}</div>
      <div class="card-sub">Low – High</div>
    </div>
    <div class="card">
      <div class="card-label">5-Day Avg Volume</div>
      <div class="card-value">{avg_vol_5d:,}</div>
      <div class="card-sub">Shares per session</div>
    </div>
  </div>

  <div class="chart-wrap">
    <div class="section-label">Price &amp; VWAP — 5 sessions · 1-minute bars</div>
    {chart_html}
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
              color:#666;font-weight:600;">5-Day VWAP</td>
          <td>${vwap_5d:.2f}</td>
        </tr>
      </tfoot>
    </table>
  </div>

  <p class="footnote">
    <strong>Methodology:</strong> VWAP&nbsp;=&nbsp;Σ(Typical&nbsp;Price&nbsp;×&nbsp;Volume)&nbsp;/&nbsp;Σ(Volume),
    where Typical&nbsp;Price&nbsp;=&nbsp;(High&nbsp;+&nbsp;Low&nbsp;+&nbsp;Close)&nbsp;/&nbsp;3 per 1-minute bar.
    Intraday VWAP resets at 09:30&nbsp;ET each session. 5-day VWAP covers all available 1-minute bars
    in the 5-session window. Yahoo Finance caps 1-min history at ~8 calendar days per request.
    Data via Yahoo Finance (yfinance). All timestamps America/New_York&nbsp;(ET).
  </p>

  <footer>
    <span>YSS Trading Dashboard · auto-refreshed Mon–Fri via GitHub Actions</span>
    <a href="https://kensielecki.github.io">← kensielecki.github.io</a>
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
