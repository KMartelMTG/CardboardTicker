#!/usr/bin/env python3
"""
daily.py — One daily run does everything:

  1. Download Scryfall bulk prices (every MTG printing, USD/foil/etched)
  2. Append today's prices to data/history.parquet
  3. Compute 1d/7d/30d % changes + 30-day z-scores (DuckDB)
  4. Export static site data into docs/data/ (search index, movers,
     90-day history shards) for GitHub Pages
  5. Post up/down spike alerts to Discord (DISCORD_WEBHOOK_URL)

Designed to run inside GitHub Actions (see .github/workflows/daily.yml),
but works identically on any machine: `python daily.py`.

Tunables (env):
    SITE_MIN_PRICE=0.25   cards below this USD price are left out of the site
    ALERT_MIN_PRICE=1.0   ALERT_MIN_PCT=20   ALERT_MIN_Z=2.5
    ALERT_MIN_HISTORY=5   ALERT_TOP_N=15

Local testing only:
    MTG_FAKE_INPUT=path/to.parquet   skip the download, use a prepared long
                                     table (scryfall_id, finish, price + catalog cols)
    MTG_FAKE_DATE=2026-09-23         override "today"
"""

import datetime as dt
import json
import os
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

import duckdb
import gzip
import polars as pl
import requests

ROOT = Path(__file__).resolve().parent
HISTORY = ROOT / "data" / "history.parquet"
SITE = ROOT / "docs" / "data"

SITE_MIN_PRICE = float(os.environ.get("SITE_MIN_PRICE", "0.25"))
ALERT_MIN_PRICE = float(os.environ.get("ALERT_MIN_PRICE", "1.0"))
ALERT_MIN_PCT = float(os.environ.get("ALERT_MIN_PCT", "20"))
ALERT_MIN_Z = float(os.environ.get("ALERT_MIN_Z", "2.5"))
ALERT_MIN_HISTORY = int(os.environ.get("ALERT_MIN_HISTORY", "5"))
ALERT_TOP_N = int(os.environ.get("ALERT_TOP_N", "15"))
WEBHOOK = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()

HEADERS = {"User-Agent": "mtg-price-tracker/2.0", "Accept": "application/json"}
FINISHES = {"usd": "nonfoil", "usd_foil": "foil", "usd_etched": "etched"}

CATALOG_COLS = [
    "scryfall_id", "name", "set_code", "set_name", "collector_number", "rarity"
]


def today() -> dt.date:
    fake = os.environ.get("MTG_FAKE_DATE")
    return dt.date.fromisoformat(fake) if fake else dt.datetime.now(dt.timezone.utc).date()


# ---------------------------------------------------------------- ingest ----

def fetch_today() -> pl.DataFrame:
    """Long table for today: catalog cols + finish + price (non-null only)."""
    fake = os.environ.get("MTG_FAKE_INPUT")
    if fake:
        return pl.read_parquet(fake)

    r = requests.get("https://api.scryfall.com/bulk-data", headers=HEADERS, timeout=60)
    r.raise_for_status()
    entry = next(e for e in r.json()["data"] if e["type"] == "default_cards")
    # Scryfall retired download_uri (July 2026); bulk files are now .jsonl.gz
    uri = entry.get("jsonl_download_uri") or entry.get("download_uri")
    if not uri:
        raise RuntimeError(f"No download URI on bulk entry; keys: {sorted(entry)}")

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".bulk")
    print(f"Downloading {uri} ...")
    with requests.get(uri, headers=HEADERS, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        for chunk in resp.iter_content(chunk_size=1 << 20):
            tmp.write(chunk)
    tmp.close()
    try:
        return parse_bulk(Path(tmp.name))
    finally:
        Path(tmp.name).unlink(missing_ok=True)


def parse_bulk(path: Path) -> pl.DataFrame:
    """Parse a Scryfall bulk file: gzipped JSONL (current) or plain JSON/JSONL."""
    with open(path, "rb") as f:
        is_gzip = f.read(2) == b"\x1f\x8b"
    opener = gzip.open if is_gzip else open
    rows = []
    with opener(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip().rstrip(",")
            if not line or line in ("[", "]"):
                continue
            card = json.loads(line)
            prices = card.get("prices") or {}
            base = {
                "scryfall_id": card.get("id"),
                "name": card.get("name"),
                "set_code": card.get("set"),
                "set_name": card.get("set_name"),
                "collector_number": card.get("collector_number"),
                "rarity": card.get("rarity"),
            }
            for field, finish in FINISHES.items():
                v = prices.get(field)
                if v is not None:
                    rows.append({**base, "finish": finish, "price": float(v)})
    return pl.DataFrame(rows, schema_overrides={"price": pl.Float64})


def append_history(today_df: pl.DataFrame, day: dt.date) -> pl.DataFrame:
    new = today_df.select("scryfall_id", "finish", "price").with_columns(
        pl.lit(day).alias("snapshot_date")
    )
    if HISTORY.exists():
        hist = pl.read_parquet(HISTORY)
        if day in hist["snapshot_date"].unique().to_list():
            print(f"{day} already in history — re-exporting site data only.")
            return hist
        hist = pl.concat([hist, new])
    else:
        hist = new
    HISTORY.parent.mkdir(parents=True, exist_ok=True)
    hist.write_parquet(HISTORY, compression="zstd")
    return hist


# --------------------------------------------------------------- metrics ----

METRICS_SQL = f"""
WITH windowed AS (
    SELECT *,
        lag(price, 1)  OVER w AS price_1d_ago,
        lag(price, 7)  OVER w AS price_7d_ago,
        lag(price, 30) OVER w AS price_30d_ago,
        avg(price)         OVER trail AS mean_30,
        stddev_samp(price) OVER trail AS std_30,
        count(*)           OVER trail AS n_history
    FROM hist
    WINDOW
        w AS (PARTITION BY scryfall_id, finish ORDER BY snapshot_date),
        trail AS (PARTITION BY scryfall_id, finish ORDER BY snapshot_date
                  ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING)
)
SELECT
    c.*, wi.finish, wi.price,
    round(100.0 * (wi.price - wi.price_1d_ago)  / nullif(wi.price_1d_ago, 0), 1) AS pct_1d,
    round(100.0 * (wi.price - wi.price_7d_ago)  / nullif(wi.price_7d_ago, 0), 1) AS pct_7d,
    round(100.0 * (wi.price - wi.price_30d_ago) / nullif(wi.price_30d_ago, 0), 1) AS pct_30d,
    CASE WHEN wi.n_history >= {ALERT_MIN_HISTORY} AND wi.std_30 > 0.01
         THEN round((wi.price - wi.mean_30) / wi.std_30, 2) END AS z_30,
    wi.price_1d_ago
FROM windowed wi
JOIN catalog c USING (scryfall_id)
WHERE wi.snapshot_date = (SELECT max(snapshot_date) FROM hist)
"""


# ----------------------------------------------------------- site export ----

def export_site(con: duckdb.DuckDBPyConnection, hist: pl.DataFrame, day: dt.date) -> None:
    SITE.mkdir(parents=True, exist_ok=True)

    latest = con.sql(
        f"SELECT * FROM ({METRICS_SQL}) WHERE price >= {SITE_MIN_PRICE}"
    ).df()

    # Search index: compact arrays [id, name, SET, cn, rarity, finish, price]
    cards = [
        [r.scryfall_id, r.name, r.set_code, r.collector_number, r.rarity,
         r.finish, round(r.price, 2)]
        for r in latest.itertuples()
    ]
    (SITE / "cards.json").write_text(json.dumps(cards, separators=(",", ":")))

    # Movers: anything with meaningful movement, client filters/sorts
    movers = con.sql(f"""
        SELECT scryfall_id, name, set_code, collector_number, finish, price,
               pct_1d, pct_7d, pct_30d, z_30
        FROM ({METRICS_SQL})
        WHERE price >= {SITE_MIN_PRICE} AND (
              abs(coalesce(pct_1d,0))  >= 5  OR abs(coalesce(pct_7d,0))  >= 10
           OR abs(coalesce(pct_30d,0)) >= 15 OR abs(coalesce(z_30,0))    >= 2)
        ORDER BY abs(coalesce(pct_1d, 0)) DESC
        LIMIT 3000
    """).df()
    movers_rows = [
        [r.scryfall_id, r.name, r.set_code, r.collector_number, r.finish,
         round(r.price, 2),
         None if r.pct_1d != r.pct_1d else r.pct_1d,      # NaN -> null
         None if r.pct_7d != r.pct_7d else r.pct_7d,
         None if r.pct_30d != r.pct_30d else r.pct_30d,
         None if r.z_30 != r.z_30 else r.z_30]
        for r in movers.itertuples()
    ]
    (SITE / "movers.json").write_text(json.dumps(movers_rows, separators=(",", ":")))

    # 90-day history shards keyed by first 2 hex chars of scryfall_id
    site_ids = set(latest["scryfall_id"])
    cutoff = day - dt.timedelta(days=90)
    h90 = hist.filter(
        (pl.col("snapshot_date") >= cutoff) & pl.col("scryfall_id").is_in(site_ids)
    ).sort("snapshot_date")
    shards: dict[str, dict[str, list]] = defaultdict(dict)
    for (sid, finish), grp in h90.group_by(["scryfall_id", "finish"]):
        shards[sid[:2]][f"{sid}|{finish}"] = [
            [d.isoformat(), p] for d, p in zip(grp["snapshot_date"], grp["price"])
        ]
    hist_dir = SITE / "history"
    hist_dir.mkdir(exist_ok=True)
    for prefix, series in shards.items():
        (hist_dir / f"{prefix}.json").write_text(json.dumps(series, separators=(",", ":")))

    n_days = hist["snapshot_date"].n_unique()
    (SITE / "meta.json").write_text(json.dumps({
        "updated": day.isoformat(),
        "days_tracked": n_days,
        "printings": len(cards),
        "z_active": n_days >= ALERT_MIN_HISTORY,
    }))
    print(f"Site export: {len(cards):,} printings, {len(movers_rows):,} movers, "
          f"{len(shards)} history shards, {n_days} day(s) tracked.")


# ---------------------------------------------------------------- alerts ----

def send_discord(content: str) -> None:
    if not WEBHOOK:
        return
    chunk = ""
    for line in content.splitlines(keepends=True):
        if len(chunk) + len(line) > 1900:
            requests.post(WEBHOOK, json={"content": chunk}, timeout=30)
            chunk = ""
        chunk += line
    if chunk.strip():
        requests.post(WEBHOOK, json={"content": chunk}, timeout=30)


def alerts(con: duckdb.DuckDBPyConnection, n_days: int) -> None:
    if n_days < 2:
        print("Fewer than 2 days of history — alerts skipped.")
        return
    base = f"""
        SELECT name, set_code, collector_number, finish, price, price_1d_ago,
               pct_1d, z_30
        FROM ({METRICS_SQL})
        WHERE price >= {ALERT_MIN_PRICE} AND price_1d_ago >= 0.25
          AND (abs(pct_1d) >= {ALERT_MIN_PCT}
               OR (abs(coalesce(z_30, 0)) >= {ALERT_MIN_Z} AND abs(pct_1d) >= 5))
    """
    for direction, order in (("up", "DESC"), ("down", "ASC")):
        sign = ">" if direction == "up" else "<"
        rows = con.sql(
            base + f" AND pct_1d {sign} 0 ORDER BY pct_1d {order} LIMIT {ALERT_TOP_N}"
        ).fetchall()
        if not rows:
            continue
        arrow = "📈" if direction == "up" else "📉"
        lines = [f"**{arrow} MTG price movers — {direction.upper()}**"]
        for name, sc, cn, finish, price, prev, pct, z in rows:
            z_txt = f", z={z:+.1f}" if z is not None else ""
            lines.append(f"`{sc.upper()} #{cn}` **{name}** ({finish}) "
                         f"${prev:.2f} → ${price:.2f} ({pct:+.1f}%{z_txt})")
        msg = "\n".join(lines)
        print(msg)
        send_discord(msg)


# ------------------------------------------------------------------ main ----

def main() -> int:
    day = today()
    today_df = fetch_today()
    print(f"Fetched {today_df.height:,} priced printing/finish rows for {day}.")

    hist = append_history(today_df, day)
    catalog = today_df.select(CATALOG_COLS).unique(subset=["scryfall_id"])

    con = duckdb.connect()
    con.register("hist", hist.to_arrow())
    con.register("catalog", catalog.to_arrow())

    export_site(con, hist, day)
    alerts(con, hist["snapshot_date"].n_unique())
    return 0


if __name__ == "__main__":
    sys.exit(main())
