#!/usr/bin/env python3
"""
backfill.py — One-time backfill of ~90 days of price history from MTGJSON.

Pulls MTGJSON AllPrices (90 days of daily prices per card) and AllIdentifiers
(uuid -> scryfallId mapping), extracts the TCGplayer retail series (same
market-price lineage as Scryfall's usd field), and merges it into
data/history.parquet — existing snapshot rows always win on conflicts, so
running this after daily snapshots have accumulated is safe and idempotent.

Run once via the Backfill workflow (Actions -> "Backfill 90-day history"
-> Run workflow), or locally:

    pip install -r requirements.txt ijson
    python backfill.py

Tunables (env):
    BACKFILL_MIN_PRICE=0.25   drop series that never reach this price
                              (keeps the parquet small; junk commons excluded)
Local testing only:
    BACKFILL_PRICES_FILE / BACKFILL_IDENTIFIERS_FILE   use local files
"""

import gzip
import os
import sys
import tempfile
from pathlib import Path

import ijson
import polars as pl
import requests

ROOT = Path(__file__).resolve().parent
HISTORY = ROOT / "data" / "history.parquet"

PRICES_URL = "https://mtgjson.com/api/v5/AllPrices.json.gz"
IDENTIFIERS_URL = "https://mtgjson.com/api/v5/AllIdentifiers.json.gz"
HEADERS = {"User-Agent": "mtg-price-tracker/2.0", "Accept": "application/json"}

MIN_PRICE = float(os.environ.get("BACKFILL_MIN_PRICE", "0.25"))
FINISH_MAP = {"normal": "nonfoil", "foil": "foil", "etched": "etched"}
CHUNK_ROWS = 3_000_000


def download(url: str, env_override: str) -> Path:
    local = os.environ.get(env_override)
    if local:
        return Path(local)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json.gz")
    print(f"Downloading {url} ...")
    with requests.get(url, headers=HEADERS, stream=True, timeout=120) as r:
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=1 << 20):
            tmp.write(chunk)
    tmp.close()
    print(f"  {Path(tmp.name).stat().st_size / 1e6:.0f} MB")
    return Path(tmp.name)


def open_maybe_gzip(path: Path):
    with open(path, "rb") as f:
        magic = f.read(2)
    return gzip.open(path, "rb") if magic == b"\x1f\x8b" else open(path, "rb")


def build_uuid_map(path: Path) -> dict[str, str]:
    print("Building uuid -> scryfallId map ...")
    mapping = {}
    with open_maybe_gzip(path) as f:
        for uuid, card in ijson.kvitems(f, "data"):
            sid = (card.get("identifiers") or {}).get("scryfallId")
            if sid:
                mapping[uuid] = sid
    print(f"  {len(mapping):,} uuids mapped")
    return mapping


def extract_prices(path: Path, uuid_map: dict[str, str], workdir: Path) -> list[Path]:
    print("Extracting TCGplayer retail series ...")
    chunks: list[Path] = []
    ids: list[str] = []; fins: list[str] = []; dates: list[str] = []; px: list[float] = []

    def flush():
        nonlocal ids, fins, dates, px
        if not ids:
            return
        df = pl.DataFrame({
            "scryfall_id": ids, "finish": fins,
            "snapshot_date": pl.Series(dates).str.to_date(),
            "price": pl.Series(px, dtype=pl.Float64),
        })
        out = workdir / f"chunk_{len(chunks):03d}.parquet"
        df.write_parquet(out, compression="zstd")
        chunks.append(out)
        ids, fins, dates, px = [], [], [], []

    with open_maybe_gzip(path) as f:
        for uuid, entry in ijson.kvitems(f, "data"):
            sid = uuid_map.get(uuid)
            if not sid:
                continue
            retail = ((entry.get("paper") or {}).get("tcgplayer") or {}).get("retail") or {}
            for fkey, finish in FINISH_MAP.items():
                for d, p in (retail.get(fkey) or {}).items():
                    ids.append(sid); fins.append(finish)
                    dates.append(d); px.append(float(p))
            if len(ids) >= CHUNK_ROWS:
                flush()
    flush()
    print(f"  {len(chunks)} chunk file(s) written")
    return chunks


def merge(chunks: list[Path]) -> None:
    lazy = pl.concat([pl.scan_parquet(c) for c in chunks])
    # Drop series that never reach MIN_PRICE (keeps the repo parquet small)
    keep = lazy.group_by("scryfall_id", "finish").agg(
        pl.col("price").max().alias("mx")
    ).filter(pl.col("mx") >= MIN_PRICE).select("scryfall_id", "finish")
    new = (
        lazy.join(keep, on=["scryfall_id", "finish"], how="inner")
        .unique(subset=["scryfall_id", "finish", "snapshot_date"], keep="last")
        .collect()
    )
    print(f"Backfill rows after filtering: {new.height:,}")

    if HISTORY.exists():
        existing = pl.read_parquet(HISTORY)
        # Existing daily snapshots win: drop backfill rows for covered key+date
        new = new.join(
            existing.select("scryfall_id", "finish", "snapshot_date"),
            on=["scryfall_id", "finish", "snapshot_date"],
            how="anti",
        )
        print(f"Rows added beyond existing history: {new.height:,}")
        merged = pl.concat([existing, new.select(existing.columns)])
    else:
        merged = new

    merged = merged.sort(["scryfall_id", "finish", "snapshot_date"])
    HISTORY.parent.mkdir(parents=True, exist_ok=True)
    merged.write_parquet(HISTORY, compression="zstd")
    lo, hi = merged["snapshot_date"].min(), merged["snapshot_date"].max()
    print(f"history.parquet: {merged.height:,} rows, {lo} to {hi}, "
          f"{HISTORY.stat().st_size / 1e6:.1f} MB")


def main() -> int:
    ids_path = download(IDENTIFIERS_URL, "BACKFILL_IDENTIFIERS_FILE")
    prices_path = download(PRICES_URL, "BACKFILL_PRICES_FILE")
    uuid_map = build_uuid_map(ids_path)
    with tempfile.TemporaryDirectory() as td:
        chunks = extract_prices(prices_path, uuid_map, Path(td))
        if not chunks:
            print("No price series extracted — check MTGJSON format.")
            return 1
        merge(chunks)
    return 0


if __name__ == "__main__":
    sys.exit(main())
