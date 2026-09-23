# The Cardboard Ticker — self-hosting MTG price tracker

A GitHub repo that runs itself. No server, no Fabric, no cost on a public repo:

- **GitHub Actions** runs the daily pipeline (`daily.py`): downloads Scryfall
  prices, appends to `data/history.parquet`, computes 1d/7d/30d moves and
  30-day z-scores, posts spike alerts (up *and* down) to Discord, and commits
  the results back to the repo.
- **GitHub Pages** serves `docs/` as a static website: searchable card index,
  price-history charts, and a filterable movers board. No backend — the site
  reads pre-computed JSON that the Action publishes each morning.

## Setup (one time, ~5 minutes)

1. **Create a new GitHub repo** (public = unlimited free Actions minutes) and
   push this folder to it:
   ```bash
   git init && git add -A && git commit -m "initial"
   git branch -M main
   git remote add origin git@github.com:YOURNAME/mtg-tracker.git
   git push -u origin main
   ```
2. **Enable Pages:** repo → Settings → Pages → Source: *Deploy from a branch*
   → Branch `main`, folder `/docs`. Your site will be
   `https://YOURNAME.github.io/mtg-tracker/`.
3. **Add the Discord webhook (optional):** repo → Settings → Secrets and
   variables → Actions → New repository secret →
   name `DISCORD_WEBHOOK_URL`, value = your webhook URL
   (Discord: Server Settings → Integrations → Webhooks).
4. **Seed the first snapshot:** repo → Actions → *Daily price snapshot* →
   *Run workflow*. It takes ~10–15 minutes (the Scryfall bulk file is ~2 GB).
   After it finishes, the site is live.

From then on it runs at 06:15 UTC daily. Percent-move alerts activate on
day 2; z-scores activate after 5 snapshots.

## Tuning

Alert thresholds are env vars in `daily.py` — edit the workflow's run step to
override, e.g. `ALERT_MIN_PCT: "15"`. `SITE_MIN_PRICE` (default $0.25)
controls which cards appear on the website at all.

## Local run / testing

```bash
pip install -r requirements.txt
python daily.py                     # real download, writes data/ and docs/data/
python -m http.server -d docs 8000  # preview at http://localhost:8000
```

## Growth and limits worth knowing

- **Repo size:** history grows a few MB/day compressed; the `docs/data/`
  rewrite each day also adds git history. Expect ~1–2 GB of repo after a
  year. When it gets heavy, squash old snapshot commits or move
  `data/history.parquet` to Cloudflare R2 and have the Action pull/push it —
  the pipeline doesn't care where the parquet lives.
- **Site data:** the search index (`cards.json`) covers every printing at
  $0.25+; the movers feed carries up to 3,000 rows; charts show the last
  90 days (full history stays in the parquet).
- **Scheduled-workflow sleep:** GitHub pauses schedules on repos with no
  activity for 60 days — the daily bot commit itself counts as activity, so
  this only matters if the pipeline breaks. Pair the Action with a free
  healthchecks.io ping if you want a dead-man's switch.
- **Next steps:** MTGJSON `AllPricesToday` for Card Kingdom / buylist prices
  (leading indicator), sealed products via a paid aggregator API.
