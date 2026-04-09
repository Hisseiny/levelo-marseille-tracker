# Le Velo Marseille Tracker

Real-time monitoring dashboard and data collector for Marseille's bike-sharing system (Le Velo).

**[Live Dashboard](https://hisseiny.github.io/levelo-marseille-tracker/)**

## What it does

- **Data collector** (`fetch_levelo_data.py`) -- Runs every 10 minutes via GitHub Actions, pulls station data from the GBFS API, stores it in Supabase for historical tracking
- **Dashboard** (`docs/index.html`) -- Live single-page dashboard showing an interactive map, station availability, and system stats. Fetches directly from the public GBFS API.

## Dashboard features

- Interactive map with color-coded station markers (green/amber/red by availability)
- Real-time stats: total bikes, active stations, system availability, empty stations
- Searchable and sortable station table
- 24h trend chart (requires Supabase connection)
- Auto-refreshes every 60 seconds

## Setup

### Data collector

Requires `SUPABASE_URL` and `SUPABASE_KEY` environment variables (configured as GitHub Secrets).

```bash
pip install -r requirements.txt
SUPABASE_URL=... SUPABASE_KEY=... python fetch_levelo_data.py
```

### Dashboard

The dashboard works out of the box with zero configuration -- it fetches live data directly from the public GBFS API. To enable the historical trend chart, set your Supabase credentials in `docs/index.html`.

Hosted via GitHub Pages from the `docs/` folder.

## Architecture

```
GBFS API (Omega) ──> fetch_levelo_data.py ──> Supabase (historical)
       │
       └──> docs/index.html (live dashboard, direct API fetch)
                  │
                  └──> Supabase (trend chart, read-only)
```

## Data source

[GBFS Omega Fifteen - Marseille](https://gbfs.omega.fifteen.eu/gbfs/2.2/marseille/en)
