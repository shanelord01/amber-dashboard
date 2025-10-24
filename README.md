# amber-dashboard

A small Flask app that pulls Amber Electric usage & price data, aggregates it, and lets you run solar/battery “what‑if” simulations.

## Features
- Paste **your own Amber API key** (per-user storage can be added later)
- Auto-discovers **Site ID** from `/sites` (or you can paste it)
- Pulls last 30 days of intervals and prices, stores in SQLite
- Shows **daily, monthly, quarterly, yearly** cost buckets
- Simple PV + battery model to estimate bill impact

## Setup

1. Copy `.env.example` → `.env` and set `FLASK_SECRET` (and optionally `SITE_ID`)
2. Run with Docker compose (recommended) or bare Python
3. Open the app, go to **Settings**, paste your Amber API token, **Save**
4. Click **Pull Now** or wait for the background scheduler

## Pangolin / Reverse Proxy
Put this behind Pangolin for login & ACL. The app itself does not store passwords.

## Caveats
- Amber API history is limited (often ~90 days) – the app keeps its own history
- Interval schema can vary a little; adjust parsing in `fetcher.py`
- The what‑if model is a simple heuristic – improve with your own PV curves, BOM data, or your inverter’s real production
