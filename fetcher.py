import os
import json
import pytz
import requests
from datetime import datetime, timedelta
from flask import current_app
from models import db, UserConfig, Interval

AEST = pytz.timezone("Australia/Sydney")

# Utility: parse Amber timestamps
def _parse_ts(val):
    if not val:
        return None
    try:
        if isinstance(val, str):
            if val.endswith("Z"):
                return datetime.fromisoformat(val.replace("Z", "+00:00"))
            return datetime.fromisoformat(val)
        return val
    except Exception:
        return None


def upsert_interval(ts: datetime, import_kwh: float, export_kwh: float, import_price: float, export_price: float, cost: float):
    row = Interval.query.filter_by(ts=ts).first()
    if not row:
        row = Interval(
            ts=ts,
            import_kwh=import_kwh,
            export_kwh=export_kwh,
            import_price=import_price,
            export_price=export_price,
            cost=cost,
            source="pull",
        )
        db.session.add(row)
    else:
        row.import_kwh = import_kwh
        row.export_kwh = export_kwh
        row.import_price = import_price
        row.export_price = export_price
        row.cost = cost
    return row


def fetch_from_api(base_url, site_id, api_key, start_date, end_date, channel_type):
    """Attempt to fetch usage data for one channel type from given base_url"""
    url = f"{base_url.rstrip('/')}/sites/{site_id}/usage"
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {
        "startDate": start_date,
        "endDate": end_date,
        "channelType": channel_type,
    }

    print(f"[fetcher] Trying {url} for channelType={channel_type}")
    try:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        print(f"[fetcher] Response {r.status_code}")
        if r.status_code != 200:
            return None, f"HTTP {r.status_code}: {r.text[:200]}"
        data = r.json()
        if isinstance(data, dict) and "data" in data:
            return data["data"], None
        if isinstance(data, list):
            return data, None
        return None, "Empty or unexpected response"
    except Exception as e:
        return None, str(e)


def fetch_from_batteries_api(site_id, api_key, start_date, end_date, channel_type):
    """Try Amber-for-Batteries variant"""
    url = "https://api.batteries.amber.com.au/v1/data/usage"
    params = {
        "api_key": api_key,
        "siteId": site_id,
        "startDate": start_date,
        "endDate": end_date,
        "channelType": channel_type,
    }

    print(f"[fetcher] Trying batteries API for channelType={channel_type}")
    try:
        r = requests.get(url, params=params, timeout=30)
        print(f"[fetcher] Batteries response {r.status_code}")
        if r.status_code != 200:
            return None, f"HTTP {r.status_code}: {r.text[:200]}"
        data = r.json()
        if isinstance(data, dict) and "data" in data:
            return data["data"], None
        if isinstance(data, list):
            return data, None
        return None, "Empty or unexpected batteries response"
    except Exception as e:
        return None, str(e)


def pull_once(now=None):
    """Pulls last 7 days of usage and prices. Works with both standard and batteries Amber APIs."""
    user = UserConfig.query.get(1)
    if not user or not user.api_key:
        print("[fetcher] No API key configured.")
        return {"status": "no-api-key"}

    api_key = user.api_key.strip()
    site_id = user.site_id or os.getenv("SITE_ID", "")
    if not site_id:
        print("[fetcher] No site ID.")
        return {"status": "no-site"}

    base_url = current_app.config.get("AMBER_BASE_URL", "https://api.amber.com.au/v1")
    if now is None:
        now = datetime.now(AEST)
    start = (now - timedelta(days=7)).date().isoformat()
    end = now.date().isoformat()

    print(f"[fetcher] Pulling from {start} to {end} for site {site_id}")
    print(f"[fetcher] Base URL: {base_url}")

    all_records = []

    # Try both channel types
    for channel in ["general", "feedIn"]:
        data, err = fetch_from_api(base_url, site_id, api_key, start, end, channel)
        if not data:
            print(f"[fetcher] Primary API failed for {channel}: {err}")
            data, err = fetch_from_batteries_api(site_id, api_key, start, end, channel)
            if not data:
                print(f"[fetcher] Batteries API also failed for {channel}: {err}")
                continue
        print(f"[fetcher] Retrieved {len(data)} intervals for {channel}")
        for rec in data:
            ts = _parse_ts(rec.get("startTime") or rec.get("intervalStart"))
            if not ts:
                continue
            kwh = float(rec.get("kwh", 0.0))
            per_kwh = float(rec.get("perKwh", 0.0))
            cost = float(rec.get("cost", 0.0))
            if channel == "general":
                import_kwh = kwh
                export_kwh = 0.0
                import_price = per_kwh
                export_price = 0.0
            else:
                import_kwh = 0.0
                export_kwh = kwh
                import_price = 0.0
                export_price = abs(per_kwh)
            upsert_interval(ts, import_kwh, export_kwh, import_price, export_price, cost)
            all_records.append(rec)

    db.session.commit()
    print(f"[fetcher] Saved {len(all_records)} intervals to DB.")
    return {"status": "ok", "count": len(all_records)}
