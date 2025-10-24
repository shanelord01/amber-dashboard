import requests
from datetime import datetime, timedelta
from models import db, Interval
import json

API_BASE = "https://api.amber.com.au/v1"

def get_headers(api_key):
    return {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "User-Agent": "Amber-Dashboard/1.0"
    }

def get_site_id(api_key):
    """Discover Amber site_id automatically"""
    url = f"{API_BASE}/sites"
    r = requests.get(url, headers=get_headers(api_key))
    r.raise_for_status()
    sites = r.json()
    if sites and isinstance(sites, list):
        site_id = sites[0].get("id")
        print(f"[Amber] Auto-discovered site ID: {site_id}")
        return site_id
    return None


def pull_once(user):
    """Pull and store interval data for one user"""
    if not user.api_key:
        return {"status": "error", "error": "Missing API key"}

    site_id = user.site_id or get_site_id(user.api_key)
    if not site_id:
        return {"status": "error", "error": "Missing site_id or API key"}

    # Date range: last 7 days
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=7)
    url = f"{API_BASE}/sites/{site_id}/usage?startDate={start_date}&endDate={end_date}"
    print(f"[Amber] Fetching usage {start_date} → {end_date}")

    r = requests.get(url, headers=get_headers(user.api_key))
    if r.status_code != 200:
        print(f"[Amber] Error {r.status_code}: {r.text}")
        return {"status": "error", "error": r.text}

    data = r.json()

    # DEBUG: show sample
    print("[Amber] Sample record from API:")
    print(json.dumps(data[:3] if isinstance(data, list) else data, indent=2)[:1500])

    # Handle new (list) or old (dict) formats
    general, feed_in = [], []

    if isinstance(data, list):
        # New format: list of channels
        for ch in data:
            ctype = ch.get("channelType", "").lower()
            if "general" in ctype or "consumption" in ctype:
                general.extend(ch.get("usage", []))
            elif "feedin" in ctype or "export" in ctype:
                feed_in.extend(ch.get("usage", []))
    elif isinstance(data, dict):
        general = data.get("general", [])
        feed_in = data.get("feedIn", [])
    else:
        print("[Amber] Unexpected API structure.")
        return {"status": "error", "error": "Unexpected API response type"}

    print(f"[Amber] Received {len(general)} records for general")
    print(f"[Amber] Received {len(feed_in)} records for feedIn")

    # Map feed-in by timestamp
    feed_in_map = {f["date"]: f for f in feed_in if "date" in f}

    count = 0
    for g in general:
        try:
            ts = datetime.fromisoformat(g["date"].replace("Z", "+00:00"))
            import_kwh = float(g.get("kwh") or g.get("usageKwh") or 0)
            import_cost = float(g.get("cost") or g.get("costValue") or 0)

            f = feed_in_map.get(g["date"], {})
            export_kwh = float(f.get("kwh") or f.get("usageKwh") or 0)
            export_cost = float(f.get("cost") or f.get("costValue") or 0)

            interval = Interval(
                ts=ts,
                import_kwh=import_kwh,
                export_kwh=export_kwh,
                cost=import_cost - export_cost
            )
            db.session.merge(interval)
            count += 1

        except Exception as e:
            print(f"[Amber] Error processing record: {e}")

    db.session.commit()
    print(f"[Amber] Stored {count} intervals.")
    return {"status": "ok", "count": count}
