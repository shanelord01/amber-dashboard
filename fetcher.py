import requests
from datetime import datetime, timedelta
from models import db, Interval

API_BASE = "https://api.amber.com.au/v1"


def get_headers(api_key):
    return {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "User-Agent": "Amber-Dashboard/1.0"
    }


def get_site_id(api_key):
    """Discover Amber site_id automatically by checking which one has real usage data."""
    url = f"{API_BASE}/sites"
    try:
        r = requests.get(url, headers=get_headers(api_key))
        r.raise_for_status()
        sites = r.json()
    except Exception as e:
        print(f"[Amber] Failed to retrieve sites: {e}")
        return None

    if not sites or not isinstance(sites, list):
        print("[Amber] No sites returned from API")
        return None

    # Check each site for real usage
    for site in sites:
        site_id = site.get("id")
        if not site_id:
            continue

        test_start = (datetime.utcnow().date() - timedelta(days=7)).isoformat()
        test_end = datetime.utcnow().date().isoformat()
        usage_url = (
            f"{API_BASE}/sites/{site_id}/usage"
            f"?channelType=general&startDate={test_start}&endDate={test_end}"
        )

        try:
            resp = requests.get(usage_url, headers=get_headers(api_key))
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and len(data) > 0:
                    print(f"[Amber] Found active site ID: {site_id}")
                    return site_id
        except Exception as e:
            print(f"[Amber] Site {site_id} check failed: {e}")

    # fallback if none returned data
    print("[Amber] No active sites returned usage data; using first site as fallback.")
    return sites[0].get("id")


def fetch_usage(api_key, site_id, channel_type, start_date, end_date):
    """Fetch usage data for one channel and date range."""
    url = (
        f"{API_BASE}/sites/{site_id}/usage"
        f"?channelType={channel_type}&startDate={start_date}&endDate={end_date}"
    )
    try:
        r = requests.get(url, headers=get_headers(api_key))
        if r.status_code != 200:
            print(f"[Amber] {channel_type} fetch failed {r.status_code}: {r.text}")
            return []
        data = r.json()
        if isinstance(data, list):
            print(f"[Amber] {channel_type} returned {len(data)} records for {start_date} → {end_date}")
            return data
        print(f"[Amber] {channel_type} unexpected type: {type(data)}")
        return []
    except Exception as e:
        print(f"[Amber] Error fetching {channel_type}: {e}")
        return []


def pull_once(user, days_back=30):
    """Pull and store interval data for one user in rolling 7-day batches."""
    if not user.api_key:
        return {"status": "error", "error": "Missing API key"}

    # Auto-discover only if not already set
    site_id = user.site_id or get_site_id(user.api_key)
    if not site_id:
        return {"status": "error", "error": "Missing site_id or API key"}

    print(f"[Amber] Using site ID: {site_id}")

    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=days_back)
    print(f"[Amber] Fetching usage {start_date} → {end_date} in 7-day batches")

    all_general, all_feed_in = [], []
    cursor = end_date

    while cursor > start_date:
        batch_end = cursor
        batch_start = max(start_date, batch_end - timedelta(days=7))
        cursor = batch_start

        general = fetch_usage(user.api_key, site_id, "general", batch_start, batch_end)
        feed_in = fetch_usage(user.api_key, site_id, "feedIn", batch_start, batch_end)
        all_general.extend(general)
        all_feed_in.extend(feed_in)

    print(f"[Amber] Total fetched: {len(all_general)} general, {len(all_feed_in)} feedIn")

    feed_in_map = {f["date"]: f for f in all_feed_in if "date" in f}
    count = 0

    for g in all_general:
        try:
            ts = datetime.fromisoformat(g["date"].replace("Z", "+00:00"))
            import_kwh = float(g.get("kwh") or g.get("usageKwh") or 0)
            import_cost = float(g.get("cost") or g.get("costValue") or 0)

            f = feed_in_map.get(g["date"], {})
            export_kwh = float(f.get("kwh") or f.get("usageKwh") or 0)
            export_cost = float(f.get("cost") or f.get("costValue") or 0)

            existing = Interval.query.filter_by(ts=ts).first()
            if existing:
                existing.import_kwh = import_kwh
                existing.export_kwh = export_kwh
                existing.cost = import_cost - export_cost
            else:
                interval = Interval(
                    ts=ts,
                    import_kwh=import_kwh,
                    export_kwh=export_kwh,
                    cost=import_cost - export_cost
                )
                db.session.add(interval)

            count += 1
        except Exception as e:
            print(f"[Amber] Error processing record: {e}")
            db.session.rollback()

    try:
        db.session.commit()
    except Exception as e:
        print(f"[Amber] Database commit failed: {e}")
        db.session.rollback()
        return {"status": "error", "error": str(e)}

    print(f"[Amber] Stored {count} intervals.")
    return {"status": "ok", "count": count}
