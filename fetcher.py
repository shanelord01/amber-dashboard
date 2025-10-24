import requests
from datetime import datetime, timedelta
from models import db, Interval
import json


def fetch_usage(api_key, site_id, channel_type, start_date, end_date):
    """Fetch usage data from Amber API for a date range and channel type."""
    url = f"https://api.amber.com.au/v1/sites/{site_id}/usage"
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {
        "channelType": channel_type,
        "startDate": start_date.strftime("%Y-%m-%d"),
        "endDate": end_date.strftime("%Y-%m-%d"),
    }

    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code != 200:
        print(f"[Amber] Error fetching {channel_type}: {resp.status_code} {resp.text}")
        return []

    try:
        data = resp.json()
    except Exception as e:
        print(f"[Amber] JSON decode error: {e}")
        return []

    # Amber returns a list of dicts; return directly
    return data if isinstance(data, list) else []


def auto_discover_site_id(api_key):
    """Fetch user's site ID from Amber API."""
    url = "https://api.amber.com.au/v1/sites"
    headers = {"Authorization": f"Bearer {api_key}"}
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        print(f"[Amber] Error discovering site: {resp.status_code} {resp.text}")
        return None
    sites = resp.json()
    if isinstance(sites, list) and len(sites) > 0:
        print(f"[Amber] Found active site ID: {sites[0].get('id')}")
        return sites[0].get("id")
    print("[Amber] No active sites found.")
    return None


def pull_once(user):
    """Pull and store usage data in batches of up to 7 days."""
    api_key = user.api_key
    site_id = user.site_id or auto_discover_site_id(api_key)

    if not api_key or not site_id:
        return {"status": "error", "error": "Missing site_id or API key"}

    user.site_id = site_id
    db.session.commit()
    print(f"[Amber] Using site ID: {site_id}")

    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=30)  # 1 month of history
    batch = timedelta(days=7)

    all_general, all_feed = [], []

    print(f"[Amber] Fetching usage {start_date} → {end_date} in 7-day batches")

    d1 = start_date
    while d1 < end_date:
        d2 = min(d1 + batch, end_date)
        for ch_type, storage in [("general", all_general), ("feedIn", all_feed)]:
            chunk = fetch_usage(api_key, site_id, ch_type, d1, d2)
            print(f"[Amber] {ch_type} returned {len(chunk)} records for {d1} → {d2}")
            storage.extend(chunk)
        d1 += batch

    print(
        f"[Amber] Total fetched: {len(all_general)} general, {len(all_feed)} feedIn"
    )

    stored = 0
    db.session.rollback()  # reset in case of previous errors

    for gen_rec, feed_rec in zip(all_general, all_feed):
        try:
            ts_raw = gen_rec.get("interval") or gen_rec.get("timestamp")
            if not ts_raw:
                continue
            ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))

            # Extract nested values safely
            gen_channels = gen_rec.get("channels", [])
            feed_channels = feed_rec.get("channels", [])

            import_kwh = (
                gen_channels[0].get("kwh") if gen_channels and gen_channels[0] else 0.0
            )
            export_kwh = (
                feed_channels[0].get("kwh") if feed_channels and feed_channels[0] else 0.0
            )
            import_price = (
                gen_channels[0].get("price")
                if gen_channels and gen_channels[0]
                else 0.0
            )
            export_price = (
                feed_channels[0].get("price")
                if feed_channels and feed_channels[0]
                else 0.0
            )

            cost = (import_kwh * import_price) - (export_kwh * export_price)

            interval = Interval.query.get(ts)
            if not interval:
                interval = Interval(
                    ts=ts,
                    import_kwh=import_kwh,
                    export_kwh=export_kwh,
                    import_price=import_price,
                    export_price=export_price,
                    cost=cost,
                )
                db.session.add(interval)
                stored += 1
            else:
                # update existing
                interval.import_kwh = import_kwh
                interval.export_kwh = export_kwh
                interval.import_price = import_price
                interval.export_price = export_price
                interval.cost = cost
        except Exception as e:
            print(f"[Amber] Error processing record: {e}")

    db.session.commit()
    print(f"[Amber] Stored {stored} intervals.")
    return {"status": "ok", "count": stored}
