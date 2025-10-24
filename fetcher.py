import requests
import datetime
from decimal import Decimal
from models import db, Interval, UserConfig


def pull_once(user=None):
    """Fetch and store Amber usage/feedIn data for the past 7 days."""
    # backward compatibility: auto-load user if not passed
    if user is None:
        user = UserConfig.query.get(1)

    if not user or not user.api_key or not user.site_id:
        return {"status": "error", "error": "Missing site_id or API key"}

    api_key = user.api_key.strip()
    site_id = user.site_id.strip()
    headers = {"Authorization": f"Bearer {api_key}"}

    end = datetime.date.today()
    start = end - datetime.timedelta(days=7)
    start_str, end_str = start.isoformat(), end.isoformat()
    print(f"[Amber] Fetching usage {start_str} → {end_str}")

    def get_usage(channel_type):
        url = (
            f"https://api.amber.com.au/v1/sites/{site_id}/usage"
            f"?channelType={channel_type}&startDate={start_str}&endDate={end_str}"
        )
        try:
            r = requests.get(url, headers=headers, timeout=20)
            if not r.ok:
                print(f"[Amber] {channel_type} request failed: {r.text}")
                return []
            data = r.json()
            if isinstance(data, list):
                print(f"[Amber] Received {len(data)} records for {channel_type}")
                return data
            else:
                print(f"[Amber] Unexpected response type for {channel_type}: {data}")
                return []
        except Exception as e:
            print(f"[Amber] Error fetching {channel_type}: {e}")
            return []

    general_data = get_usage("general")
    feedin_data = get_usage("feedIn")

    if not general_data and not feedin_data:
        return {"status": "ok", "count": 0}

    all_records = general_data + feedin_data
    print(f"[Amber] Processing {len(all_records)} total records")

    added = 0
    for rec in all_records:
        try:
            ts = datetime.datetime.fromisoformat(rec["nemTime"].replace("Z", "+00:00"))
            kwh = Decimal(str(rec.get("kwh", 0)))
            cost = Decimal(str(rec.get("cost", 0))) / Decimal("100")  # AUD
            channel = rec.get("channelType", "general")

            existing = Interval.query.filter_by(timestamp=ts).first()
            if not existing:
                interval = Interval(timestamp=ts)
                db.session.add(interval)
            else:
                interval = existing

            if channel == "general":
                interval.kwh_import = float(kwh)
                interval.cost_import = float(cost)
            elif channel == "feedIn":
                interval.kwh_export = float(kwh)
                interval.cost_export = float(cost)

            added += 1
        except Exception as e:
            print(f"[Amber] Error processing record: {e}")
            continue

    db.session.commit()
    print(f"[Amber] Stored {added // 2} intervals.")
    return {"status": "ok", "count": added // 2}
