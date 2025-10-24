import requests
import datetime
from decimal import Decimal
from models import db, Interval, UserConfig


def pull_once(user=None):
    """Fetch and store Amber usage/feedIn data for the past 7 days.
    - Works even if app.py doesn't pass `user`
    - Auto-discovers the correct Amber site_id if not stored
    - Uses Amber's actual cost field for pricing accuracy
    """
    # backward compatibility: load default user
    if user is None:
        user = UserConfig.query.get(1)

    if not user or not user.api_key:
        return {"status": "error", "error": "Missing Amber API key"}

    api_key = user.api_key.strip()
    headers = {"Authorization": f"Bearer {api_key}"}

    end = datetime.date.today()
    start = end - datetime.timedelta(days=7)
    start_str, end_str = start.isoformat(), end.isoformat()

    # 🔍 Auto-discover site ID if not set or invalid
    if not user.site_id:
        try:
            r = requests.get("https://api.amber.com.au/v1/sites", headers=headers, timeout=10)
            if r.ok and isinstance(r.json(), list) and len(r.json()) > 0:
                sites = r.json()
                chosen = None

                # Try each site until one returns data
                for s in sites:
                    sid = s.get("id")
                    utest = requests.get(
                        f"https://api.amber.com.au/v1/sites/{sid}/usage?channelType=general&startDate={start_str}&endDate={end_str}",
                        headers=headers, timeout=10,
                    )
                    if utest.ok and isinstance(utest.json(), list) and len(utest.json()) > 0:
                        chosen = sid
                        print(f"[Amber] Found active site ID: {sid}")
                        break

                if not chosen:
                    chosen = sites[0]["id"]
                    print(f"[Amber] Defaulted to first site ID: {chosen}")

                user.site_id = chosen
                db.session.commit()
                print(f"[Amber] Auto-discovered site ID: {chosen}")
            else:
                print(f"[Amber] Failed to auto-discover site ID: {r.text}")
                return {"status": "error", "error": "Unable to auto-discover site ID"}
        except Exception as e:
            print(f"[Amber] Error discovering site ID: {e}")
            return {"status": "error", "error": f"Site ID lookup failed: {e}"}

    site_id = user.site_id.strip()
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
        print("[Amber] No records returned for either channel.")
        return {"status": "ok", "count": 0}

    all_records = general_data + feedin_data
    print(f"[Amber] Processing {len(all_records)} total records")

    added = 0
    for rec in all_records:
        try:
            ts = datetime.datetime.fromisoformat(rec["nemTime"].replace("Z", "+00:00"))
            kwh = Decimal(str(rec.get("kwh", 0)))
            cost = Decimal(str(rec.get("cost", 0))) / Decimal("100")  # cents→AUD
            channel = rec.get("channelType", "general")

            # Use 'ts' instead of 'timestamp' for backwards compatibility
            existing = Interval.query.filter_by(ts=ts).first()
            interval = existing or Interval(ts=ts)
            if not existing:
                db.session.add(interval)

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
    stored = added // 2
    print(f"[Amber] Stored {stored} intervals.")
    return {"status": "ok", "count": stored}
