from datetime import datetime, timedelta
import requests
from flask import current_app
import pytz
from models import db, Interval, UserConfig
from collections import defaultdict

AEST = pytz.timezone("Australia/Sydney")


def _parse_ts(val):
    """Parse Amber timestamps safely to AEST datetimes."""
    if not val:
        return None
    try:
        if isinstance(val, str):
            if val.endswith("Z"):
                dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
            else:
                dt = datetime.fromisoformat(val)
            return dt.astimezone(AEST)
        return val.astimezone(AEST)
    except Exception as e:
        print(f"[Amber] Failed to parse timestamp {val}: {e}")
        return None


def upsert(ts, imp, exp, imp_price, exp_price):
    """Insert or update a 30-min interval row."""
    if not ts:
        return
    row = Interval.query.filter_by(ts=ts).first()
    cost = (imp * imp_price / 100.0) + (exp * exp_price / 100.0)
    if not row:
        row = Interval(
            ts=ts,
            import_kwh=imp,
            export_kwh=exp,
            import_price=imp_price / 100.0,
            export_price=exp_price / 100.0,
            cost=cost,
        )
        db.session.add(row)
    else:
        row.import_kwh = imp
        row.export_kwh = exp
        row.import_price = imp_price / 100.0
        row.export_price = exp_price / 100.0
        row.cost = cost
    return row


def _fetch_chunk(site_id, api_key, start, end, channel):
    """Directly query Amber API for a channel."""
    url = f"https://api.amber.com.au/v1/sites/{site_id}/usage"
    params = {"channelType": channel, "startDate": start, "endDate": end}
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code != 200:
            print(f"[Amber] {channel} {r.status_code} error: {r.text[:120]}")
            return []
        data = r.json()
        if not isinstance(data, list):
            print(f"[Amber] Unexpected {channel} response type: {type(data)}")
            return []
        return data
    except Exception as e:
        print(f"[Amber] _fetch_chunk() failed for {channel}: {e}")
        return []


def pull_once(user=None):
    """Pull Amber usage directly via requests."""
    try:
        if user is None:
            user = UserConfig.query.first()
            if not user:
                return {"status": "error", "error": "No user configured"}

        site_id = user.site_id
        api_key = user.api_key
        if not (site_id and api_key):
            return {"status": "error", "error": "Missing site_id or API key"}

        start_date = datetime(2025, 10, 17).date()
        end_date = datetime(2025, 10, 24).date()

        print(f"[Amber] Fetching usage {start_date} → {end_date}")

        combined = defaultdict(lambda: dict(imp=0.0, exp=0.0, imp_p=0.0, exp_p=0.0))
        for channel in ["general", "feedIn"]:
            data = _fetch_chunk(site_id, api_key, start_date, end_date, channel)
            print(f"[Amber] Received {len(data)} records for {channel}")
            for d in data:
                ts = _parse_ts(d.get("startTime") or d.get("nemTime") or d.get("date"))
                if not ts:
                    continue
                kwh = float(d.get("kwh", 0.0))
                per = d.get("spotPerKwh")
                if per is None:
                    per = d.get("perKwh", 0.0)
                per = float(per)
                if channel == "general":
                    combined[ts]["imp"] += kwh
                    combined[ts]["imp_p"] = per
                else:
                    combined[ts]["exp"] += kwh
                    combined[ts]["exp_p"] = per

        count = 0
        for ts, v in combined.items():
            upsert(ts, v["imp"], v["exp"], v["imp_p"], v["exp_p"])
            count += 1
        db.session.commit()

        print(f"[Amber] Stored {count} intervals.")
        return {"status": "ok", "count": count}

    except Exception as e:
        db.session.rollback()
        print(f"[Amber] pull_once() failed: {e}")
        return {"status": "error", "error": str(e)}
