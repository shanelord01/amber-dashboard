from datetime import datetime, timedelta
from flask import current_app
import pytz
from amber import AmberClient
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


def pull_once(user=None):
    """
    Pulls both 'general' and 'feedIn' usage channels from Amber.
    Works with 7-day limit and aggregates them into Interval.
    """
    try:
        # Resolve user
        if user is None:
            user = UserConfig.query.first()
            if not user:
                return {"status": "error", "error": "No user configured"}

        base_url = current_app.config.get("AMBER_BASE_URL", "https://api.amber.com.au/v1")
        client = AmberClient(base_url=base_url, api_key=user.api_key)

        # Resolve site
        site_id = user.site_id
        if not site_id:
            sites = client.sites()
            if isinstance(sites, dict) and "sites" in sites:
                sites = sites["sites"]
            if sites:
                site_id = sites[0].get("id") or sites[0].get("siteId")
                user.site_id = site_id
                db.session.commit()
        if not site_id:
            return {"status": "no-site"}

        # Amber max 7-day range
        start_date = datetime(2025, 10, 17).date()
        end_date = datetime(2025, 10, 24).date()

        print(f"[Amber] Fetching usage {start_date} → {end_date}")

        combined = defaultdict(lambda: dict(imp=0.0, exp=0.0, imp_p=0.0, exp_p=0.0))
        for channel in ["general", "feedIn"]:
            url = f"sites/{site_id}/usage?channelType={channel}&startDate={start_date}&endDate={end_date}"
            data = client._get(url)
            if not isinstance(data, list):
                print(f"[Amber] Invalid or empty {channel} response: {data}")
                continue

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
