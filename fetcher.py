from datetime import datetime, timedelta
from flask import current_app
import pytz
from amber import AmberClient
from models import db, Interval, Agg, UserConfig
from collections import defaultdict

AEST = pytz.timezone("Australia/Sydney")


def _parse_ts(val):
    """Parse Amber timestamp to AEST datetime."""
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
        print(f"[Amber] Timestamp parse failed for {val}: {e}")
        return None


def upsert(ts, imp, exp, imp_price, exp_price):
    """Store or update a 30-minute interval."""
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


def _fetch_chunk(client, site_id, start, end, channel):
    """Fetch ≤7-day chunk of data."""
    url = f"sites/{site_id}/usage?channelType={channel}&startDate={start}&endDate={end}"
    data = client._get(url)
    if not isinstance(data, list):
        print(f"[Amber] Invalid response for {channel} {start}→{end}: {data}")
        return []
    return data


def _update_aggregates():
    """Rebuild daily aggregates after each pull."""
    db.session.query(Agg).delete()
    daily = defaultdict(lambda: dict(imp=0.0, exp=0.0, cost=0.0))
    for i in Interval.query.all():
        d = i.ts.date()
        daily[d]["imp"] += i.import_kwh
        daily[d]["exp"] += i.export_kwh
        daily[d]["cost"] += i.cost
    for d, v in daily.items():
        db.session.add(Agg(date=d, import_kwh=v["imp"], export_kwh=v["exp"], cost=v["cost"]))
    db.session.commit()
    print(f"[Amber] Aggregated {len(daily)} days into Agg table.")


def pull_once(user=None):
    """Pull Amber usage for import/export, safe 7-day chunks."""
    try:
        if user is None:
            user = UserConfig.query.first()
            if not user:
                return {"status": "error", "error": "No user found"}

        base_url = current_app.config.get("AMBER_BASE_URL", "https://api.amber.com.au/v1")
        client = AmberClient(base_url=base_url, api_key=user.api_key)

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

        # Use explicit 7-day window for now
        start_date = datetime(2025, 10, 17).date()
        end_date = datetime(2025, 10, 24).date()
        print(f"[Amber] Pulling {site_id} {start_date}→{end_date}")

        combined = defaultdict(lambda: dict(imp=0.0, exp=0.0, imp_p=0.0, exp_p=0.0))

        for ch in ["general", "feedIn"]:
            data = _fetch_chunk(client, site_id, start_date, end_date, ch)
            print(f"[Amber] Got {len(data)} records for {ch}")
            for d in data:
                ts = _parse_ts(d.get("startTime") or d.get("nemTime") or d.get("date"))
                if not ts:
                    continue
                kwh = float(d.get("kwh", 0.0))
                # Amber changed field naming a few times
                per = d.get("spotPerKwh")
                if per is None:
                    per = d.get("perKwh", 0.0)
                per = float(per)
                combined[ts]["imp" if ch == "general" else "exp"] += kwh
                if ch == "general":
                    combined[ts]["imp_p"] = per
                else:
                    combined[ts]["exp_p"] = per

        count = 0
        for ts, v in combined.items():
            upsert(ts, v["imp"], v["exp"], v["imp_p"], v["exp_p"])
            count += 1
        db.session.commit()
        print(f"[Amber] Stored {count} intervals between {start_date}→{end_date}")

        _update_aggregates()
        return {"status": "ok", "count": count}

    except Exception as e:
        db.session.rollback()
        print(f"[Amber] pull failed: {e}")
        return {"status": "error", "error": str(e)}
