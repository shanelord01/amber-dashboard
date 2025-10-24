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
    except Exception:
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
    """Fetch one ≤7-day chunk of data."""
    url = f"sites/{site_id}/usage?channelType={channel}&startDate={start}&endDate={end}"
    data = client._get(url)
    if isinstance(data, dict) and "error" in data:
        print(f"[Amber] Error fetching {channel} {start}→{end}: {data['error']}")
        return []
    return data or []


def _update_aggregates():
    """Aggregate all Interval data into Agg (daily totals)."""
    print("[Amber] Updating aggregate daily totals…")
    db.session.query(Agg).delete()
    daily = defaultdict(lambda: dict(imp=0.0, exp=0.0, cost=0.0))
    for i in Interval.query.all():
        d = i.ts.date()
        daily[d]["imp"] += i.import_kwh
        daily[d]["exp"] += i.export_kwh
        daily[d]["cost"] += i.cost
    for d, vals in sorted(daily.items()):
        agg = Agg(date=d, import_kwh=vals["imp"], export_kwh=vals["exp"], cost=vals["cost"])
        db.session.add(agg)
    db.session.commit()
    print(f"[Amber] Aggregated {len(daily)} days into Agg table.")


def pull_once(user=None):
    """
    Pull Amber usage for import (general) and feedIn (export),
    respecting Amber’s 7-day range limit, then update aggregates.
    """
    try:
        if user is None:
            user = UserConfig.query.first()
            if not user:
                return {"status": "error", "error": "No user found in DB"}

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

        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=14)  # pull 2 weeks, 7-day chunks
        print(f"[Amber] Fetching site {site_id} from {start_date} to {end_date} (max 7-day chunks)")

        combined = defaultdict(lambda: dict(imp=0.0, exp=0.0, imp_p=0.0, exp_p=0.0))
        day = start_date
        while day < end_date:
            chunk_end = min(day + timedelta(days=7), end_date)
            for ch in ["general", "feedIn"]:
                data = _fetch_chunk(client, site_id, day, chunk_end, ch)
                print(f"[Amber] Got {len(data)} {ch} records for {day}→{chunk_end}")
                for d in data:
                    ts = _parse_ts(d.get("startTime"))
                    if not ts:
                        continue
                    kwh = float(d.get("kwh", 0.0))
                    per = float(d.get("spotPerKwh", 0.0))
                    if ch == "general":
                        combined[ts]["imp"] += kwh
                        combined[ts]["imp_p"] = per
                    else:
                        combined[ts]["exp"] += kwh
                        combined[ts]["exp_p"] = per
            day += timedelta(days=7)

        count = 0
        for ts, v in combined.items():
            upsert(ts, v["imp"], v["exp"], v["imp_p"], v["exp_p"])
            count += 1

        db.session.commit()
        print(f"[Amber] Stored {count} half-hour intervals ({start_date}→{end_date})")

        _update_aggregates()

        print("[Amber] Pull complete.")
        return {"status": "ok", "count": count}

    except Exception as e:
        db.session.rollback()
        print(f"[Amber] pull failed: {e}")
        return {"status": "error", "error": str(e)}
