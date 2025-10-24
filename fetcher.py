from datetime import datetime, timedelta
from flask import current_app
import pytz
from amber import AmberClient
from models import db, Interval, UserConfig

AEST = pytz.timezone("Australia/Sydney")


def _parse_ts(val):
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
    # Amber prices are in cents/kWh; convert to dollars.
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
    Pull Amber usage for both import (general) and feedIn (export),
    then compute costs exactly as Amber does.
    Works with or without user explicitly passed.
    """
    try:
        # ✅ Fallback: query first user if not passed
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
        start_date = end_date - timedelta(days=10)

        # ✅ Pull import (general usage) and export (feedIn)
        imp_data = client._get(
            f"sites/{site_id}/usage?channelType=general&startDate={start_date}&endDate={end_date}"
        )
        exp_data = client._get(
            f"sites/{site_id}/usage?channelType=feedIn&startDate={start_date}&endDate={end_date}"
        )

        combined = {}

        # --- IMPORT ---
        for d in imp_data or []:
            ts = _parse_ts(d.get("startTime"))
            if not ts:
                continue
            kwh = float(d.get("kwh", 0.0))
            per = float(d.get("spotPerKwh", 0.0))  # cents/kWh
            combined.setdefault(ts, dict(imp=0.0, exp=0.0, imp_p=0.0, exp_p=0.0))
            combined[ts]["imp"] += kwh
            combined[ts]["imp_p"] = per

        # --- EXPORT ---
        for d in exp_data or []:
            ts = _parse_ts(d.get("startTime"))
            if not ts:
                continue
            kwh = float(d.get("kwh", 0.0))
            per = float(d.get("spotPerKwh", 0.0))  # negative for feed-in
            combined.setdefault(ts, dict(imp=0.0, exp=0.0, imp_p=0.0, exp_p=0.0))
            combined[ts]["exp"] += kwh
            combined[ts]["exp_p"] = per

        count = 0
        for ts, v in combined.items():
            upsert(ts, v["imp"], v["exp"], v["imp_p"], v["exp_p"])
            count += 1

        db.session.commit()
        print(f"[Amber] Stored {count} intervals ({start_date}→{end_date})")
        return {"status": "ok", "count": count}

    except Exception as e:
        db.session.rollback()
        print(f"[Amber] pull failed: {e}")
        return {"status": "error", "error": str(e)}
