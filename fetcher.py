from datetime import datetime, timedelta
import pytz
from flask import current_app
from models import db, UserConfig, Interval
from amber import AmberClient

AEST = pytz.timezone("Australia/Sydney")


def _parse_ts(val):
    """Parse Amber timestamp strings safely."""
    if not val:
        return None
    try:
        if isinstance(val, str):
            if val.endswith("Z"):
                return datetime.fromisoformat(val.replace("Z", "+00:00"))
            return datetime.fromisoformat(val)
        return val
    except Exception:
        return None


def upsert_interval(ts, import_kwh, export_kwh, import_price, export_price):
    """Insert or update an interval row in the DB."""
    row = Interval.query.filter_by(ts=ts).first()
    cost = (import_kwh * import_price) - (export_kwh * export_price)
    if not row:
        row = Interval(
            ts=ts,
            import_kwh=import_kwh,
            export_kwh=export_kwh,
            import_price=import_price,
            export_price=export_price,
            cost=cost,
        )
        db.session.add(row)
    else:
        row.import_kwh = import_kwh
        row.export_kwh = export_kwh
        row.import_price = import_price
        row.export_price = export_price
        row.cost = cost
    return row


def pull_once(now: datetime | None = None):
    """Pull ~30 days of usage data from Amber and store locally."""
    user = UserConfig.query.get(1)
    if not user or not user.api_key:
        return {"status": "no-api-key"}

    base_url = current_app.config.get("AMBER_BASE_URL", "https://api.amber.com.au/v1")
    client = AmberClient(base_url=base_url, api_key=user.api_key)

    # --- Site discovery -------------------------------------------------------
    site_id = user.site_id
    if not site_id:
        try:
            sites = client.sites()
            if isinstance(sites, list) and sites:
                site_id = sites[0].get("id")
                user.site_id = site_id
                db.session.commit()
        except Exception as e:
            return {"status": "sites-error", "error": str(e)}

    if not site_id:
        return {"status": "no-site"}

    # --- Time window ----------------------------------------------------------
    if now is None:
        now = datetime.now(AEST)
    start = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    end = now.strftime("%Y-%m-%d")

    # --- Fetch usage ----------------------------------------------------------
    try:
        # Amber usage API expects startDate / endDate (not startTime / endTime)
        usage = client._get(
            f"sites/{site_id}/usage",
            params={
                "startDate": start,
                "endDate": end,
                "channelType": "general",
                "resolution": "30m",
            },
        )
    except Exception as e:
        return {"status": "usage-error", "error": str(e)}

    # Extract interval list
    intervals = usage.get("data") if isinstance(usage, dict) else usage
    if not intervals:
        intervals = []

    # --- Store results --------------------------------------------------------
    for it in intervals:
        ts = _parse_ts(it.get("interval_start") or it.get("start") or it.get("end"))
        if not ts:
            continue
        import_kwh = float(it.get("kwh", 0.0))
        export_kwh = 0.0

        # Placeholder prices (until public plan API wired up)
        import_price = 0.30
        export_price = 0.10

        upsert_interval(ts, import_kwh, export_kwh, import_price, export_price)

    db.session.commit()
    return {"status": "ok", "count": len(intervals)}
