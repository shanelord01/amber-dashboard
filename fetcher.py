from datetime import datetime, timedelta
import pytz
import os
from flask import current_app
from models import db, UserConfig, Interval
from amber import AmberClient

AEST = pytz.timezone("Australia/Sydney")


def upsert_interval(ts: datetime, import_kwh: float, export_kwh: float, import_price: float, export_price: float):
    """Insert or update an interval record in the DB."""
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
    """Pull ~30 days of usage & prices and upsert into DB."""
    user = UserConfig.query.get(1)
    if not user or not user.api_key:
        return {"status": "no-api-key"}

    # Try Flask config first, fallback to env var
    try:
        base_url = current_app.config.get("AMBER_BASE_URL")
    except Exception:
        base_url = os.getenv("AMBER_BASE_URL", "https://api.amber.com.au/v1")

    client = AmberClient(base_url=base_url, api_key=user.api_key)

    # --- Site discovery ----------------------------------------------------
    site_id = user.site_id
    if not site_id:
        try:
            sites = client.sites()
            if isinstance(sites, dict) and "sites" in sites:
                sites = sites["sites"]
            if sites:
                site_id = sites[0].get("id") or sites[0].get("siteId")
                if site_id:
                    user.site_id = site_id
                    db.session.commit()
        except Exception as e:
            return {"status": "sites-error", "error": str(e)}

    if not site_id:
        return {"status": "no-site"}

    # --- Time window -------------------------------------------------------
    if now is None:
        now = datetime.now(AEST)
    start = (now - timedelta(days=30)).astimezone(AEST)

    # --- Fetch usage -------------------------------------------------------
    try:
        end_time = datetime.utcnow().isoformat() + "Z"
        start_time = (datetime.utcnow() - timedelta(days=30)).isoformat() + "Z"
        usage = client.usage(
            site_id,
            resolution="30m",
            startTime=start_time,
            endTime=end_time,
        )
    except Exception as e:
        return {"status": "usage-error", "error": str(e)}

    intervals = usage.get("data") if isinstance(usage, dict) else usage or []

    # --- Fetch prices ------------------------------------------------------
    try:
        prices = client.prices(site_id)
    except Exception:
        prices = {}

    price_idx = {}
    if isinstance(prices, dict):
        price_list = prices.get("data") or prices.get("prices") or []
    else:
        price_list = prices or []

    for p in price_list:
        ts = _parse_ts(p.get("start") or p.get("interval_start"))
        if ts:
            price_idx[ts] = {
                "import": p.get("per_kwh", p.get("import", 0.0)),
                "export": p.get("export", 0.0),
            }

    # --- Merge usage & prices ---------------------------------------------
    count = 0
    for it in intervals:
        ts = _parse_ts(it.get("interval_start") or it.get("start"))
        if not ts or ts < start:
            continue

        import_kwh = it.get("import_kwh") or it.get("kwh", 0.0)
        export_kwh = it.get("export_kwh", 0.0)
        price = price_idx.get(ts, {})
        import_price = float(price.get("import", 0.0))
        export_price = float(price.get("export", 0.0))

        upsert_interval(ts, float(import_kwh or 0.0), float(export_kwh or 0.0), import_price, export_price)
        count += 1

    db.session.commit()
    return {"status": "ok", "count": count}


def _parse_ts(val):
    """Parse ISO8601 timestamp into datetime."""
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
