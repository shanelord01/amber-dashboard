from datetime import datetime, timedelta
import pytz
from models import db, UserConfig, Interval
from amber import AmberClient

AEST = pytz.timezone("Australia/Sydney")

def upsert_interval(ts: datetime, import_kwh: float, export_kwh: float, import_price: float, export_price: float):
    row = Interval.query.filter_by(ts=ts).first()
    cost = (import_kwh * import_price) - (export_kwh * export_price)
    if not row:
        row = Interval(ts=ts, import_kwh=import_kwh, export_kwh=export_kwh,
                       import_price=import_price, export_price=export_price, cost=cost)
        db.session.add(row)
    else:
        row.import_kwh = import_kwh
        row.export_kwh = export_kwh
        row.import_price = import_price
        row.export_price = export_price
        row.cost = cost
    return row


def pull_once(now: datetime | None = None):
    """Pull last ~30 days of usage & prices and upsert into DB.
    Many Amber accounts only expose ~90 days history. We keep our own DB.
    """
    user = UserConfig.query.get(1)
    if not user or not user.api_key:
        return {"status": "no-api-key"}

    client = AmberClient(base_url=current_app.config['AMBER_BASE_URL'], api_key=user.api_key)

    # Site discovery if not stored
    site_id = user.site_id
    if not site_id:
        try:
            sites = client.sites()
            if isinstance(sites, dict) and 'sites' in sites:
                sites = sites['sites']
            if sites:
                site_id = sites[0].get('id') or sites[0].get('siteId')
                if site_id:
                    user.site_id = site_id
                    db.session.commit()
        except Exception as e:
            return {"status": "sites-error", "error": str(e)}

    if not site_id:
        return {"status": "no-site"}

    # Time window (last 30 days)
    if now is None:
        now = datetime.now(AEST)
    start = (now - timedelta(days=30)).astimezone(AEST)

    # Fetch usage
    try:
        usage = client.usage(site_id)
    except Exception as e:
        return {"status": "usage-error", "error": str(e)}

    # expected shape often contains interval objects with start, kwh fields; adapt if your account differs
    intervals = usage.get('data') if isinstance(usage, dict) else usage
    if not intervals:
        intervals = []

    # Fetch prices (import/export). If site-scoped fails, client.prices falls back to global.
    try:
        prices = client.prices(site_id)
    except Exception:
        prices = {}
    price_idx = {}
    # build map by timestamp if available
    if isinstance(prices, dict):
        price_list = prices.get('data') or prices.get('prices') or []
    else:
        price_list = prices or []
    for p in price_list:
        ts = _parse_ts(p.get('start') or p.get('interval_start'))
        if ts:
            price_idx[ts] = {
                'import': p.get('per_kwh', p.get('import', 0.0)),
                'export': p.get('export', 0.0)
            }

    count = 0
    for it in intervals:
        ts = _parse_ts(it.get('interval_start') or it.get('start'))
        if not ts:
            continue
        if ts < start:
            continue
        import_kwh = it.get('import_kwh') or it.get('kwh', 0.0)
        export_kwh = it.get('export_kwh', 0.0)
        price = price_idx.get(ts, {})
        import_price = float(price.get('import', 0.0))
        export_price = float(price.get('export', 0.0))
        upsert_interval(ts, float(import_kwh or 0.0), float(export_kwh or 0.0), import_price, export_price)
        count += 1

    db.session.commit()
    return {"status": "ok", "count": count}


def _parse_ts(val):
    from datetime import datetime
    if not val:
        return None
    # Amber usually returns ISO8601 Z timestamps
    try:
        if isinstance(val, str):
            if val.endswith('Z'):
                return datetime.fromisoformat(val.replace('Z', '+00:00'))
            return datetime.fromisoformat(val)
        return val
    except Exception:
        return None
