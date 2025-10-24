from datetime import datetime, timedelta
import os
import pytz
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import current_app
from models import db, UserConfig, Interval
from amber import AmberClient

AEST = pytz.timezone("Australia/Sydney")

# -------------------------------
# Helpers: DB write
# -------------------------------

def upsert_interval(
    ts: datetime,
    import_kwh: float,
    export_kwh: float,
    import_price: float,
    export_price: float,
):
    """Insert or update a usage interval in the database."""
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


# -------------------------------
# Helpers: HTTP fetch
# -------------------------------

def _cfg_base_url() -> str:
    """Get Amber base url from Flask config or env."""
    try:
        return current_app.config.get("AMBER_BASE_URL")
    except Exception:
        return os.getenv("AMBER_BASE_URL", "https://api.amber.com.au/v1")


def _safe_json(val):
    """Normalize Amber responses to a list/dict without throwing."""
    if isinstance(val, dict):
        return val
    return val or {}


def _parse_ts(val) -> Optional[datetime]:
    """Parse ISO8601 timestamp into datetime (UTC-aware when Z)."""
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


# -------------------------------
# Channel discovery + parallel pulls
# -------------------------------

DEFAULT_CHANNELS = ["general", "feedin", "battery_charge", "battery_discharge"]

def discover_channels(client: AmberClient, site_id: str) -> List[str]:
    """
    Ask Amber for available channels. If the endpoint isn't present or
    returns 4xx/5xx, fall back to a sensible default superset.
    """
    try:
        data = client._get(f"sites/{site_id}/channels")
        if isinstance(data, dict):
            # Shape can be {"data":[{"channelType":"general"}, ...]} or {"channels":[...]}
            items = data.get("data") or data.get("channels") or []
            types = set()
            for it in items:
                ct = it.get("channelType") or it.get("type")
                if ct:
                    types.add(ct)
            if types:
                # Keep only channels we know how to merge
                return [c for c in DEFAULT_CHANNELS if c in types]
    except Exception:
        pass
    # Fallback: try them all; we'll skip 400/403 gracefully per-channel.
    return DEFAULT_CHANNELS[:]


def fetch_channel_blocking(
    client: AmberClient,
    site_id: str,
    channel: str,
    start_date: str,
    end_date: str,
) -> Tuple[str, List[dict]]:
    """
    Pull a single channel's usage, trying both resolution styles.
    Returns (channel, intervals_list). On error/403/400, returns (channel, []).
    """
    # First try "30m"
    params = {
        "startDate": start_date,
        "endDate": end_date,
        "channelType": channel,
        "resolution": "30m",
    }
    try:
        res = client.usage(site_id, **params)
        if isinstance(res, dict):
            data = res.get("data") or []
        else:
            data = res or []
        return channel, data
    except Exception:
        # Retry with ISO8601 duration "PT30M"
        params["resolution"] = "PT30M"
        try:
            res = client.usage(site_id, **params)
            if isinstance(res, dict):
                data = res.get("data") or []
            else:
                data = res or []
            return channel, data
        except Exception:
            # Give up on this channel
            return channel, []


def parallel_fetch_usage(
    client: AmberClient,
    site_id: str,
    channels: List[str],
    start_date: str,
    end_date: str,
) -> Dict[str, List[dict]]:
    """Fetch all channels in parallel and return dict[channel] -> list[intervals]."""
    out: Dict[str, List[dict]] = {ch: [] for ch in channels}
    with ThreadPoolExecutor(max_workers=min(8, len(channels) or 1)) as ex:
        futs = [
            ex.submit(fetch_channel_blocking, client, site_id, ch, start_date, end_date)
            for ch in channels
        ]
        for fut in as_completed(futs):
            ch, data = fut.result()
            out[ch] = data or []
    return out


# -------------------------------
# Price pulling
# -------------------------------

def build_price_index(prices_resp) -> Dict[datetime, Dict[str, float]]:
    """
    Build a map ts -> {"import": per_kwh, "export": export}
    from Amber prices response.
    """
    idx: Dict[datetime, Dict[str, float]] = {}
    if isinstance(prices_resp, dict):
        plist = prices_resp.get("data") or prices_resp.get("prices") or []
    else:
        plist = prices_resp or []
    for p in plist:
        ts = _parse_ts(p.get("start") or p.get("interval_start"))
        if not ts:
            continue
        idx[ts] = {
            "import": float(p.get("per_kwh", p.get("import", 0.0)) or 0.0),
            "export": float(p.get("export", 0.0) or 0.0),
        }
    return idx


# -------------------------------
# Merge channels -> net grid import/export
# -------------------------------

def merge_channels_to_grid(
    by_channel: Dict[str, List[dict]]
) -> Dict[datetime, Dict[str, float]]:
    """
    Combine channel intervals into net grid import/export per timestamp.

    Heuristic:
      - general          : household grid import (kWh)
      - feedin           : household grid export (kWh)
      - battery_charge   : energy drawn from grid to charge battery (adds to import)
      - battery_discharge: energy supplied back to house/grid (reduces import; if surplus -> export)

    We compute a net grid exchange per interval:
        net = general + battery_charge - battery_discharge - feedin
      If net >= 0 -> import_kwh = net, export_kwh = 0
      If net <  0 -> import_kwh = 0,   export_kwh = -net
    """
    # First, index each channel by timestamp
    index: Dict[str, Dict[datetime, float]] = {}
    for ch, items in by_channel.items():
        tsmap: Dict[datetime, float] = {}
        for it in items or []:
            ts = _parse_ts(it.get("interval_start") or it.get("start"))
            if not ts:
                continue
            # normalized kWh key
            kwh = it.get("kwh")
            if kwh is None:
                # sometimes Amber uses specific keys per channel
                kwh = it.get("import_kwh") or it.get("export_kwh") or 0.0
            tsmap[ts] = tsmap.get(ts, 0.0) + float(kwh or 0.0)
        index[ch] = tsmap

    # Union of all timestamps
    all_ts = set()
    for m in index.values():
        all_ts.update(m.keys())

    # Merge into net import/export
    merged: Dict[datetime, Dict[str, float]] = {}
    for ts in sorted(all_ts):
        general = index.get("general", {}).get(ts, 0.0)
        feedin = index.get("feedin", {}).get(ts, 0.0)
        batt_chg = index.get("battery_charge", {}).get(ts, 0.0)
        batt_dis = index.get("battery_discharge", {}).get(ts, 0.0)

        net = general + batt_chg - batt_dis - feedin

        if net >= 0:
            merged[ts] = {"import_kwh": net, "export_kwh": 0.0}
        else:
            merged[ts] = {"import_kwh": 0.0, "export_kwh": -net}

    return merged


# -------------------------------
# Main entry: pull_once
# -------------------------------

def pull_once(now: datetime | None = None):
    """Pull ~30 days of usage (multi-channel) + prices and upsert into DB."""
    user = UserConfig.query.get(1)
    if not user or not user.api_key:
        return {"status": "no-api-key"}

    base_url = _cfg_base_url()
    client = AmberClient(base_url=base_url, api_key=user.api_key)

    # Site discovery
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

    # Time window
    if now is None:
        now = datetime.now(AEST)
    start_date = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    end_date = now.strftime("%Y-%m-%d")

    # Discover channels (fallback to defaults)
    channels = discover_channels(client, site_id)
    if not channels:
        channels = DEFAULT_CHANNELS[:]

    # Pull usage (parallel per channel)
    by_channel = parallel_fetch_usage(client, site_id, channels, start_date, end_date)

    # Merge channels -> grid import/export
    merged = merge_channels_to_grid(by_channel)

    # Prices
    try:
        prices = client.prices(site_id)
    except Exception as e:
        return {"status": "prices-error", "error": str(e)}
    price_idx = build_price_index(prices)

    # Upsert into DB
    count = 0
    for ts, vals in merged.items():
        import_price = float(price_idx.get(ts, {}).get("import", 0.0))
        export_price = float(price_idx.get(ts, {}).get("export", 0.0))
        upsert_interval(ts, vals["import_kwh"], vals["export_kwh"], import_price, export_price)
        count += 1

    db.session.commit()
    return {"status": "ok", "count": count}
