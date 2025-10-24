from datetime import datetime
from typing import Dict, List
import math

# Very simple daylight bell-curve profile for PV (per kW of DC capacity)
# Returns kWh produced in the interval, assuming 30-min intervals.

def pv_profile_kwh(ts: datetime, solar_kw: float) -> float:
    # crude model: production between 7:00 and 19:00 local time; peak at 13:00
    hour = ts.hour + ts.minute/60
    if hour < 7 or hour > 19 or solar_kw <= 0:
        return 0.0
    # bell curve centred at 13h, std dev ~3h
    peak = solar_kw  # kW peak near noon per kW installed
    sigma = 3.0
    val_kw = peak * math.exp(-0.5 * ((hour - 13)/sigma)**2)
    # 30-minute energy
    return max(val_kw * 0.5, 0.0)


def simulate(intervals: List[dict], solar_kw: float, battery_kwh: float, batt_eff: float = 0.9):
    """
    intervals: list of {ts, import_kwh, export_kwh, import_price, export_price}
    returns: dict with totals and per-interval simulated costs
    """
    soc = 0.0
    results = []
    orig_cost = 0.0
    new_cost = 0.0

    for it in intervals:
        ts = it['ts']
        load = it['import_kwh'] - it['export_kwh']  # net load before PV; if negative, net export
        pv = pv_profile_kwh(ts, solar_kw)
        net = load - pv

        # Battery logic: if net > 0, try to discharge to cover; if net < 0, charge from surplus
        if battery_kwh > 0:
            if net > 0 and soc > 0:
                discharge = min(net, soc)  # kWh available this interval
                net -= discharge
                soc -= discharge
                # discharge losses already occurred when charging; keep simple here
            elif net < 0 and soc < battery_kwh:
                surplus = -net
                room = battery_kwh - soc
                charge = min(surplus, room) * batt_eff
                soc += charge
                net += charge  # reduce export by charging

        # net > 0 means import required; net < 0 means export
        import_kwh = max(net, 0.0)
        export_kwh = max(-net, 0.0)

        # prices
        ip = it.get('import_price', 0.0)
        ep = it.get('export_price', 0.0)

        # costs
        baseline = it['import_kwh'] * ip - it['export_kwh'] * ep
        scenario = import_kwh * ip - export_kwh * ep

        orig_cost += baseline
        new_cost += scenario
        results.append({
            'ts': ts,
            'import_kwh': import_kwh,
            'export_kwh': export_kwh,
            'import_price': ip,
            'export_price': ep,
            'baseline_cost': baseline,
            'scenario_cost': scenario
        })

    return {
        'intervals': results,
        'baseline_total': orig_cost,
        'scenario_total': new_cost,
        'delta': orig_cost - new_cost
    }
