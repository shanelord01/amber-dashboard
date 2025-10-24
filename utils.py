from collections import defaultdict
from datetime import datetime


def aggregate_cost(intervals):
    buckets = {
        'daily': defaultdict(float),
        'monthly': defaultdict(float),
        'quarterly': defaultdict(float),
        'yearly': defaultdict(float)
    }
    for it in intervals:
        ts = it.ts
        dkey = ts.date().isoformat()
        mkey = f"{ts.year}-{ts.month:02d}"
        qkey = f"{ts.year}-Q{((ts.month-1)//3)+1}"
        ykey = str(ts.year)
        for key, b in [(dkey,'daily'), (mkey,'monthly'), (qkey,'quarterly'), (ykey,'yearly')]:
            buckets[b][key] += it.cost
    return {k: dict(v) for k, v in buckets.items()}
