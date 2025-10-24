from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()

class UserConfig(db.Model):
    id = db.Column(db.Integer, primary_key=True, default=1)
    api_key = db.Column(db.String(256), nullable=True)
    site_id = db.Column(db.String(64), nullable=True)
    solar_kw = db.Column(db.Float, default=0.0)
    battery_kwh = db.Column(db.Float, default=0.0)
    battery_efficiency = db.Column(db.Float, default=0.9)  # 90% round-trip

class Interval(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    ts = db.Column(db.DateTime, index=True)
    import_kwh = db.Column(db.Float, default=0.0)
    export_kwh = db.Column(db.Float, default=0.0)
    import_price = db.Column(db.Float, default=0.0)  # $/kWh
    export_price = db.Column(db.Float, default=0.0)  # $/kWh
    cost = db.Column(db.Float, default=0.0)          # computed import_kwh * import_price - export_kwh * export_price
    source = db.Column(db.String(32), default="pull")
    
    def date_key(self):
        d = self.ts.date()
        return d.isoformat()
