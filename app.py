from flask import Flask, render_template, request, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import pandas as pd

from config import settings
from models import db, UserConfig, Interval
from amber import AmberClient
from fetcher import pull_once
from utils import aggregate_cost
from simulator import simulate

app = Flask(__name__)
app.config['SECRET_KEY'] = settings.SECRET_KEY
app.config['SQLALCHEMY_DATABASE_URI'] = settings.SQLALCHEMY_DATABASE_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['AMBER_BASE_URL'] = settings.AMBER_BASE_URL

db.init_app(app)

@app.before_request
def ensure_db():
    db.create_all()
    # ensure singleton user row exists
    if not UserConfig.query.get(1):
        db.session.add(UserConfig(id=1))
        db.session.commit()

@app.route('/')
def home():
    user = UserConfig.query.get(1)
    # last 90 days by default
    rows = Interval.query.order_by(Interval.ts.desc()).limit(90*48).all()
    rows = list(reversed(rows))
    agg = aggregate_cost(rows)
    return render_template('dashboard.html', user=user, agg=agg)

@app.route('/settings', methods=['GET','POST'])
def settings_view():
    user = UserConfig.query.get(1)
    if request.method == 'POST':
        user.api_key = request.form.get('api_key','').strip()
        user.site_id = request.form.get('site_id','').strip()
        user.solar_kw = float(request.form.get('solar_kw', 0) or 0)
        user.battery_kwh = float(request.form.get('battery_kwh', 0) or 0)
        user.battery_efficiency = float(request.form.get('battery_efficiency', 0.9) or 0.9)
        db.session.commit()
        try:
            pull_once()
            flash('Saved. Pulled latest data.')
        except Exception as e:
            flash(f'Saved, but pull failed: {e}')
        return redirect(url_for('home'))
    return render_template('settings.html', user=user)

@app.route('/pull')
def pull_now():
    res = pull_once()
    flash(str(res))
    return redirect(url_for('home'))

@app.route('/simulate', methods=['GET','POST'])
def simulate_view():
    user = UserConfig.query.get(1)
    # choose a recent month
    rows = Interval.query.order_by(Interval.ts.desc()).limit(30*48).all()
    rows = list(reversed(rows))
    intervals = [
        {
            'ts': r.ts,
            'import_kwh': r.import_kwh,
            'export_kwh': r.export_kwh,
            'import_price': r.import_price,
            'export_price': r.export_price,
        } for r in rows
    ]
    result = None
    params = {
        'solar_kw': user.solar_kw or 0.0,
        'battery_kwh': user.battery_kwh or 0.0,
        'battery_efficiency': user.battery_efficiency or 0.9
    }
    if request.method == 'POST':
        params['solar_kw'] = float(request.form.get('solar_kw', params['solar_kw']) or 0)
        params['battery_kwh'] = float(request.form.get('battery_kwh', params['battery_kwh']) or 0)
        params['battery_efficiency'] = float(request.form.get('battery_efficiency', params['battery_efficiency']) or 0.9)
    if intervals:
        result = simulate(intervals, params['solar_kw'], params['battery_kwh'], params['battery_efficiency'])
    return render_template('simulate.html', user=user, params=params, result=result)


# background scheduler to keep DB fresh
scheduler = BackgroundScheduler(daemon=True)
@scheduler.scheduled_job('interval', minutes=settings.PULL_MINUTES)
def scheduled_pull():
    try:
        with app.app_context():
            pull_once()
    except Exception as e:
        print('scheduled pull failed:', e)

scheduler.start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
