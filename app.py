import os
from flask import Flask, render_template, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from models import db, UserConfig, Interval
from fetcher import pull_once

app = Flask(__name__)

# === Config ===
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///amber.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET', 'dev')

db.init_app(app)
migrate = Migrate(app, db)


@app.route('/')
def home():
    user = UserConfig.query.first()
    if not user:
        user = UserConfig(api_key='', site_id='')
        db.session.add(user)
        db.session.commit()

    agg = Interval.aggregate()
    return render_template('dashboard.html', user=user, agg=agg)


@app.route('/save-settings', methods=['POST'])
def save_settings():
    api_key = request.form.get('api_key')
    site_id = request.form.get('site_id')
    solar_kw = request.form.get('solar_kw', type=float)
    battery_kwh = request.form.get('battery_kwh', type=float)

    user = UserConfig.query.first()
    if not user:
        user = UserConfig(api_key=api_key, site_id=site_id)
        db.session.add(user)
    else:
        user.api_key = api_key
        user.site_id = site_id
        user.solar_kw = solar_kw
        user.battery_kwh = battery_kwh

    db.session.commit()

    try:
        res = pull_once(user)  # ✅ FIX: now passes the user object
    except Exception as e:
        res = {'status': 'error', 'error': str(e)}

    return jsonify(res)


@app.route('/pull', methods=['POST'])
def pull():
    user = UserConfig.query.first()
    if not user:
        return jsonify({'status': 'error', 'error': 'no user configured'})

    try:
        res = pull_once(user)  # ✅ FIX: passes user
    except Exception as e:
        res = {'status': 'error', 'error': str(e)}

    return jsonify(res)


if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(host='0.0.0.0', port=5000)
