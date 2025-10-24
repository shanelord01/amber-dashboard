from flask import Flask, render_template, request, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from models import db, Interval, UserConfig
from fetcher import pull_once

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///amber.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db.init_app(app)

# --- Initialize database safely for Flask 3.x ---
with app.app_context():
    db.create_all()
    if not UserConfig.query.get(1):
        user = UserConfig(id=1, api_key="", site_id=None)
        db.session.add(user)
        db.session.commit()


@app.route("/")
def index():
    # Latest 48 intervals (~24 hours)
    intervals = Interval.query.order_by(Interval.ts.desc()).limit(48).all()

    # Daily aggregation
    daily = (
        db.session.query(
            db.func.date(Interval.ts).label("day"),
            db.func.sum(Interval.import_kwh).label("import_kwh"),
            db.func.sum(Interval.export_kwh).label("export_kwh"),
            db.func.sum(Interval.cost).label("net_cost"),
        )
        .group_by(db.func.date(Interval.ts))
        .order_by(db.func.date(Interval.ts).desc())
        .all()
    )

    # Monthly aggregation
    monthly = (
        db.session.query(
            db.func.strftime("%Y-%m", Interval.ts).label("month"),
            db.func.sum(Interval.import_kwh).label("import_kwh"),
            db.func.sum(Interval.export_kwh).label("export_kwh"),
            db.func.sum(Interval.cost).label("net_cost"),
        )
        .group_by(db.func.strftime("%Y-%m", Interval.ts))
        .order_by(db.func.strftime("%Y-%m", Interval.ts).desc())
        .all()
    )

    user = UserConfig.query.get(1)
    return render_template(
        "index.html",
        intervals=intervals,
        daily=daily,
        monthly=monthly,
        user=user,
    )


@app.route("/settings", methods=["GET", "POST"])
def settings():
    user = UserConfig.query.get(1)
    if request.method == "POST":
        user.api_key = request.form.get("api_key", "").strip()
        user.site_id = request.form.get("site_id", "").strip() or None
        db.session.commit()
        return redirect(url_for("index"))
    return render_template("settings.html", user=user)


@app.route("/pull")
def pull():
    user = UserConfig.query.get(1)
    result = pull_once(user)
    print(result)
    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
