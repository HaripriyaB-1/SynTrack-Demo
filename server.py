"""
SynTrack — Assembly Intelligence Platform
Team SyndiCAT_E5 · Caterpillar Tech Challenge 2026

Two portals:
  http://localhost:5000          → Operator portal (does assembly)
  http://localhost:5000/super    → Supervisor portal (read-only, live data)

Run: python server.py
"""

from flask import Flask, request, jsonify, render_template, Response
import sqlite3, json, time, threading, queue, os
from datetime import datetime, timedelta

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

# On Railway: /data is a persistent volume. Locally: ./data/
_RAILWAY = os.environ.get('RAILWAY_ENVIRONMENT') or os.environ.get('RAILWAY_PROJECT_ID')
if _RAILWAY:
    DB_PATH = '/data/syntrack.db'
else:
    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'syntrack.db')
sse_clients = []
sse_lock = threading.Lock()

# ─── ASSEMBLY DATA ────────────────────────────────────────────────────────────
ASSEMBLIES = {
    "WO-001": {
        "name": "Bracket Assembly LH", "machine": "CAT 777 Water Truck",
        "variant": "BracketAssy_LH", "total_steps": 6,
        "steps": [
            {"step":1,"tray":"R1C2","tray_key":"A","part":"M10 Bolt Grade 8.8","qty":4},
            {"step":2,"tray":"R2C1","tray_key":"B","part":"Lock Washer M10","qty":4},
            {"step":3,"tray":"R1C4","tray_key":"C","part":"Bracket Assy LH","qty":1},
            {"step":4,"tray":"R3C3","tray_key":"D","part":"Hex Nut M10","qty":4},
            {"step":5,"tray":"R2C4","tray_key":"E","part":"Spring Washer M10","qty":4},
            {"step":6,"tray":"R3C1","tray_key":"F","part":"Dowel Pin 8mm","qty":2},
        ]
    },
    "WO-002": {
        "name": "Engine Mount Kit", "machine": "CAT D11 Dozer",
        "variant": "EngineMount_Std", "total_steps": 5,
        "steps": [
            {"step":1,"tray":"R1C1","tray_key":"A","part":"M12 Bolt Grade 10.9","qty":6},
            {"step":2,"tray":"R2C3","tray_key":"B","part":"Engine Mount Pad","qty":2},
            {"step":3,"tray":"R3C2","tray_key":"C","part":"Isolator Bushing","qty":4},
            {"step":4,"tray":"R1C3","tray_key":"D","part":"M12 Washer","qty":6},
            {"step":5,"tray":"R2C2","tray_key":"E","part":"Nyloc Nut M12","qty":6},
        ]
    },
    "WO-003": {
        "name": "Hydraulic Manifold Kit", "machine": "CAT 390F Excavator",
        "variant": "HydManifold_A", "total_steps": 5,
        "steps": [
            {"step":1,"tray":"R1C1","tray_key":"A","part":"O-Ring 25mm","qty":8},
            {"step":2,"tray":"R2C4","tray_key":"B","part":"Banjo Bolt M14","qty":4},
            {"step":3,"tray":"R3C3","tray_key":"C","part":"Manifold Block","qty":1},
            {"step":4,"tray":"R1C2","tray_key":"D","part":"Copper Crush Washer","qty":4},
            {"step":5,"tray":"R3C1","tray_key":"E","part":"Cap Screw M8","qty":6},
        ]
    }
}

OPERATORS = [
    {"id":"OP-047","name":"Arun Kumar",     "role":"Senior Assembler", "shift":"Morning"},
    {"id":"OP-023","name":"Priya Sharma",   "role":"Line Operator",    "shift":"Morning"},
    {"id":"OP-091","name":"Karthik Raj",    "role":"Junior Assembler", "shift":"Afternoon"},
    {"id":"OP-058","name":"Meena Sundaram", "role":"Senior Assembler", "shift":"Afternoon"},
]

# ─── DATABASE ─────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with get_db() as db:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS work_orders (
            id TEXT PRIMARY KEY, name TEXT, machine TEXT, variant TEXT, total_steps INTEGER,
            status TEXT DEFAULT 'pending', activated_at TEXT, completed_at TEXT
        );
        CREATE TABLE IF NOT EXISTS pick_events (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            work_order_id   TEXT,
            operator_id     TEXT,
            step            INTEGER,
            tray_id         TEXT,
            part_name       TEXT,
            qty_expected    INTEGER,
            qty_actual      INTEGER,
            correct_tray    INTEGER,   -- 1 = correct, 0 = WRONG (wrong tray logged separately)
            wrong_attempts  INTEGER DEFAULT 0,
            duration_ms     INTEGER,
            timestamp       TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS wrong_tray_events (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            work_order_id TEXT,
            operator_id   TEXT,
            step          INTEGER,
            expected_tray TEXT,
            pressed_key   TEXT,
            timestamp     TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS operators (
            id TEXT PRIMARY KEY, name TEXT, role TEXT, shift TEXT
        );
        CREATE TABLE IF NOT EXISTS demo_state (key TEXT PRIMARY KEY, value TEXT);
        """)
        for wo_id, wo in ASSEMBLIES.items():
            db.execute("INSERT OR IGNORE INTO work_orders (id,name,machine,variant,total_steps) VALUES(?,?,?,?,?)",
                       (wo_id, wo["name"], wo["machine"], wo["variant"], wo["total_steps"]))
        for op in OPERATORS:
            db.execute("INSERT OR IGNORE INTO operators VALUES(?,?,?,?)",
                       (op["id"], op["name"], op["role"], op["shift"]))
        _seed_history(db)

def _seed_history(db):
    existing = db.execute("SELECT COUNT(*) FROM pick_events").fetchone()[0]
    if existing > 0:
        return
    import random as rnd
    print("  Seeding 30 days of historical data...")
    base = datetime.now() - timedelta(days=30)

    # Each operator has distinct personality
    profiles = {
        "OP-047": {"base_ms":2700,"jitter":600, "err":0.04},  # fast, experienced
        "OP-023": {"base_ms":3500,"jitter":950, "err":0.09},  # average
        "OP-091": {"base_ms":4600,"jitter":1500,"err":0.17},  # slow, learning
        "OP-058": {"base_ms":2400,"jitter":400, "err":0.02},  # expert
    }

    pick_rows       = []
    wrong_tray_rows = []
    keys = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

    for day_off in range(30):
        day = base + timedelta(days=day_off)
        for op_id, prof in profiles.items():
            if rnd.random() < 0.65:
                # Junior improves over time
                impr = 1.0 - (day_off/30)*0.22 if op_id == "OP-091" else 1.0
                wo_id = rnd.choice(list(ASSEMBLIES.keys()))
                for s in ASSEMBLIES[wo_id]["steps"]:
                    wrong_att = 0
                    if rnd.random() < prof["err"] * impr:
                        wrong_att = rnd.randint(1, 2)
                        # Log wrong tray events
                        for _ in range(wrong_att):
                            wrong_key = rnd.choice([k for k in keys[:8] if k != s["tray_key"].upper()])
                            ts = (day.replace(hour=rnd.randint(8,15), minute=rnd.randint(0,59))
                                  + timedelta(seconds=s["step"]*25)).strftime("%Y-%m-%d %H:%M:%S")
                            wrong_tray_rows.append((wo_id, op_id, s["step"], s["tray"], wrong_key, ts))

                    dur = max(900, int(prof["base_ms"]*impr + rnd.uniform(-prof["jitter"], prof["jitter"])))
                    ts = (day.replace(hour=rnd.randint(8,15), minute=rnd.randint(0,59))
                          + timedelta(seconds=s["step"]*25 + wrong_att*8)).strftime("%Y-%m-%d %H:%M:%S")
                    pick_rows.append((wo_id, op_id, s["step"], s["tray"], s["part"],
                                      s["qty"], s["qty"], 1, wrong_att, dur, ts))

    db.executemany("""INSERT INTO pick_events
        (work_order_id,operator_id,step,tray_id,part_name,qty_expected,qty_actual,
         correct_tray,wrong_attempts,duration_ms,timestamp) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
        pick_rows)
    db.executemany("""INSERT INTO wrong_tray_events
        (work_order_id,operator_id,step,expected_tray,pressed_key,timestamp)
        VALUES(?,?,?,?,?,?)""", wrong_tray_rows)
    print(f"  ✓ {len(pick_rows)} pick events + {len(wrong_tray_rows)} wrong tray events seeded")

# ─── ACCURACY FORMULA ─────────────────────────────────────────────────────────
# Accuracy = steps completed with ZERO wrong attempts / total steps completed
# This means if you had any wrong attempt on a step, that step is "impure"
def compute_accuracy(picks):
    if not picks:
        return 0.0
    perfect_steps = sum(1 for p in picks if (p["wrong_attempts"] if hasattr(p,"keys") else p[0]) == 0)
    total_steps   = len(picks)
    return round(perfect_steps / total_steps * 100, 1)

# ─── FATIGUE ANALYSIS ─────────────────────────────────────────────────────────
def compute_fatigue(op_id, db):
    today = datetime.now().strftime("%Y-%m-%d")
    picks = db.execute("""SELECT duration_ms, wrong_attempts FROM pick_events
        WHERE operator_id=? AND timestamp>=? ORDER BY id""", (op_id, today)).fetchall()

    if len(picks) < 3:
        return {"score":0,"level":"ok","flags":[],"metrics":{"duration_trend":100,"error_rate":0}}

    durs  = [p["duration_ms"] for p in picks]
    flags, metrics = [], {}

    # M1: pace slowdown
    baseline = sum(durs[:min(5,len(durs))]) / min(5,len(durs))
    recent   = sum(durs[-min(5,len(durs)):]) / min(5,len(durs))
    ratio    = recent / baseline if baseline > 0 else 1.0
    metrics["duration_trend"] = round(ratio * 100)
    if ratio > 1.30:
        flags.append("SLOW_PICKS")

    # M2: error rate from wrong_attempts
    last20    = picks[-20:]
    wrong_sum = sum(p["wrong_attempts"] for p in last20)
    total_att = len(last20) + wrong_sum
    err_rate  = wrong_sum / total_att if total_att > 0 else 0.0
    metrics["error_rate"] = round(err_rate * 100, 1)
    if err_rate > 0.15:
        flags.append("HIGH_ERRORS")

    level = "critical" if len(flags) >= 2 else "warning" if flags else "ok"
    return {"score":len(flags), "level":level, "flags":flags, "metrics":metrics}

# ─── SSE ─────────────────────────────────────────────────────────────────────
def broadcast(event_type, data):
    """Push to ALL connected clients — both operator and supervisor portals."""
    msg = f"event: {event_type}\ndata: {json.dumps(data)}\n\n"
    with sse_lock:
        dead = []
        for q in sse_clients:
            try:    q.put_nowait(msg)
            except: dead.append(q)
        for q in dead:
            sse_clients.remove(q)

# ─── ROUTES ───────────────────────────────────────────────────────────────────

@app.route('/')
def operator_portal():
    return render_template('operator.html', assemblies=ASSEMBLIES, operators=OPERATORS)

@app.route('/super')
def supervisor_portal():
    return render_template('supervisor.html', operators=OPERATORS, assemblies=ASSEMBLIES)

@app.route('/api/workorders')
def workorders():
    with get_db() as db:
        rows = db.execute("SELECT * FROM work_orders").fetchall()
    return jsonify([dict(r) for r in rows])

@app.route('/api/activate_wo', methods=['POST'])
def activate_wo():
    d = request.json or {}
    wo_id = d.get("wo_id")
    op_id = d.get("op_id")
    if wo_id not in ASSEMBLIES:
        return jsonify({"error": "unknown work order"}), 400
    with get_db() as db:
        db.execute("UPDATE work_orders SET status='pending'")
        db.execute("""UPDATE work_orders SET status='active',
                      activated_at=datetime('now','localtime') WHERE id=?""", (wo_id,))
        for k, v in [("active_wo", wo_id), ("active_op", op_id),
                     ("steps_done", "0"), ("session_errors", "0")]:
            db.execute("INSERT OR REPLACE INTO demo_state VALUES(?,?)", (k, v))
    wo = ASSEMBLIES[wo_id]
    op = next((o for o in OPERATORS if o["id"] == op_id), {"name": op_id})
    broadcast("wo_activated", {
        "wo_id": wo_id, "wo_name": wo["name"], "machine": wo["machine"],
        "total_steps": wo["total_steps"], "op_id": op_id, "op_name": op["name"],
        "first_step": wo["steps"][0]
    })
    return jsonify({"status": "activated", "wo": wo, "first_step": wo["steps"][0]})

@app.route('/api/log_wrong_tray', methods=['POST'])
def log_wrong_tray():
    """
    Called every time the operator presses a WRONG key.
    This is logged IMMEDIATELY — supervisor sees errors in real time.
    """
    d = request.json or {}
    with get_db() as db:
        db.execute("""INSERT INTO wrong_tray_events
            (work_order_id,operator_id,step,expected_tray,pressed_key)
            VALUES(?,?,?,?,?)""",
            (d["work_order_id"], d["operator_id"], d["step"],
             d["expected_tray"], d["pressed_key"]))
        # Increment session error counter
        se = db.execute("SELECT value FROM demo_state WHERE key='session_errors'").fetchone()
        new_se = int(se["value"] if se else 0) + 1
        db.execute("INSERT OR REPLACE INTO demo_state VALUES('session_errors',?)", (str(new_se),))

    # Broadcast immediately to supervisor — they see the mistake as it happens
    broadcast("wrong_tray", {
        "operator_id":   d["operator_id"],
        "op_name":       next((o["name"] for o in OPERATORS if o["id"]==d["operator_id"]), d["operator_id"]),
        "step":          d["step"],
        "expected_tray": d["expected_tray"],
        "pressed_key":   d["pressed_key"],
        "part_name":     d.get("part_name",""),
        "timestamp":     datetime.now().strftime("%H:%M:%S"),
    })
    return jsonify({"status": "logged"})

@app.route('/api/log_pick', methods=['POST'])
def log_pick():
    """
    Called when the operator successfully picks the correct tray.
    wrong_attempts = number of wrong keys pressed before getting it right.
    Accuracy = steps where wrong_attempts == 0.
    """
    d = request.json or {}
    with get_db() as db:
        db.execute("""INSERT INTO pick_events
            (work_order_id,operator_id,step,tray_id,part_name,
             qty_expected,qty_actual,correct_tray,wrong_attempts,duration_ms)
            VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (d["work_order_id"], d["operator_id"], d["step"],
             d["tray_id"], d["part_name"],
             d["qty_expected"], d["qty_actual"],
             1,                                    # correct_tray always 1 here
             d.get("wrong_attempts", 0),           # ← this is what drives accuracy
             d["duration_ms"]))

        sd = db.execute("SELECT value FROM demo_state WHERE key='steps_done'").fetchone()
        steps_done = int(sd["value"] if sd else 0) + 1
        db.execute("INSERT OR REPLACE INTO demo_state VALUES('steps_done',?)", (str(steps_done),))

        total = ASSEMBLIES.get(d["work_order_id"], {}).get("total_steps", 99)
        if steps_done >= total:
            db.execute("""UPDATE work_orders SET status='completed',
                          completed_at=datetime('now','localtime') WHERE id=?""",
                       (d["work_order_id"],))

        fat = compute_fatigue(d["operator_id"], db)

        # Today's accuracy computation
        today = datetime.now().strftime("%Y-%m-%d")
        today_picks = db.execute("""SELECT wrong_attempts FROM pick_events
            WHERE operator_id=? AND timestamp>=?""", (d["operator_id"], today)).fetchall()
        accuracy = compute_accuracy(today_picks)

        sp  = db.execute("SELECT COUNT(*) FROM pick_events WHERE operator_id=? AND timestamp>=?",
                         (d["operator_id"], today)).fetchone()[0]
        se  = db.execute("SELECT COALESCE(SUM(wrong_attempts),0) FROM pick_events WHERE operator_id=? AND timestamp>=?",
                         (d["operator_id"], today)).fetchone()[0]

    payload = {
        **d,
        "steps_done":     steps_done,
        "total_steps":    total,
        "fatigue":        fat,
        "session_picks":  sp,
        "session_errors": se,
        "accuracy":       accuracy,
        "timestamp":      datetime.now().strftime("%H:%M:%S"),
        "complete":       steps_done >= total,
        "op_name":        next((o["name"] for o in OPERATORS if o["id"]==d["operator_id"]), d["operator_id"]),
        "first_time_correct": d.get("wrong_attempts", 0) == 0,
    }
    broadcast("pick_logged", payload)

    if fat["level"] in ("warning", "critical"):
        broadcast("fatigue_alert", {
            "operator_id": d["operator_id"],
            "op_name": payload["op_name"],
            "level": fat["level"], "flags": fat["flags"],
            "metrics": fat["metrics"]
        })
    if steps_done >= total:
        broadcast("wo_complete", {
            "wo_id": d["work_order_id"],
            "wo_name": ASSEMBLIES.get(d["work_order_id"], {}).get("name", ""),
            "steps": steps_done
        })

    return jsonify({"status": "logged", "fatigue": fat, "steps_done": steps_done})

@app.route('/api/stats')
def get_stats():
    today = datetime.now().strftime("%Y-%m-%d")
    with get_db() as db:
        picks_today = db.execute("""SELECT wrong_attempts, duration_ms, operator_id
            FROM pick_events WHERE timestamp>=?""", (today,)).fetchall()

        total   = len(picks_today)
        # Accuracy = first-time-correct steps / total steps
        perfect = sum(1 for p in picks_today if p["wrong_attempts"] == 0)
        accuracy= round(perfect / total * 100, 1) if total > 0 else 0.0

        wrong_sum = sum(p["wrong_attempts"] for p in picks_today)
        avg_ms    = sum(p["duration_ms"] for p in picks_today) / total if total > 0 else 0

        # Wrong tray events today (real-time mistakes)
        wrong_events_today = db.execute(
            "SELECT COUNT(*) FROM wrong_tray_events WHERE timestamp>=?", (today,)).fetchone()[0]

        comp = db.execute("SELECT COUNT(*) FROM work_orders WHERE status='completed'").fetchone()[0]
        awo  = db.execute("SELECT * FROM work_orders WHERE status='active' LIMIT 1").fetchone()
        sd   = db.execute("SELECT value FROM demo_state WHERE key='steps_done'").fetchone()
        steps_done = int(sd["value"]) if sd else 0

        last20 = db.execute("""SELECT duration_ms, operator_id, wrong_attempts FROM pick_events
            WHERE timestamp>=? ORDER BY id DESC LIMIT 20""", (today,)).fetchall()

        tray_e = db.execute("""SELECT expected_tray tray_id, COUNT(*) e FROM wrong_tray_events
            WHERE timestamp>=? GROUP BY expected_tray ORDER BY e DESC LIMIT 12""", (today,)).fetchall()

        ops = []
        for op in OPERATORS:
            f    = compute_fatigue(op["id"], db)
            op_picks = db.execute("SELECT wrong_attempts FROM pick_events WHERE operator_id=? AND timestamp>=?",
                                  (op["id"], today)).fetchall()
            p_count  = len(op_picks)
            p_wrong  = sum(r["wrong_attempts"] for r in op_picks)
            p_acc    = compute_accuracy(op_picks)
            ops.append({
                "id": op["id"], "name": op["name"], "role": op["role"],
                "picks": p_count, "wrong_attempts": p_wrong,
                "accuracy": p_acc,
                "fatigue": f
            })

    return jsonify({
        "total_picks":        total,
        "perfect_picks":      perfect,
        "accuracy":           accuracy,
        "wrong_tray_events":  wrong_events_today,
        "wrong_attempts_sum": wrong_sum,
        "avg_duration_ms":    round(avg_ms),
        "completed_today":    comp,
        "steps_done":         steps_done,
        "active_wo":          dict(awo) if awo else None,
        "operators":          ops,
        "last20":             [{"ms": r["duration_ms"], "op": r["operator_id"],
                                "wa": r["wrong_attempts"]} for r in reversed(last20)],
        "tray_errors":        [dict(r) for r in tray_e],
    })

@app.route('/api/wrong_tray_log')
def wrong_tray_log():
    """Live feed of all wrong tray events today — for supervisor portal."""
    today = datetime.now().strftime("%Y-%m-%d")
    with get_db() as db:
        rows = db.execute("""SELECT wt.*, pe.part_name FROM wrong_tray_events wt
            LEFT JOIN pick_events pe
              ON wt.work_order_id=pe.work_order_id AND wt.operator_id=pe.operator_id AND wt.step=pe.step
            WHERE wt.timestamp>=? ORDER BY wt.id DESC LIMIT 50""", (today,)).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route('/api/picks_today')
def picks_today():
    today = datetime.now().strftime("%Y-%m-%d")
    with get_db() as db:
        rows = db.execute("""SELECT * FROM pick_events WHERE timestamp>=?
            ORDER BY id DESC LIMIT 60""", (today,)).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route('/api/operator/<op_id>/analytics')
def op_analytics(op_id):
    with get_db() as db:
        daily = db.execute("""SELECT DATE(timestamp) d, COUNT(*) picks,
            COALESCE(SUM(wrong_attempts),0) errors, COALESCE(AVG(duration_ms),0) avg_ms,
            SUM(CASE WHEN wrong_attempts=0 THEN 1 ELSE 0 END) perfect
            FROM pick_events WHERE operator_id=? GROUP BY DATE(timestamp) ORDER BY d""",
            (op_id,)).fetchall()
        by_wo = db.execute("""SELECT work_order_id, COUNT(*) picks,
            COALESCE(SUM(wrong_attempts),0) errors, COALESCE(AVG(duration_ms),0) avg_ms,
            SUM(CASE WHEN wrong_attempts=0 THEN 1 ELSE 0 END) perfect
            FROM pick_events WHERE operator_id=? GROUP BY work_order_id""",
            (op_id,)).fetchall()
        totals = db.execute("""SELECT COUNT(*) picks,
            COALESCE(SUM(wrong_attempts),0) errors, COALESCE(AVG(duration_ms),0) avg_ms,
            SUM(CASE WHEN wrong_attempts=0 THEN 1 ELSE 0 END) perfect
            FROM pick_events WHERE operator_id=?""", (op_id,)).fetchone()
        tray = db.execute("""SELECT expected_tray tray_id, COUNT(*) errors
            FROM wrong_tray_events WHERE operator_id=? GROUP BY expected_tray ORDER BY errors DESC""",
            (op_id,)).fetchall()

    op_info = next((o for o in OPERATORS if o["id"]==op_id),
                   {"id":op_id,"name":op_id,"role":"","shift":""})
    tot = dict(totals)
    tot["accuracy"] = round(tot["perfect"]/max(1,tot["picks"])*100,1)

    return jsonify({
        "operator": op_info, "totals": tot,
        "daily": [dict(r) for r in daily],
        "by_assembly": [{
            "wo_id":    r["work_order_id"],
            "wo_name":  ASSEMBLIES.get(r["work_order_id"],{}).get("name", r["work_order_id"]),
            "picks":    r["picks"], "errors": r["errors"], "avg_ms": round(r["avg_ms"]),
            "accuracy": round(r["perfect"]/max(1,r["picks"])*100,1)
        } for r in by_wo],
        "worst_trays": [dict(r) for r in tray],
    })

@app.route('/api/reset', methods=['POST'])
def reset():
    today = datetime.now().strftime("%Y-%m-%d")
    with get_db() as db:
        db.execute("DELETE FROM pick_events WHERE timestamp>=?", (today,))
        db.execute("DELETE FROM wrong_tray_events WHERE timestamp>=?", (today,))
        db.execute("UPDATE work_orders SET status='pending',activated_at=NULL,completed_at=NULL")
        db.execute("DELETE FROM demo_state")
    broadcast("reset", {})
    return jsonify({"status": "reset"})

@app.route('/api/stream')
def stream():
    """Single SSE stream — both portals connect here."""
    q = queue.Queue(maxsize=50)
    with sse_lock:
        sse_clients.append(q)
    def gen():
        try:
            yield "event: connected\ndata: {}\n\n"
            while True:
                try:    yield q.get(timeout=20)
                except: yield "event: heartbeat\ndata: {}\n\n"
        except GeneratorExit:
            with sse_lock:
                if q in sse_clients: sse_clients.remove(q)
    return Response(gen(), mimetype='text/event-stream',
                    headers={'Cache-Control':'no-cache','X-Accel-Buffering':'no',
                             'Access-Control-Allow-Origin':'*'})


@app.route('/api/db_info')
def db_info():
    with get_db() as db:
        picks    = db.execute('SELECT COUNT(*) FROM pick_events').fetchone()[0]
        wrong    = db.execute('SELECT COUNT(*) FROM wrong_tray_events').fetchone()[0]
        ops      = db.execute('SELECT COUNT(*) FROM operators').fetchone()[0]
        earliest = db.execute('SELECT MIN(timestamp) FROM pick_events').fetchone()[0]
        latest   = db.execute('SELECT MAX(timestamp) FROM pick_events').fetchone()[0]
    return jsonify({
        'db_path':           DB_PATH,
        'total_pick_events': picks,
        'wrong_tray_events': wrong,
        'operators':         ops,
        'data_from':         earliest,
        'data_to':           latest,
        'message':           'Data persists across restarts'
    })

@app.route('/api/seed_demo', methods=['POST'])
def seed_demo():
    with get_db() as db:
        existing = db.execute('SELECT COUNT(*) FROM pick_events').fetchone()[0]
        if existing >= 50:
            return jsonify({'status': 'skipped',
                'message': f'{existing} events already stored — not overwriting'})
        _seed_history(db)
    return jsonify({'status': 'seeded', 'message': '30-day demo history seeded'})

@app.after_request
def cors(r):
    r.headers['Access-Control-Allow-Origin']  = '*'
    r.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    r.headers['Access-Control-Allow-Methods'] = 'GET,POST,OPTIONS'
    return r

@app.route('/api/<path:p>', methods=['OPTIONS'])
def opts(p): return '', 204

if __name__ == '__main__':
    init_db()
    print("\n" + "═"*52)
    print("  SynTrack — Assembly Intelligence Platform")
    print("  SyndiCAT_E5 · Caterpillar Tech Challenge 2026")
    print("═"*52)
    print("  Operator portal:   http://localhost:5000")
    print("  Supervisor portal: http://localhost:5000/super")
    print("═"*52 + "\n")
    app.run(debug=False, host='0.0.0.0', port=5000, threaded=True)
