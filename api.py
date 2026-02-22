"""
api.py  —  TeamPulse local Flask server

Endpoints:
  GET  /api/status
  GET  /api/employees
  GET  /api/normalize          ?date=YYYY-MM-DD
  GET  /api/availability       ?date=YYYY-MM-DD
  GET  /api/tasks
  GET  /api/utilization        ?date=YYYY-MM-DD
  GET  /api/reassignments      ?date=YYYY-MM-DD
  POST /api/reassign           { task_id, new_assignee_id, reason? }
  POST /api/generate
  POST /api/fetch_google

Run:
  pip install flask flask-cors
  python api.py
  open http://localhost:5050
"""

import json, os, subprocess, sys
from datetime import datetime, timezone
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from normalizer import (
    normalize_google_json, normalize_google_csv,
    normalize_microsoft_json, get_available_employees, get_events_for_date,
)
from reassignment import suggest_reassignments, execute_reassignment, get_utilization

def p(f): return os.path.join(BASE_DIR, f)

EMPLOYEE_DB     = p("EmployeeDatabase.json")
GOOGLE_JSON     = p("google_calendar_events.json")
GOOGLE_CSV      = p("google_calendar_events.csv")
MS_JSON         = p("microsoft_calendar_events.json")
TIMELINE_SCRIPT = p("TimelineTest.py")
GOOGLE_SCRIPT   = p("GoogleCalender.py")

app = Flask(__name__, static_folder=BASE_DIR)
CORS(app)

# ── Loaders ──────────────────────────────────────────────────────────────────

def load_db():
    with open(EMPLOYEE_DB, "r", encoding="utf-8") as f:
        return json.load(f)

def load_employees():
    return load_db().get("employees", [])

def load_tasks():
    return load_db().get("tasks", [])

def load_events():
    events = []
    if os.path.exists(GOOGLE_JSON):
        events.extend(normalize_google_json(GOOGLE_JSON))
    if os.path.exists(GOOGLE_CSV):
        events.extend(normalize_google_csv(GOOGLE_CSV, load_employees()))
    if os.path.exists(MS_JSON):
        events.extend(normalize_microsoft_json(MS_JSON))
    return events

def parse_date(date_str):
    if not date_str:
        return datetime.now(timezone.utc)
    return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

def file_status():
    files = {
        "EmployeeDatabase.json": EMPLOYEE_DB,
        "google_calendar_events.json": GOOGLE_JSON,
        "google_calendar_events.csv": GOOGLE_CSV,
        "microsoft_calendar_events.json": MS_JSON,
        "TimelineTest.py": TIMELINE_SCRIPT,
        "GoogleCalender.py": GOOGLE_SCRIPT,
        "normalizer.py": p("normalizer.py"),
        "reassignment.py": p("reassignment.py"),
    }
    return {name: os.path.exists(fp) for name, fp in files.items()}

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(BASE_DIR, "calendar_dashboard.html")

@app.route("/api/status")
def api_status():
    return jsonify({"ok": True, "files": file_status(), "base_dir": BASE_DIR})

@app.route("/api/employees")
def api_employees():
    try:
        emps = load_employees()
        return jsonify({"employees": emps, "count": len(emps)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/tasks")
def api_tasks():
    try:
        tasks = load_tasks()
        status_filter = request.args.get("status")
        if status_filter:
            tasks = [t for t in tasks if t.get("status") == status_filter]
        return jsonify({"tasks": tasks, "count": len(tasks)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/normalize")
def api_normalize():
    try:
        events = load_events()
        date_str = request.args.get("date")
        if date_str:
            result = get_events_for_date(events, parse_date(date_str))
        else:
            result = [e.to_dict() for e in events]
        by_source, by_avail = {}, {}
        for e in events:
            by_source[e.source] = by_source.get(e.source, 0) + 1
            by_avail[e.availability] = by_avail.get(e.availability, 0) + 1
        return jsonify({
            "events": result,
            "count": len(result),
            "total_normalized": len(events),
            "by_source": by_source,
            "by_availability": by_avail,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/availability")
def api_availability():
    try:
        events = load_events()
        result = get_available_employees(events, load_employees(), parse_date(request.args.get("date")))
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/utilization")
def api_utilization():
    try:
        events = load_events()
        query_date = parse_date(request.args.get("date"))
        util = get_utilization(events, load_employees(), load_tasks(), query_date)
        return jsonify({"date": query_date.date().isoformat(), "utilization": util})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/reassignments")
def api_reassignments():
    try:
        events = load_events()
        query_date = parse_date(request.args.get("date"))
        result = suggest_reassignments(events, load_employees(), load_tasks(), query_date)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/reassign", methods=["POST"])
def api_reassign():
    try:
        body = request.get_json()
        task_id = body.get("task_id")
        new_assignee_id = int(body.get("new_assignee_id"))
        reason = body.get("reason", "Reassigned via dashboard")
        audit = execute_reassignment(task_id, new_assignee_id, EMPLOYEE_DB, reason)
        return jsonify({"success": True, "audit": audit})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/generate", methods=["POST"])
def api_generate():
    if not os.path.exists(TIMELINE_SCRIPT):
        return jsonify({"error": f"TimelineTest.py not found"}), 404
    try:
        r = subprocess.run([sys.executable, TIMELINE_SCRIPT],
                           capture_output=True, text=True, cwd=BASE_DIR, timeout=30)
        if r.returncode != 0:
            return jsonify({"error": r.stderr, "stdout": r.stdout}), 500
        return jsonify({"message": "Fake calendar data regenerated.",
                        "files_written": ["google_calendar_events.json", "microsoft_calendar_events.json"],
                        "output": r.stdout.strip()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/fetch_google", methods=["POST"])
def api_fetch_google():
    if not os.path.exists(GOOGLE_SCRIPT):
        return jsonify({"error": "GoogleCalender.py not found"}), 404
    try:
        r = subprocess.run([sys.executable, GOOGLE_SCRIPT],
                           capture_output=True, text=True, cwd=BASE_DIR, timeout=60)
        if r.returncode != 0:
            return jsonify({"error": r.stderr, "stdout": r.stdout}), 500
        return jsonify({"message": "Google Calendar data fetched.",
                        "files_written": ["google_calendar_events.csv"],
                        "output": r.stdout.strip()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    print(f"\n TeamPulse API")
    print(f" Serving from : {BASE_DIR}")
    print(f" Dashboard    : http://localhost:5050")
    print(f" Network      : http://0.0.0.0:5050\n")
    for name, exists in file_status().items():
        print(f"  {'✓' if exists else '✗ MISSING':<12}{name}")
    print()
    app.run(debug=True, port=5050, host="0.0.0.0")
