"""
reassignment.py
Core reassignment logic engine.

Functions:
  get_utilization(events, employees, tasks, query_date)
    → Scores every employee by calendar load + task load for a given date.

  suggest_reassignments(events, employees, tasks, query_date)
    → For every task whose assignee is unavailable, returns ranked candidates.

  execute_reassignment(tasks, task_id, new_assignee_id, db_path)
    → Writes the reassignment back to EmployeeDatabase.json and returns
      an audit record.

Scoring formula:
  candidate_score = (skill_match_count × 15) - utilization_pct - (cal_event_count × 3)

  Higher score = better fit.
  Ties broken by free_capacity (more capacity wins).
"""

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


# ── Utilization ───────────────────────────────────────────────────────────────

def get_utilization(
    events: List[Dict],
    employees: List[Dict],
    tasks: List[Dict],
    query_date: datetime,
) -> List[Dict]:
    """
    Returns every employee annotated with workload metrics, sorted from
    least-loaded (index 0) to most-loaded.

    Fields added to each employee dict:
      is_available         bool   — no OOF/all-day block on query_date
      active_task_count    int    — tasks in_progress or todo assigned to them
      active_task_hours    float  — sum of effort_hours for those tasks
      calendar_event_count int    — timed calendar events on query_date
      utilization_pct      int    — (active_tasks / max_tasks_per_day) × 100, capped at 100
      free_capacity        int    — max_tasks_per_day - active_task_count (min 0)
    """
    from normalizer import get_available_employees, get_events_for_date

    target_date = query_date.date()
    avail_result = get_available_employees(events, employees, query_date)
    available_ids = {str(e["id"]) for e in avail_result["available"]}

    # All calendar events on this day (as dicts from to_dict())
    day_events = get_events_for_date(events, query_date)
    # day_events is already a list of dicts when called from api context

    results = []
    for emp in employees:
        emp_id = str(emp["id"])
        max_tasks = emp.get("max_tasks_per_day", 4)

        # Active tasks assigned to this person
        active_tasks = [
            t for t in tasks
            if str(t.get("assigned_to_id", "")) == emp_id
            and t.get("status") in ("in_progress", "todo")
        ]
        task_count = len(active_tasks)
        task_hours = sum(float(t.get("effort_hours", 1)) for t in active_tasks)

        # Calendar events on this specific day (timed, not all-day)
        cal_events = [
            e for e in day_events
            if (e.get("employee_id") == emp_id or e.get("employee_email") == emp.get("email"))
            and not e.get("is_all_day", False)
        ]

        is_available = emp_id in available_ids
        utilization_pct = min(100, round((task_count / max(max_tasks, 1)) * 100))
        free_capacity = max(0, max_tasks - task_count)

        results.append({
            **emp,
            "is_available": is_available,
            "active_task_count": task_count,
            "active_task_hours": task_hours,
            "active_tasks": active_tasks,
            "calendar_event_count": len(cal_events),
            "utilization_pct": utilization_pct,
            "free_capacity": free_capacity,
        })

    # Sort: available first, then lowest utilization, then most free capacity
    results.sort(key=lambda e: (
        0 if e["is_available"] else 1,
        e["utilization_pct"],
        -e["free_capacity"],
    ))
    return results


# ── Candidate scoring ─────────────────────────────────────────────────────────

def _score_candidate(candidate: Dict, task: Dict) -> float:
    """
    Score = (skill_match × 15) - utilization_pct - (cal_events × 3)
    Higher is better.
    """
    required = set(task.get("required_skills", []))
    emp_skills = set(candidate.get("skills", []))
    skill_match = len(required & emp_skills)
    return (skill_match * 15) - candidate["utilization_pct"] - (candidate["calendar_event_count"] * 3)


# ── Suggest reassignments ─────────────────────────────────────────────────────

def suggest_reassignments(
    events: List[Dict],
    employees: List[Dict],
    tasks: List[Dict],
    query_date: datetime,
    top_n: int = 3,
) -> Dict[str, Any]:
    """
    For every active task whose assignee is unavailable on query_date,
    return a ranked list of up to top_n replacement candidates.

    Returns:
    {
      "date": "YYYY-MM-DD",
      "total_tasks_checked": int,
      "needs_reassignment": int,
      "suggestions": [ ... ]
    }
    """
    target_str = query_date.date().isoformat()
    utilization = get_utilization(events, employees, tasks, query_date)
    util_map = {str(e["id"]): e for e in utilization}
    unavailable_ids = {str(e["id"]) for e in utilization if not e["is_available"]}
    available_pool = [e for e in utilization if e["is_available"] and e["free_capacity"] > 0]

    active_tasks = [t for t in tasks if t.get("status") in ("in_progress", "todo")]
    suggestions = []

    for task in active_tasks:
        assignee_id = str(task.get("assigned_to_id", ""))
        assignee_info = util_map.get(assignee_id, {})

        if assignee_id not in unavailable_ids:
            # Assignee is available — still include in response for full picture
            suggestions.append({
                "task": _serialize_task(task),
                "current_assignee": _serialize_emp(assignee_info),
                "needs_reassignment": False,
                "reason": None,
                "recommendations": [],
            })
            continue

        # Build candidate list for this task
        required_skills = set(task.get("required_skills", []))
        candidates = []
        for emp in available_pool:
            if str(emp["id"]) == assignee_id:
                continue
            emp_skills = set(emp.get("skills", []))
            skill_match = len(required_skills & emp_skills)
            skill_gap = sorted(required_skills - emp_skills)
            score = _score_candidate(emp, task)
            candidates.append({
                **_serialize_emp(emp),
                "skill_match_count": skill_match,
                "skill_match_pct": round(len(required_skills & emp_skills) / max(len(required_skills), 1) * 100),
                "skill_gap": skill_gap,
                "score": round(score, 1),
            })

        candidates.sort(key=lambda c: (-c["score"], -c["free_capacity"]))

        suggestions.append({
            "task": _serialize_task(task),
            "current_assignee": _serialize_emp(assignee_info),
            "needs_reassignment": True,
            "reason": f"Assignee unavailable on {target_str}",
            "recommendations": candidates[:top_n],
        })

    # Sort: needs reassignment first, then by task priority
    priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    suggestions.sort(key=lambda s: (
        0 if s["needs_reassignment"] else 1,
        priority_order.get(s["task"].get("priority", "low"), 9),
    ))

    needs_reassignment = sum(1 for s in suggestions if s["needs_reassignment"])

    return {
        "date": target_str,
        "total_tasks_checked": len(active_tasks),
        "needs_reassignment": needs_reassignment,
        "suggestions": suggestions,
        "utilization_snapshot": [_serialize_emp(e) for e in utilization],
    }


# ── Execute reassignment ──────────────────────────────────────────────────────

def execute_reassignment(
    task_id: str,
    new_assignee_id: int,
    db_path: str,
    reason: str = "Manual reassignment via dashboard",
) -> Dict[str, Any]:
    """
    Writes the new assignee into EmployeeDatabase.json for the given task_id.
    Returns an audit record with before/after state.

    Raises ValueError if task_id or new_assignee_id not found.
    """
    with open(db_path, "r", encoding="utf-8") as f:
        db = json.load(f)

    tasks = db.get("tasks", [])
    employees = db.get("employees", [])

    # Find task
    task = next((t for t in tasks if t["id"] == task_id), None)
    if not task:
        raise ValueError(f"Task {task_id!r} not found in database")

    # Find new assignee
    new_emp = next((e for e in employees if e["id"] == new_assignee_id), None)
    if not new_emp:
        raise ValueError(f"Employee ID {new_assignee_id} not found in database")

    # Find old assignee
    old_emp = next((e for e in employees if e["id"] == task.get("assigned_to_id")), None)

    old_assignee_id = task.get("assigned_to_id")
    task["assigned_to_id"] = new_assignee_id
    task["last_reassigned"] = datetime.now(timezone.utc).isoformat()
    task["reassignment_reason"] = reason

    # Write back
    with open(db_path, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=2)

    audit = {
        "task_id": task_id,
        "task_title": task.get("title"),
        "from_employee_id": old_assignee_id,
        "from_employee_name": f"{old_emp['first_name']} {old_emp['last_name']}" if old_emp else "Unknown",
        "to_employee_id": new_assignee_id,
        "to_employee_name": f"{new_emp['first_name']} {new_emp['last_name']}",
        "reason": reason,
        "executed_at": datetime.now(timezone.utc).isoformat(),
    }
    return audit


# ── Helpers ───────────────────────────────────────────────────────────────────

def _serialize_task(task: Dict) -> Dict:
    return {
        "id": task.get("id"),
        "title": task.get("title"),
        "description": task.get("description"),
        "status": task.get("status"),
        "priority": task.get("priority"),
        "required_skills": task.get("required_skills", []),
        "effort_hours": task.get("effort_hours"),
        "due_date": task.get("due_date"),
        "assigned_to_id": task.get("assigned_to_id"),
        "last_reassigned": task.get("last_reassigned"),
        "reassignment_reason": task.get("reassignment_reason"),
    }


def _serialize_emp(emp: Dict) -> Dict:
    return {
        "id": emp.get("id"),
        "first_name": emp.get("first_name"),
        "last_name": emp.get("last_name"),
        "email": emp.get("email"),
        "role": emp.get("role"),
        "skills": emp.get("skills", []),
        "max_tasks_per_day": emp.get("max_tasks_per_day", 4),
        "is_available": emp.get("is_available"),
        "utilization_pct": emp.get("utilization_pct"),
        "free_capacity": emp.get("free_capacity"),
        "active_task_count": emp.get("active_task_count"),
        "calendar_event_count": emp.get("calendar_event_count"),
    }
