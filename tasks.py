from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Literal, Any
from datetime import datetime, timedelta
from dateutil import tz
from dateutil.parser import isoparse

from main import PEOPLE_BY_ID, seed_people_if_empty, compute_status_for_person  # type: ignore


router = APIRouter(prefix="/tasks", tags=["tasks"])

Priority = Literal["P0", "P1", "P2", "P3"]


class Task(BaseModel):
    task_id: str
    title: str
    priority: Priority
    due: datetime
    owner_id: str
    team: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    required_skills: List[str] = Field(default_factory=list)


# In-memory task store
TASKS: Dict[str, Task] = {}


def seed_tasks_if_empty():
    """
    Seed a few tasks for demo. Make sure these line up with your Person IDs/teams/skills.
    """
    if TASKS:
        return

    seed_people_if_empty()
    now = datetime.now(tz=tz.gettz("America/New_York"))

    # If your people IDs differ, adjust these owner_id values.
    # By default, earlier code seeded p1/p2/p3.
    TASKS["t1"] = Task(
        task_id="t1",
        title="Fix checkout outage",
        priority="P0",
        due=now + timedelta(hours=6),
        owner_id="p1",
        team="Payments",
        tags=["incident", "payments"],
        required_skills=["payments"],
    )
    TASKS["t2"] = Task(
        task_id="t2",
        title="Deploy API rate-limit patch",
        priority="P1",
        due=now + timedelta(hours=30),
        owner_id="p2",
        team="Payments",
        tags=["api", "mitigation"],
        required_skills=["api"],
    )
    TASKS["t3"] = Task(
        task_id="t3",
        title="K8s node pool upgrade",
        priority="P2",
        due=now + timedelta(days=5),
        owner_id="p3",
        team="Platform",
        tags=["infra"],
        required_skills=["k8s"],
    )


def _parse_at(at: Optional[str]) -> datetime:
    if at:
        at_dt = isoparse(at)
        if at_dt.tzinfo is None:
            at_dt = at_dt.replace(tzinfo=tz.gettz("America/New_York"))
        return at_dt
    return datetime.now(tz=tz.gettz("America/New_York"))


def _is_high_priority(priority: str) -> bool:
    return priority in ("P0", "P1")


def _build_status_map(at_dt: datetime):
    seed_people_if_empty()
    return {pid: compute_status_for_person(p, at_dt) for pid, p in PEOPLE_BY_ID.items()}


def _build_load_map() -> Dict[str, int]:
    load: Dict[str, int] = {}
    for t in TASKS.values():
        load[t.owner_id] = load.get(t.owner_id, 0) + 1
    return load


@router.get("")
def list_tasks():
    seed_tasks_if_empty()
    return {"count": len(TASKS), "tasks": list(TASKS.values())}


@router.get("/at-risk")
def get_at_risk_tasks(window_hours: int = 48, at: Optional[str] = None):
    seed_tasks_if_empty()

    at_dt = _parse_at(at)
    window_end = at_dt + timedelta(hours=window_hours)

    status_by_person = _build_status_map(at_dt)

    items: List[Dict[str, Any]] = []
    for task in TASKS.values():
        owner_status = status_by_person.get(task.owner_id)
        if not owner_status:
            continue

        due_soon = task.due <= window_end
        high = _is_high_priority(task.priority)

        if (due_soon or high) and owner_status.status in ("OFF", "TENTATIVE"):
            items.append(
                {
                    "task": task,
                    "owner_status": owner_status.status,
                    "owner_reason": owner_status.reason,
                }
            )

    return {"at": at_dt, "count": len(items), "items": items}


class SuggestRequest(BaseModel):
    task_ids: Optional[List[str]] = None
    at: Optional[datetime] = None
    top_k: int = 3


@router.post("/suggest-reassignments")
def suggest_reassignments(req: SuggestRequest):
    seed_tasks_if_empty()

    at_dt = req.at or datetime.now(tz=tz.gettz("America/New_York"))
    status_by_person = _build_status_map(at_dt)
    load_by_person = _build_load_map()

    # choose task set
    if req.task_ids:
        tasks = [TASKS[tid] for tid in req.task_ids if tid in TASKS]
    else:
        tasks = list(TASKS.values())

    def score_candidate(task: Task, person_id: str) -> tuple[int, List[str]]:
        person = PEOPLE_BY_ID[person_id]
        st = status_by_person[person_id]

        # availability gate
        if st.status != "AVAILABLE":
            return -10**9, ["not available"]

        score = 0
        reasons: List[str] = []

        # same team bonus
        if task.team and person.team and task.team == person.team:
            score += 5
            reasons.append("same team")

        # skill overlap
        skill_matches = len(set(task.required_skills) & set(person.skills))
        if skill_matches:
            score += 3 * skill_matches
            reasons.append(f"skills match ({skill_matches})")

        # simple tag overlap (you can refine later)
        tag_matches = len(set(task.tags) & set(person.skills))
        if tag_matches:
            score += 2 * tag_matches
            reasons.append(f"tag overlap ({tag_matches})")

        # prefer under-utilized
        load = load_by_person.get(person_id, 0)
        score -= 2 * load
        reasons.append(f"current load ({load})")

        return score, reasons

    results = []
    for task in tasks:
        owner_st = status_by_person.get(task.owner_id)
        # Only suggest if the task is currently at risk / owner isn't available
        if owner_st and owner_st.status == "AVAILABLE":
            continue

        candidates = []
        for pid in PEOPLE_BY_ID.keys():
            if pid == task.owner_id:
                continue
            sc, reasons = score_candidate(task, pid)
            if sc > -10**8:
                p = PEOPLE_BY_ID[pid]
                candidates.append((sc, p, reasons))

        candidates.sort(key=lambda x: x[0], reverse=True)
        top = candidates[: max(1, req.top_k)]

        results.append(
            {
                "task_id": task.task_id,
                "task_title": task.title,
                "priority": task.priority,
                "due": task.due,
                "current_owner_id": task.owner_id,
                "current_owner_status": owner_st.status if owner_st else "UNKNOWN",
                "suggestions": [
                    {
                        "person_id": p.person_id,
                        "name": p.name,
                        "email": p.email,
                        "score": sc,
                        "reasons": reasons,
                    }
                    for (sc, p, reasons) in top
                ],
            }
        )

    return {"at": at_dt, "results": results}


class ReassignRequest(BaseModel):
    task_id: str
    new_owner_id: str


@router.post("/reassign")
def reassign_task(req: ReassignRequest):
    seed_tasks_if_empty()

    if req.task_id not in TASKS:
        raise HTTPException(status_code=404, detail="Task not found")
    if req.new_owner_id not in PEOPLE_BY_ID:
        raise HTTPException(status_code=404, detail="Person not found")

    t = TASKS[req.task_id]
    TASKS[req.task_id] = t.model_copy(update={"owner_id": req.new_owner_id})
    return {"ok": True, "task": TASKS[req.task_id]}


@router.post("/debug/reset-tasks")
def reset_tasks():
    TASKS.clear()
    return {"ok": True, "count": len(TASKS)}