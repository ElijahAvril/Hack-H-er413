from __future__ import annotations

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Literal, Tuple
from datetime import datetime, timezone
from dateutil import tz
from dateutil.parser import isoparse

from icalendar import Calendar

app = FastAPI(title="Availability Hub Backend (ICS)")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Types
Status = Literal["AVAILABLE", "OFF", "TENTATIVE", "UNKNOWN"]
SignalType = Literal["OOO", "PTO", "MEETING", "UNKNOWN"]
SourceType = Literal["calendar_ics", "timeoff_app", "coverage", "manual"]

OOO_KEYWORDS = [
    "ooo", "out of office", "out-of-office", "vacation", "pto", "sick", "leave", "holiday"
]


class Person(BaseModel):
    person_id: str
    name: str
    email: str
    team: Optional[str] = None
    role: Optional[str] = None
    skills: List[str] = Field(default_factory=list)


class AvailabilitySignal(BaseModel):
    signal_id: str
    person_id: str
    start: datetime
    end: datetime
    type: SignalType
    source: SourceType
    confidence: float = 0.8
    raw_summary: Optional[str] = None


class AvailabilityStatus(BaseModel):
    person_id: str
    name: str
    email: str
    status: Status
    reason: str
    until: Optional[datetime] = None
    confidence: float = 0.0
    sources: List[SourceType] = Field(default_factory=list)


class AvailabilityResponse(BaseModel):
    at: datetime
    summary: Dict[str, int]
    people: List[AvailabilityStatus]


# --- In-memory stores 
PEOPLE_BY_EMAIL: Dict[str, Person] = {}
PEOPLE_BY_ID: Dict[str, Person] = {}
SIGNALS: List[AvailabilitySignal] = []


def seed_people_if_empty():
    """Create a small roster so demo works immediately."""
    if PEOPLE_BY_ID:
        return

    roster = [
        Person(person_id="p1", name="Alice Kim", email="alice@example.com", team="Payments", skills=["payments", "oncall"]),
        Person(person_id="p2", name="Bob Jones", email="bob@example.com", team="Payments", skills=["payments", "api"]),
        Person(person_id="p3", name="Cara Patel", email="cara@example.com", team="Platform", skills=["infra", "k8s"]),
    ]
    for p in roster:
        PEOPLE_BY_EMAIL[p.email.lower()] = p
        PEOPLE_BY_ID[p.person_id] = p


def is_ooo_summary(summary: str) -> bool:
    s = (summary or "").strip().lower()
    return any(k in s for k in OOO_KEYWORDS)


def safe_parse_dt(component) -> Optional[datetime]:
    """
    icalendar returns dt as date or datetime.
    Convert to timezone-aware datetime when possible.
    """
    if component is None:
        return None
    dt = component.dt

    # If it's a date (all-day event), treat as start-of-day in local tz
    if hasattr(dt, "year") and not hasattr(dt, "hour"):
        # dt is a date (no time); convert to datetime midnight local
        local = tz.gettz("America/New_York")
        return datetime(dt.year, dt.month, dt.day, 0, 0, 0, tzinfo=local)

    # dt is datetime
    if dt.tzinfo is None:
        # assume local if missing tz
        local = tz.gettz("America/New_York")
        return dt.replace(tzinfo=local)
    return dt


def upsert_person_by_email(email: str) -> Person:
    """
    If email isn't in roster, auto-create a person.
    This helps when the ICS contains real emails.
    """
    email_l = (email or "").strip().lower()
    if not email_l:
        raise HTTPException(status_code=400, detail="Missing email for person mapping")

    existing = PEOPLE_BY_EMAIL.get(email_l)
    if existing:
        return existing

    new_id = f"p{len(PEOPLE_BY_ID) + 1}"
    p = Person(person_id=new_id, name=email_l.split("@")[0].title(), email=email_l)
    PEOPLE_BY_EMAIL[email_l] = p
    PEOPLE_BY_ID[new_id] = p
    return p


def choose_person_email_for_event(event) -> Optional[str]:
    """
    - If ORGANIZER exists with mailto:
    - Else try ATTENDEE list
    - Else None (event can't be mapped)
    """
    org = event.get("ORGANIZER")
    if org:
        # often like 'MAILTO:alice@example.com'
        val = str(org)
        if "mailto:" in val.lower():
            return val.split(":")[-1].strip()

    attendees = event.get("ATTENDEE")
    if attendees:
        # can be a list or a single item
        if isinstance(attendees, list):
            for a in attendees:
                s = str(a)
                if "mailto:" in s.lower():
                    return s.split(":")[-1].strip()
        else:
            s = str(attendees)
            if "mailto:" in s.lower():
                return s.split(":")[-1].strip()

    return None


def compute_status_for_person(person: Person, at: datetime) -> AvailabilityStatus:
    # find signals overlapping 'at'
    overlapping: List[AvailabilitySignal] = [
        s for s in SIGNALS
        if s.person_id == person.person_id and s.start <= at < s.end
    ]

    if not overlapping:
        return AvailabilityStatus(
            person_id=person.person_id,
            name=person.name,
            email=person.email,
            status="UNKNOWN",
            reason="No signals",
            confidence=0.0,
            sources=[],
        )

    # choose strongest signal
    # Prefer OOO/PTO > meeting, then by confidence
    def rank(sig: AvailabilitySignal) -> Tuple[int, float]:
        type_weight = 2 if sig.type in ("OOO", "PTO") else (1 if sig.type == "MEETING" else 0)
        return (type_weight, sig.confidence)

    best = sorted(overlapping, key=rank, reverse=True)[0]
    sources = sorted(list({s.source for s in overlapping}))
    until = best.end

    if best.type in ("OOO", "PTO") and best.confidence >= 0.8:
        status: Status = "OFF"
    elif best.type in ("OOO", "PTO"):
        status = "TENTATIVE"
    else:
        status = "AVAILABLE"

    reason = f"{best.source}: {best.raw_summary or best.type}"
    return AvailabilityStatus(
        person_id=person.person_id,
        name=person.name,
        email=person.email,
        status=status,
        reason=reason,
        until=until,
        confidence=best.confidence,
        sources=sources,
    )


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/people")
def list_people():
    seed_people_if_empty()
    return list(PEOPLE_BY_ID.values())


@app.post("/ingest/calendar_ics")
async def ingest_calendar_ics(file: UploadFile = File(...)):
    raw = await file.read()
    cal = Calendar.from_ical(raw)

    added = 0
    skipped = []  # store reasons for skipped VEVENTs

    for component in cal.walk():
        if component.name != "VEVENT":
            continue

        summary = str(component.get("SUMMARY") or "")
        start_raw = component.get("DTSTART")
        end_raw = component.get("DTEND")

        start = safe_parse_dt(start_raw)
        end = safe_parse_dt(end_raw)

        email = choose_person_email_for_event(component)

        # Diagnose skips 
        if not email:
            skipped.append({
                "reason": "no_email",
                "summary": summary,
                "dtstart": str(start_raw),
                "dtend": str(end_raw),
                "organizer": str(component.get("ORGANIZER")),
                "attendee": str(component.get("ATTENDEE")),
            })
            continue

        if not start or not end:
            skipped.append({
                "reason": "no_time",
                "summary": summary,
                "dtstart": str(start_raw),
                "dtend": str(end_raw),
                "email": email,
            })
            continue

        person = upsert_person_by_email(email)

        if is_ooo_summary(summary):
            sig_type = "OOO"
            conf = 0.9
        else:
            sig_type = "MEETING"
            conf = 0.4
           

        SIGNALS.append(
            AvailabilitySignal(
                signal_id=f"ics-{file.filename}-{added}",
                person_id=person.person_id,
                start=start,
                end=end,
                type=sig_type,
                source="calendar_ics",
                confidence=conf,
                raw_summary=summary[:200],
            )
        )
        added += 1

    return {
        "added_signals": added,
        "total_signals_now": len(SIGNALS),
        "skipped_count": len(skipped),
        "skipped": skipped[:5],  # show first 5
    }
@app.get("/")
def root():
    return {"message": "Availability Hub Backend Running"}
async def ingest_calendar_ics(file: UploadFile = File(...)):
    """
    Upload an .ics file and ingest OOO/PTO-like events.
    """
    seed_people_if_empty()

    if not file.filename.lower().endswith(".ics"):
        raise HTTPException(status_code=400, detail="Please upload an .ics file")

    raw = await file.read()
    try:
        cal = Calendar.from_ical(raw)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse ICS: {e}")

    added = 0
    for component in cal.walk():
        if component.name != "VEVENT":
            continue

        summary = str(component.get("SUMMARY") or "")
        start = safe_parse_dt(component.get("DTSTART"))
        end = safe_parse_dt(component.get("DTEND"))

        if not start or not end:
            continue

        email = choose_person_email_for_event(component)
        if not email:
        
            continue

        person = upsert_person_by_email(email)

        # classify event
        if is_ooo_summary(summary):
            sig_type: SignalType = "OOO"
            conf = 0.9
        else:
            sig_type = "MEETING"
            conf = 0.4

        sig = AvailabilitySignal(
            signal_id=f"ics-{file.filename}-{added}",
            person_id=person.person_id,
            start=start,
            end=end,
            type=sig_type,
            source="calendar_ics",
            confidence=conf,
            raw_summary=summary[:200],
        )
        SIGNALS.append(sig)
        added += 1

    return {"added_signals": added, "total_signals": len(SIGNALS)}

@app.get("/debug/signals")
def debug_signals():
    return SIGNALS

@app.get("/availability", response_model=AvailabilityResponse)
def get_availability(at: Optional[str] = None):
    seed_people_if_empty()

    if at:
        at_dt = isoparse(at)
        if at_dt.tzinfo is None:
            at_dt = at_dt.replace(tzinfo=tz.gettz("America/New_York"))
    else:
        at_dt = datetime.now(tz=tz.gettz("America/New_York"))

    statuses = [compute_status_for_person(p, at_dt) for p in PEOPLE_BY_ID.values()]
    summary = {
        "available": sum(1 for s in statuses if s.status == "AVAILABLE"),
        "off": sum(1 for s in statuses if s.status == "OFF"),
        "tentative": sum(1 for s in statuses if s.status == "TENTATIVE"),
        "unknown": sum(1 for s in statuses if s.status == "UNKNOWN"),
    }

    return AvailabilityResponse(at=at_dt, summary=summary, people=statuses)