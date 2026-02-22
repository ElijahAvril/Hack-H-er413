import json
import random
import string
from datetime import datetime, timedelta, timezone

# Optional: use IANA timezone if available (pip install tzdata on Windows).
try:
    from zoneinfo import ZoneInfo
    HAS_ZONEINFO = True
except Exception:
    HAS_ZONEINFO = False


EMPLOYEES_PATH = "EmployeeDatabase.json"

GOOGLE_OUT = "google_calendar_events.json"
MS_OUT = "microsoft_calendar_events.json"

# Timeline controls
DAYS_OUT = 14
EVENTS_PER_PERSON_MIN = 1
EVENTS_PER_PERSON_MAX = 3
OOO_PROBABILITY = 0.25

# Google calendar metadata
GOOGLE_TZ = "America/New_York"
GOOGLE_TZ_OFFSET_FALLBACK = "-05:00"  # used if zoneinfo isn't available
GOOGLE_CALENDAR_ID = "fake-team-calendar@group.calendar.google.com"
GOOGLE_CALENDAR_NAME = "TeamCalendar"
GOOGLE_CREATOR_EMAIL = "seed@contoso.com"

# Microsoft calendar metadata
MS_TZ = "Pacific Standard Time"
MS_USER_ID = "00000000-0000-0000-0000-000000000000"

MEETING_TITLES = [
    "Standup", "1:1", "Design Review", "Client Call",
    "Sprint Planning", "Retro", "Demo", "Project Sync",
    "Engineering Sync", "All Hands"
]
OOO_TITLES = ["OOO", "PTO", "Vacation", "Sick Leave", "Out of Office"]


def load_employees(path: str) -> list[dict]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    emps = data.get("employees", [])
    if not isinstance(emps, list) or len(emps) < 20:
        raise ValueError("employees.json must contain at least 20 employees under key 'employees'")
    return emps


def now_utc_iso_ms() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def random_id(n: int = 26) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return "".join(random.choice(alphabet) for _ in range(n))


def fake_etag_num() -> str:
    # Google sample: "\"3543460762602238\"" (string with quotes inside)
    num = random.randint(10**15, 10**16 - 1)
    return f"\"{num}\""


def iso_google_datetime(dt_local: datetime) -> str:
    """
    Return RFC3339 with offset like 2026-02-21T22:30:00-05:00.
    If zoneinfo exists, compute offset properly; otherwise append fallback.
    """
    if HAS_ZONEINFO:
        tz = ZoneInfo(GOOGLE_TZ)
        aware = dt_local.replace(tzinfo=tz)
        return aware.isoformat(timespec="seconds")
    return dt_local.strftime("%Y-%m-%dT%H:%M:%S") + GOOGLE_TZ_OFFSET_FALLBACK


def pick_random_day_local(start_day_local: datetime, days_out: int) -> datetime:
    return (start_day_local + timedelta(days=random.randint(0, days_out - 1))).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def make_google_all_day_ooo(employee: dict, day_local: datetime) -> dict:
    event_id = random_id()
    created = now_utc_iso_ms()
    updated = now_utc_iso_ms()
    title = random.choice(OOO_TITLES)

    # Google all-day uses start.date and end.date (end is exclusive)
    start_date = day_local.date().isoformat()
    end_date = (day_local.date() + timedelta(days=random.choice([1, 2]))).isoformat()

    return {
        "kind": "calendar#event",
        "etag": fake_etag_num(),
        "id": event_id,
        "status": "confirmed",
        "htmlLink": f"https://www.google.com/calendar/event?eid=fake-{event_id}",
        "created": created,
        "updated": updated,
        "summary": f"{title} - {employee['first_name']} {employee['last_name']}",
        "creator": {"email": employee["email"]},
        "organizer": {"email": GOOGLE_CALENDAR_ID, "displayName": GOOGLE_CALENDAR_NAME, "self": True},
        "start": {"date": start_date},
        "end": {"date": end_date},
        "transparency": "transparent",
        "iCalUID": f"{event_id}@google.com",
        "sequence": 0,
        "eventType": "default",
        # helpful for your normalization/debugging
        "extendedProperties": {
            "private": {
                "employeeEmail": employee["email"],
                "employeeId": str(employee["id"]),
                "availabilityKind": "oof"
            }
        }
    }


def make_google_timed_busy(employee: dict, day_local: datetime) -> dict:
    event_id = random_id()
    created = now_utc_iso_ms()
    updated = now_utc_iso_ms()

    # Workday meeting
    start_hour = random.randint(9, 16)
    start_min = random.choice([0, 30])
    duration = random.choice([30, 45, 60, 90])

    start_dt = day_local.replace(hour=start_hour, minute=start_min)
    end_dt = start_dt + timedelta(minutes=duration)

    title = random.choice(MEETING_TITLES)

    return {
        "kind": "calendar#event",
        "etag": fake_etag_num(),
        "id": event_id,
        "status": "confirmed",
        "htmlLink": f"https://www.google.com/calendar/event?eid=fake-{event_id}",
        "created": created,
        "updated": updated,
        "summary": f"{title} - {employee['first_name']}",
        "creator": {"email": employee["email"]},
        "organizer": {"email": GOOGLE_CALENDAR_ID, "displayName": GOOGLE_CALENDAR_NAME, "self": True},
        "start": {"dateTime": iso_google_datetime(start_dt), "timeZone": GOOGLE_TZ},
        "end": {"dateTime": iso_google_datetime(end_dt), "timeZone": GOOGLE_TZ},
        "iCalUID": f"{event_id}@google.com",
        "sequence": 0,
        "eventType": "default",
        "extendedProperties": {
            "private": {
                "employeeEmail": employee["email"],
                "employeeId": str(employee["id"]),
                "availabilityKind": "busy"
            }
        }
    }


def make_msgraph_event(employee: dict, day_local: datetime, is_ooo: bool) -> dict:
    event_id = f"AAMk{random_id(12)}"
    title = random.choice(OOO_TITLES if is_ooo else MEETING_TITLES)

    if is_ooo:
        # All-day OOO block using showAs="oof"
        start_dt = day_local.replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = start_dt + timedelta(days=random.choice([1, 2]))
        return {
            "@odata.type": "#microsoft.graph.event",
            "@odata.etag": f"W/\"{random_id(22)}\"",
            "id": event_id,
            "subject": f"{title} - {employee['first_name']} {employee['last_name']}",
            "bodyPreview": "Out of office block.",
            "isAllDay": True,
            "showAs": "oof",
            "start": {"dateTime": start_dt.strftime("%Y-%m-%dT%H:%M:%S"), "timeZone": MS_TZ},
            "end": {"dateTime": end_dt.strftime("%Y-%m-%dT%H:%M:%S"), "timeZone": MS_TZ},
            "organizer": {
                "emailAddress": {
                    "name": f"{employee['first_name']} {employee['last_name']}",
                    "address": employee["email"]
                }
            },
            "attendees": [],
            "location": {"displayName": "N/A"},
            "lastModifiedDateTime": now_utc_iso_ms(),
            "extensions": [
                {
                    "@odata.type": "microsoft.graph.openTypeExtension",
                    "extensionName": "com.contoso.availability",
                    "employeeId": str(employee["id"]),
                    "employeeEmail": employee["email"],
                    "availabilityKind": "oof"
                }
            ]
        }

    # Timed busy meeting
    start_hour = random.randint(9, 16)
    start_min = random.choice([0, 30])
    duration = random.choice([30, 45, 60, 90])

    start_dt = day_local.replace(hour=start_hour, minute=start_min)
    end_dt = start_dt + timedelta(minutes=duration)

    return {
        "@odata.type": "#microsoft.graph.event",
        "@odata.etag": f"W/\"{random_id(22)}\"",
        "id": event_id,
        "subject": f"{title} - {employee['first_name']}",
        "bodyPreview": "Scheduled meeting.",
        "isAllDay": False,
        "showAs": "busy",
        "start": {"dateTime": start_dt.strftime("%Y-%m-%dT%H:%M:%S"), "timeZone": MS_TZ},
        "end": {"dateTime": end_dt.strftime("%Y-%m-%dT%H:%M:%S"), "timeZone": MS_TZ},
        "organizer": {
            "emailAddress": {
                "name": f"{employee['first_name']} {employee['last_name']}",
                "address": employee["email"]
            }
        },
        "attendees": [],
        "location": {"displayName": random.choice(["Conf Room A", "Conf Room B", "Zoom", "Teams"])},
        "lastModifiedDateTime": now_utc_iso_ms(),
        "extensions": [
            {
                "@odata.type": "microsoft.graph.openTypeExtension",
                "extensionName": "com.contoso.availability",
                "employeeId": str(employee["id"]),
                "employeeEmail": employee["email"],
                "availabilityKind": "busy"
            }
        ]
    }


def main():
    employees = load_employees(EMPLOYEES_PATH)
    random.shuffle(employees)

    google_emps = employees[:10]
    ms_emps = employees[10:20]

    # Anchor to today in local time (naive), generate forward
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    # ---- GOOGLE JSON (events.list style) ----
    google_items = []
    for emp in google_emps:
        n = random.randint(EVENTS_PER_PERSON_MIN, EVENTS_PER_PERSON_MAX)
        for _ in range(n):
            day = pick_random_day_local(today, DAYS_OUT)
            if random.random() < OOO_PROBABILITY:
                google_items.append(make_google_all_day_ooo(emp, day))
            else:
                google_items.append(make_google_timed_busy(emp, day))

    # Sort by start (all-day by date)
    def google_sort_key(ev: dict):
        s = ev.get("start", {})
        if "dateTime" in s:
            return s["dateTime"]
        return s.get("date", "") + "T00:00:00"

    google_items.sort(key=google_sort_key)

    google_payload = {
        "kind": "calendar#events",
        "etag": "\"p33nqhpnqijm940o\"",
        "summary": GOOGLE_CALENDAR_NAME,
        "description": "",
        "updated": now_utc_iso_ms(),
        "timeZone": GOOGLE_TZ,
        "accessRole": "reader",
        "defaultReminders": [],
        "nextSyncToken": "CO-o5vqU7JIDEAAYASDK-FAKE-TOKEN==",
        "items": google_items
    }

    with open(GOOGLE_OUT, "w", encoding="utf-8") as f:
        json.dump(google_payload, f, indent=2)

    # ---- MICROSOFT JSON (/events response style) ----
    ms_items = []
    for emp in ms_emps:
        n = random.randint(EVENTS_PER_PERSON_MIN, EVENTS_PER_PERSON_MAX)
        for _ in range(n):
            day = pick_random_day_local(today, DAYS_OUT)
            is_ooo = (random.random() < OOO_PROBABILITY)
            ms_items.append(make_msgraph_event(emp, day, is_ooo=is_ooo))

    # Sort by start.dateTime
    ms_items.sort(key=lambda ev: ev["start"]["dateTime"])

    ms_payload = {
        "@odata.context": f"https://graph.microsoft.com/v1.0/$metadata#users('{MS_USER_ID}')/events",
        "value": ms_items
    }

    with open(MS_OUT, "w", encoding="utf-8") as f:
        json.dump(ms_payload, f, indent=2)

    print(f"Wrote Google events for {len(google_emps)} employees to {GOOGLE_OUT} ({len(google_items)} events)")
    print(f"Wrote Microsoft events for {len(ms_emps)} employees to {MS_OUT} ({len(ms_items)} events)")

    print("\n--- SAMPLE GOOGLE (first 1 item) ---")
    print(json.dumps(google_payload["items"][:1], indent=2))

    print("\n--- SAMPLE MICROSOFT (first 1 item) ---")
    print(json.dumps(ms_payload["value"][:1], indent=2))


if __name__ == "__main__":
    main()