"""
normalizer.py
Normalizes Google Calendar events (JSON + CSV) and Microsoft Calendar events (JSON)
into a unified schema:

NormalizedEvent:
  event_id        str
  source          "google" | "microsoft"
  employee_id     str
  employee_email  str
  title           str
  availability    "busy" | "oof" | "free"
  start_utc       datetime (UTC, timezone-aware)
  end_utc         datetime (UTC, timezone-aware)
  is_all_day      bool
"""

import csv
import json
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict

# ──────────────────────────────────────────────
# Data class
# ──────────────────────────────────────────────

@dataclass
class NormalizedEvent:
    event_id: str
    source: str          # "google" | "microsoft"
    employee_id: str
    employee_email: str
    title: str
    availability: str    # "busy" | "oof" | "free"
    start_utc: datetime
    end_utc: datetime
    is_all_day: bool

    def to_dict(self) -> dict:
        d = asdict(self)
        d["start_utc"] = self.start_utc.isoformat()
        d["end_utc"] = self.end_utc.isoformat()
        return d


# ──────────────────────────────────────────────
# Timezone helpers
# ──────────────────────────────────────────────

_TZ_OFFSETS = {
    "Pacific Standard Time": -8,
    "Pacific Daylight Time": -7,
    "Mountain Standard Time": -7,
    "Mountain Daylight Time": -6,
    "Central Standard Time": -6,
    "Central Daylight Time": -5,
    "Eastern Standard Time": -5,
    "Eastern Daylight Time": -4,
    "UTC": 0,
}


def _ms_tz_to_offset(tz_name: str) -> timezone:
    hours = _TZ_OFFSETS.get(tz_name, 0)
    return timezone(timedelta(hours=hours))


def _parse_dt_to_utc(dt_str: str, tz_hint: Optional[str] = None) -> datetime:
    """
    Parse an ISO 8601 datetime string to a UTC-aware datetime.
    Handles:
      - "2026-02-24T11:30:00-05:00"  (offset embedded)
      - "2026-02-24T11:30:00"        (naive → use tz_hint)
      - "2026-02-24"                 (date only → midnight UTC)
    """
    s = dt_str.strip()

    # Date-only
    if len(s) == 10:
        return datetime(int(s[:4]), int(s[5:7]), int(s[8:10]), tzinfo=timezone.utc)

    # Has offset in string
    for sep in ("+", "-"):
        # Find the offset part: look for +HH:MM or -HH:MM at end
        if "T" in s:
            body, _, tail = s.partition("T")
            # scan for offset in tail
            tail_stripped = tail
            for i, ch in enumerate(tail):
                if i > 0 and ch in ("+", "-"):
                    # offset starts at i
                    offset_str = tail[i:]
                    dt_naive_str = body + "T" + tail[:i]
                    try:
                        dt_naive = datetime.fromisoformat(dt_naive_str)
                    except ValueError:
                        break
                    sign = 1 if ch == "+" else -1
                    parts = offset_str[1:].split(":")
                    oh = int(parts[0])
                    om = int(parts[1]) if len(parts) > 1 else 0
                    tz_offset = timezone(timedelta(hours=sign * oh, minutes=sign * om))
                    aware = dt_naive.replace(tzinfo=tz_offset)
                    return aware.astimezone(timezone.utc)

    # Naive – use tz_hint
    try:
        dt_naive = datetime.fromisoformat(s)
    except ValueError:
        dt_naive = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")

    if tz_hint:
        tz = _ms_tz_to_offset(tz_hint)
        aware = dt_naive.replace(tzinfo=tz)
        return aware.astimezone(timezone.utc)

    # Assume UTC
    return dt_naive.replace(tzinfo=timezone.utc)


# ──────────────────────────────────────────────
# Google JSON normalizer
# ──────────────────────────────────────────────

def normalize_google_json(path: str) -> List[NormalizedEvent]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    items = data.get("items", [])
    results: List[NormalizedEvent] = []

    for ev in items:
        ext_priv = (ev.get("extendedProperties") or {}).get("private") or {}
        emp_id = ext_priv.get("employeeId", "")
        emp_email = ext_priv.get("employeeEmail", "") or (ev.get("creator") or {}).get("email", "")
        avail = ext_priv.get("availabilityKind", "busy")

        start_obj = ev.get("start") or {}
        end_obj = ev.get("end") or {}
        is_all_day = "date" in start_obj and "dateTime" not in start_obj

        start_str = start_obj.get("dateTime") or start_obj.get("date", "")
        end_str = end_obj.get("dateTime") or end_obj.get("date", "")
        tz_hint = start_obj.get("timeZone")

        if not start_str:
            continue

        try:
            start_utc = _parse_dt_to_utc(start_str, tz_hint)
            end_utc = _parse_dt_to_utc(end_str, tz_hint) if end_str else start_utc
        except Exception:
            continue

        results.append(NormalizedEvent(
            event_id=ev.get("id", ""),
            source="google",
            employee_id=emp_id,
            employee_email=emp_email,
            title=ev.get("summary", ""),
            availability=avail,
            start_utc=start_utc,
            end_utc=end_utc,
            is_all_day=is_all_day,
        ))

    return results


# ──────────────────────────────────────────────
# Google CSV normalizer
# ──────────────────────────────────────────────

def normalize_google_csv(path: str, employee_db: Optional[List[Dict]] = None) -> List[NormalizedEvent]:
    """
    The CSV from GoogleCalendar.py doesn't include employeeId directly.
    We match via summary text or a provided employee_db lookup.
    """
    results: List[NormalizedEvent] = []

    # Build email→id lookup
    email_to_id: Dict[str, str] = {}
    if employee_db:
        for emp in employee_db:
            email_to_id[emp["email"]] = str(emp["id"])

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            start_str = row.get("start", "")
            end_str = row.get("end", "")
            if not start_str:
                continue

            # Try to detect OOF from summary
            summary = row.get("summary", "")
            ooo_keywords = {"ooo", "pto", "vacation", "sick", "out of office"}
            avail = "oof" if any(k in summary.lower() for k in ooo_keywords) else "busy"

            # Try to extract employee email from description or creator
            emp_email = ""
            emp_id = ""

            try:
                start_utc = _parse_dt_to_utc(start_str)
                end_utc = _parse_dt_to_utc(end_str) if end_str else start_utc
            except Exception:
                continue

            is_all_day = len(start_str.strip()) == 10

            results.append(NormalizedEvent(
                event_id=row.get("id", ""),
                source="google_csv",
                employee_id=emp_id,
                employee_email=emp_email,
                title=summary,
                availability=avail,
                start_utc=start_utc,
                end_utc=end_utc,
                is_all_day=is_all_day,
            ))

    return results


# ──────────────────────────────────────────────
# Microsoft JSON normalizer
# ──────────────────────────────────────────────

def normalize_microsoft_json(path: str) -> List[NormalizedEvent]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    items = data.get("value", [])
    results: List[NormalizedEvent] = []

    for ev in items:
        # Extract employee info from extensions
        extensions = ev.get("extensions") or []
        emp_id = ""
        emp_email = ""
        avail_kind = ""

        for ext in extensions:
            if "employeeId" in ext:
                emp_id = str(ext["employeeId"])
                emp_email = ext.get("employeeEmail", "")
                avail_kind = ext.get("availabilityKind", "")
                break

        # Fallback to organizer
        if not emp_email:
            emp_email = ((ev.get("organizer") or {}).get("emailAddress") or {}).get("address", "")

        show_as = ev.get("showAs", "busy")
        if not avail_kind:
            avail_kind = "oof" if show_as == "oof" else "busy"

        start_obj = ev.get("start") or {}
        end_obj = ev.get("end") or {}
        is_all_day = ev.get("isAllDay", False)

        start_str = start_obj.get("dateTime", "")
        end_str = end_obj.get("dateTime", "")
        tz_hint = start_obj.get("timeZone")

        if not start_str:
            continue

        try:
            start_utc = _parse_dt_to_utc(start_str, tz_hint)
            end_utc = _parse_dt_to_utc(end_str, tz_hint) if end_str else start_utc
        except Exception:
            continue

        results.append(NormalizedEvent(
            event_id=ev.get("id", ""),
            source="microsoft",
            employee_id=emp_id,
            employee_email=emp_email,
            title=ev.get("subject", ""),
            availability=avail_kind,
            start_utc=start_utc,
            end_utc=end_utc,
            is_all_day=is_all_day,
        ))

    return results


# ──────────────────────────────────────────────
# Availability query
# ──────────────────────────────────────────────

def get_available_employees(
    events: List[NormalizedEvent],
    all_employees: List[Dict],
    query_date: datetime,
) -> Dict[str, Any]:
    """
    Return employees who have NO busy/oof events on query_date (UTC date).
    query_date: any datetime; we use its date() in UTC.
    """
    target_date = query_date.date()

    # Collect busy/oof employee IDs for the target date
    busy_ids = set()
    busy_emails = set()

    for ev in events:
        if ev.availability not in ("busy", "oof"):
            continue
        ev_start_date = ev.start_utc.date()
        ev_end_date = ev.end_utc.date()

        # Event overlaps target_date if start <= target < end (or same day)
        if ev.is_all_day:
            overlaps = ev_start_date <= target_date < ev_end_date
        else:
            overlaps = ev_start_date <= target_date <= ev_end_date

        if overlaps:
            if ev.employee_id:
                busy_ids.add(ev.employee_id)
            if ev.employee_email:
                busy_emails.add(ev.employee_email)

    available = []
    unavailable = []
    for emp in all_employees:
        emp_id = str(emp["id"])
        emp_email = emp["email"]
        is_busy = emp_id in busy_ids or emp_email in busy_emails
        entry = {**emp, "employee_id": emp_id}
        if is_busy:
            unavailable.append(entry)
        else:
            available.append(entry)

    return {
        "date": target_date.isoformat(),
        "available": available,
        "unavailable": unavailable,
        "available_count": len(available),
        "unavailable_count": len(unavailable),
    }


def get_events_for_date(events: List[NormalizedEvent], query_date: datetime) -> List[Dict]:
    """Return all normalized events that overlap query_date."""
    target_date = query_date.date()
    result = []
    for ev in events:
        ev_start = ev.start_utc.date()
        ev_end = ev.end_utc.date()
        if ev.is_all_day:
            overlaps = ev_start <= target_date < ev_end
        else:
            overlaps = ev_start <= target_date <= ev_end
        if overlaps:
            result.append(ev.to_dict())
    return result
