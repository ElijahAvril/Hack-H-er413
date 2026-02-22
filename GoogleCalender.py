import csv
import requests
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv("Google_Api_Key")  # e.g. "AIzaSyA-EXAMPLEKEY1234567890"
CALENDAR_ID = os.getenv("Google_Calendar_ID")  # e.g. "...@group.calendar.google.com"
OUT_CSV = "google_calendar_events.csv"

def extract_start_end(event: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Google Calendar events can be timed (dateTime) or all-day (date).
    Return ISO strings for start/end.
    """
    start_obj = event.get("start", {}) or {}
    end_obj = event.get("end", {}) or {}
    start = start_obj.get("dateTime") or start_obj.get("date")
    end = end_obj.get("dateTime") or end_obj.get("date")
    return start, end

def fetch_all_events(
    calendar_id: str,
    api_key: str,
    time_min_utc: datetime,
    time_max_utc: datetime
) -> List[Dict[str, Any]]:
    """
    Fetch events from a (public) Google Calendar using an API key.
    Handles pagination via nextPageToken.
    """
    base_url = "https://www.googleapis.com/calendar/v3/calendars/{}/events".format(
        requests.utils.quote(calendar_id, safe="")
    )

    params = {
        "key": api_key,
        "timeMin": time_min_utc.isoformat(),
        "timeMax": time_max_utc.isoformat(),
        "singleEvents": "true",  # expands recurring events into instances
        "orderBy": "startTime",
        "showDeleted": "true",
        "maxResults": 2500,      # max per page
    }

    all_items: List[Dict[str, Any]] = []
    page_token: Optional[str] = None

    while True:
        if page_token:
            params["pageToken"] = page_token
        else:
            params.pop("pageToken", None)

        resp = requests.get(base_url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        all_items.extend(data.get("items", []))
        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return all_items

def save_events_to_csv(events: List[Dict[str, Any]], out_path: str) -> None:
    """
    Save summary, description, start, end (+ a few helpful fields) to CSV.
    """
    fieldnames = [
        "id",
        "status",
        "summary",
        "description",
        "start",
        "end",
        "htmlLink",
        "updated",
    ]

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for e in events:
            start, end = extract_start_end(e)
            writer.writerow({
                "id": e.get("id", ""),
                "status": e.get("status", ""),
                "summary": e.get("summary", ""),
                "description": e.get("description", ""),
                "start": start or "",
                "end": end or "",
                "htmlLink": e.get("htmlLink", ""),
                "updated": e.get("updated", ""),
            })

if __name__ == "__main__":
    # Window: now -> next 60 days (adjust as you want)
    time_min = datetime.now(timezone.utc)
    time_max = time_min + timedelta(days=60)

    events = fetch_all_events(CALENDAR_ID, API_KEY, time_min, time_max)
    save_events_to_csv(events, OUT_CSV)

    print(f"Saved {len(events)} events to {OUT_CSV}")