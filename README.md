# TeamPulse

**Team availability dashboard with intelligent task reassignment.**

TeamPulse normalizes calendar data from Google Calendar and Microsoft Graph into a unified schema, tracks employee availability by date, and automatically suggests task reassignments when an assignee is unavailable — surfacing the best replacement based on skill fit and current workload.

---

## Features

- **Calendar normalization** — ingests Google Calendar JSON, Google Calendar CSV exports, and Microsoft Graph JSON into a single unified event schema
- **Availability tracking** — see who is available, busy, or OOF on any given date
- **Utilization scoring** — combines calendar load and active task count into a per-employee utilization percentage
- **Reassignment engine** — for every task whose assignee is unavailable, ranks available teammates by skill match and free capacity
- **One-click reassignment** — confirm a reassignment in the dashboard and it writes back to `EmployeeDatabase.json` immediately with a full audit trail
- **Live data** — all panels read from disk on every request; no stale embedded data
- **Local-first** — runs entirely on your machine, no cloud dependency

---

## Project Structure

```
your-project/
├── api.py                          # Flask REST API server
├── normalizer.py                   # Calendar normalization logic
├── reassignment.py                 # Utilization scoring + reassignment engine
├── calendar_dashboard.html         # Single-file web dashboard
├── EmployeeDatabase.json           # Employee records + task definitions
│
├── TimelineTest.py                 # Generates fake Google + MS calendar data
├── GoogleCalender.py               # Fetches live Google Calendar via API
│
├── google_calendar_events.json     # Output of TimelineTest.py
├── google_calendar_events.csv      # Output of GoogleCalender.py
├── microsoft_calendar_events.json  # Output of TimelineTest.py
├── GoogleAccounts.csv
├── MicrosoftAccounts.csv
└── .env                            # API credentials (not committed)
```

---

## Quickstart

### 1. Clone the repo

```bash
git clone https://github.com/your-username/teampulse.git
cd teampulse
```

### 2. Install dependencies

```bash
pip install flask flask-cors python-dotenv
```

### 3. Set up credentials

Create a `.env` file in the project root:

```env
GOOGLE_API_KEY=your_google_api_key_here
GOOGLE_CALENDAR_ID=your_calendar_id_here
```

### 4. Generate sample calendar data

```bash
python TimelineTest.py
```

This creates `google_calendar_events.json` and `microsoft_calendar_events.json` with randomized events for 20 employees over the next 14 days.

### 5. Start the server

```bash
python api.py
```

### 6. Open the dashboard

```
http://localhost:5050
```

To share with others on your local network, open:

```
http://<your-local-ip>:5050
```

Find your IP with `ipconfig` (Windows) or `ifconfig` (Mac/Linux).

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/status` | Server health check + file presence on disk |
| `GET` | `/api/employees` | All employees |
| `GET` | `/api/tasks` | All tasks. Optional `?status=todo\|in_progress` |
| `GET` | `/api/normalize` | All normalized calendar events. Optional `?date=YYYY-MM-DD` |
| `GET` | `/api/availability` | Available/unavailable employees. Optional `?date=YYYY-MM-DD` |
| `GET` | `/api/utilization` | Per-employee utilization scores. Optional `?date=YYYY-MM-DD` |
| `GET` | `/api/reassignments` | Reassignment suggestions for a date. Optional `?date=YYYY-MM-DD` |
| `POST` | `/api/reassign` | Execute a reassignment. Body: `{ task_id, new_assignee_id, reason? }` |
| `POST` | `/api/generate` | Run `TimelineTest.py` to regenerate fake calendar data |
| `POST` | `/api/fetch_google` | Run `GoogleCalender.py` to fetch live Google Calendar data |

---

## Database Schema

### Employee

```json
{
  "id": 1,
  "first_name": "Alex",
  "last_name": "Wilber",
  "email": "alex.wilber@contoso.com",
  "role": "Backend Engineer",
  "skills": ["python", "backend", "sql", "api-design"],
  "max_tasks_per_day": 4,
  "timezone": "America/New_York"
}
```

### Task

```json
{
  "id": "T001",
  "title": "Fix Safari login bug",
  "description": "Users on Safari 16+ cannot complete OAuth flow.",
  "assigned_to_id": 8,
  "status": "in_progress",
  "priority": "critical",
  "required_skills": ["javascript", "frontend"],
  "effort_hours": 3,
  "due_date": "2026-02-27",
  "last_reassigned": "2026-02-26T14:32:00Z",
  "reassignment_reason": "Assignee unavailable"
}
```

`status` values: `todo` · `in_progress` · `done`  
`priority` values: `critical` · `high` · `medium` · `low`

---

## Reassignment Scoring

Candidates are ranked per task using:

```
score = (skill_match_count × 15) - utilization_pct - (calendar_event_count × 3)
```

Higher score = better fit. Filters applied before scoring:
- Assignee must be **available** on the query date (no OOF or all-day block)
- Must have **free capacity** (`active_tasks < max_tasks_per_day`)

Up to 3 ranked candidates are returned per task, each showing skill match %, utilization bar, and any skill gaps.

---

## Normalized Event Schema

All calendar sources are normalized into this structure:

| Field | Type | Description |
|-------|------|-------------|
| `event_id` | string | Original event ID from source |
| `source` | string | `google` · `microsoft` · `google_csv` |
| `employee_id` | string | Matched employee ID |
| `employee_email` | string | Employee email |
| `title` | string | Event title |
| `availability` | string | `busy` · `oof` · `free` |
| `start_utc` | ISO 8601 | Start time in UTC |
| `end_utc` | ISO 8601 | End time in UTC |
| `is_all_day` | bool | Whether this is an all-day event |

---

## Dashboard Tabs

| Tab | Description |
|-----|-------------|
| **Availability** | Employee grid for a selected date. Color-coded available/unavailable with utilization bars. |
| **Reassignments** | Tasks needing reassignment with ranked candidates. Click Assign → to execute. |
| **Utilization** | All employees sorted by workload. Cards + sortable table. |
| **Events** | Filterable table of all normalized calendar events. |
| **Timeline** | Gantt-style view of events across the day (7am–11pm UTC). |
| **Normalized** | Raw normalized event data with metadata summary. |
| **Actions** | Buttons to generate fake data, run normalization, fetch live Google Calendar, and export JSON/CSV. |

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `GOOGLE_API_KEY` | Google Calendar API key |
| `GOOGLE_CALENDAR_ID` | Calendar ID to fetch from |

---

## Roadmap

- [ ] Create Task form with auto-assign in the dashboard
- [ ] Live Microsoft Graph calendar integration
- [ ] Task status updates from dashboard
- [ ] Assignment history log view
- [ ] Multi-day reassignment planning

---

## Built With

- [Flask](https://flask.palletsprojects.com/) — local API server
- [python-dotenv](https://github.com/theskumar/python-dotenv) — credential management
- [Syne](https://fonts.google.com/specimen/Syne) + [DM Mono](https://fonts.google.com/specimen/DM+Mono) — typography
- Google Calendar API
- Microsoft Graph API
