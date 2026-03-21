# Rota PDF Importer add-on

A simple Home Assistant add-on that:

- exposes a web UI through Ingress
- accepts PDF uploads
- extracts rota-like rows from the PDF
- stores uploads and parsed rows in SQLite
- exports each upload as CSV

## Install

1. Copy this folder into your Home Assistant add-on repository, for example:
   `/addons/rota_pdf_importer/`
2. In Home Assistant, add that local repository if needed and reload the add-on store.
3. Install **Rota PDF Importer**.
4. Start the add-on.
5. Open the web UI.

## Stored files

Because `addon_config:rw` is mapped, the add-on writes to `/config` inside the container, which maps to the add-on config folder exposed by Home Assistant.

- SQLite DB: `/config/rota.db` by default
- Uploaded PDFs: `/config/uploads/`
- Exported CSVs: `/config/exports/`

## Use with Home Assistant automations (notifications)

Yes — you can send notifications with rota data using Home Assistant automations.

The recommended approach is to call this add-on's HTTP API from Home Assistant (instead of querying SQLite directly from an automation).

### Why API-first

- Works with standard Home Assistant `rest` integration and templates.
- Avoids direct SQLite file access and locking concerns.
- Keeps your automation independent from DB schema changes.

### Example: fetch uploads and notify when a new upload appears

Add a REST sensor in `configuration.yaml` (or split package):

```yaml
sensor:
  - platform: rest
    name: rota_importer_uploads
    # Replace host with your add-on endpoint reachable by Home Assistant.
    # Common options are an internal hostname, reverse proxy path, or direct URL.
    resource: http://YOUR_ADDON_HOST:8099/api/uploads
    method: GET
    scan_interval: 300
    value_template: "{{ value_json | count }}"
    json_attributes:
      - 0
```

Then create an automation that uses the JSON attributes and sends a notification:

```yaml
automation:
  - alias: "Rota importer: notify latest upload"
    mode: single
    trigger:
      - platform: state
        entity_id: sensor.rota_importer_uploads
    condition:
      - condition: template
        value_template: >-
          {{ trigger.to_state.state not in ['unknown', 'unavailable'] }}
    action:
      - service: notify.mobile_app_your_phone
        data:
          title: "Rota upload update"
          message: >-
            Latest file: {{ state_attr('sensor.rota_importer_uploads', '0').original_filename }}
            | Upload ID: {{ state_attr('sensor.rota_importer_uploads', '0').id }}
            | Parsed rows: {{ state_attr('sensor.rota_importer_uploads', '0').row_count }}
```

### More detailed payloads

You can also use:

- `/api/upload/<id>/model` to get a full viewer model for one upload.
- `/api/viewer_sync` to get all uploads in the viewer format.
- `/api/ask` to ask Siri-friendly rota questions with one JSON request.

Use a second REST sensor or a `rest_command` and template the returned JSON into any `notify.*` service.

## Siri Shortcuts / iOS automation endpoint

The add-on now exposes a lightweight rule-based question endpoint:

- `POST /api/ask`
- `Content-Type: application/json`
- `Authorization: Bearer <token>` (required)
- Body:
  - `question` (required string)
  - `person` (optional string; required for "who am I working with" style questions)

Authentication and security notes:

- Requests without a Bearer token are rejected.
- Invalid tokens are rejected with:

```json
{
  "error": "unauthorized"
}
```

- Token validation behavior:
  - If `ASK_API_TOKEN` is set in the add-on environment, `/api/ask` requires that exact token.
  - Otherwise the add-on validates the Bearer token against Home Assistant using `/auth/current_user` (or `ASK_AUTH_VALIDATE_URL` if set).
- Basic in-memory per-IP rate limiting is enabled for `/api/ask` (default: 30 requests per 60 seconds).

### Supported question types

`/api/ask` currently matches these intents:

- `who_is_working_today`
- `who_am_i_working_with_today`
- `opening_shift`
- `closing_shift`

Example phrases:

- `who is working today?`
- `who am I working with?`
- `who is opening tomorrow?`
- `who opens tomorrow?`
- `who is closing today?`
- `who closes today?`

Date resolution rules:

- `today` → current local date
- `tomorrow` → current local date + 1 day
- no date phrase → defaults to `today`

### Opening/closing mapping

This add-on does not depend on named shift labels like "open" or "close" in the PDF.
Instead it derives them from parsed shift times:

- **Opening shift** = earliest `start_time` on the resolved day
- **Closing shift** = latest `end_time` on the resolved day

If your rota uses different wording, this time-based mapping is what `/api/ask` uses.

### cURL examples

Who is opening tomorrow:

```bash
curl -s -X POST "http://YOUR_ADDON_HOST:8099/api/ask" \
  -H "Authorization: Bearer <LONG_LIVED_ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"question":"who is opening tomorrow?"}'
```

Who am I working with:

```bash
curl -s -X POST "http://YOUR_ADDON_HOST:8099/api/ask" \
  -H "Authorization: Bearer <LONG_LIVED_ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"question":"who am I working with today?","person":"Nathan"}'
```

Unknown question fallback:

```bash
curl -s -X POST "http://YOUR_ADDON_HOST:8099/api/ask" \
  -H "Authorization: Bearer <LONG_LIVED_ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"question":"can you sing me a song?"}'
```

### Example iOS Shortcut payloads

For **Get Contents of URL** (method `POST`, JSON body):

```json
{
  "question": "who is opening tomorrow?"
}
```

```json
{
  "question": "who am I working with today?",
  "person": "Nathan"
}
```

Success response shape:

```json
{
  "answer": "Tom is opening tomorrow.",
  "date": "2026-03-22",
  "matched_intent": "opening_shift"
}
```

If `person` is missing for a person-specific question:

```json
{
  "error": "person is required for this question type"
}
```

## Extract specific rota data with the API

If you want only *specific* rota data (for example one person, one day, or the latest upload), use the API in two steps:

1. Call `/api/uploads` to find the upload ID you want.
2. Call `/api/upload/<id>/model` and filter the JSON.

### Step 1: Get the latest upload ID

```bash
curl -s http://YOUR_ADDON_HOST:8099/api/uploads | jq '.[0].id'
```

`/api/uploads` returns objects like:

```json
[
  {
    "id": 42,
    "original_filename": "rota_week_14.pdf",
    "uploaded_at": "2026-03-07T18:22:11",
    "row_count": 35
  }
]
```

### Step 2: Query one upload model

```bash
curl -s http://YOUR_ADDON_HOST:8099/api/upload/42/model | jq
```

Important fields in this payload:

- `model.dayHeaders`: day labels in order (`sun`, `mon`, `tue`, `wed`, `thu`, `fri`, `sat`).
- `model.rows[]`: one object per employee.
- `model.rows[].days[]`: shift cells aligned to the same day order as `dayHeaders`.

### Practical `jq` examples

Get all shift cells for a single employee:

```bash
curl -s http://YOUR_ADDON_HOST:8099/api/upload/42/model \
  | jq '.model.rows[] | select(.name == "Jane Smith") | .days'
```

Get just one day for a person (Monday = index `1`, because order starts at Sunday index `0`):

```bash
curl -s http://YOUR_ADDON_HOST:8099/api/upload/42/model \
  | jq '.model.rows[] | select(.name == "Jane Smith") | {name, monday: .days[1]}'
```

Build a compact list for notifications (`name` + `today` cell, example uses Wednesday index `3`):

```bash
curl -s http://YOUR_ADDON_HOST:8099/api/upload/42/model \
  | jq '[.model.rows[] | {name, today: .days[3]}]'
```

### Home Assistant template example

If you store `/api/upload/<id>/model` JSON in a REST sensor attribute, you can extract one person's day cell with Jinja:

```jinja2
{% set rows = state_attr('sensor.rota_upload_model', 'model').rows %}
{% set jane = rows | selectattr('name', 'equalto', 'Jane Smith') | list | first %}
{{ jane.days[1] if jane else 'No shift found' }}
```

This makes it easy to drive `notify.*` actions with only the rota detail you need.

## Important

The parsing logic in `app/app.py` is intentionally generic. It will probably need tweaking for your exact rota PDF layout.

The key function is:

- `parse_pdf_to_rows()`

That is the bit you tune once you have 2 or 3 real rota samples.

## Per-person ICS feeds

This add-on now exposes per-person iCalendar feeds:

- `GET /api/people/{person_name}/calendar.ics`
- optional query: `upload_id=<id>` to pin to one upload (otherwise latest upload for that person)

Each event includes an `ATTACH` URL to a per-day chart image endpoint:

- `GET /api/people/{person_name}/charts/{YYYY-MM-DD}.png`

The chart image is a generated line chart showing staffing counts across the day for that date.

In the **People Settings** popup, each person now shows a ready-to-copy **Per-person ICS link** field so you can paste it directly into subscription-capable calendar/person settings.

For iPhone calendar subscriptions, the URL must be a full externally reachable URL (not a relative path) and must not require an interactive Home Assistant login page during fetch.

### External access with Nabu Casa

If your add-on URL is externally reachable through Home Assistant Cloud/Nabu Casa, the same `.ics` URL can be used from outside your home. Use your Home Assistant external URL/proxy path and make sure access is protected as needed for your setup.
