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

Yes — and now there is a dedicated shifts API for that use case.

Use `GET /api/shifts` to query who is working on specific dates or date ranges.

### Query endpoint for "who is working when"

`/api/shifts` supports these query parameters:

- `date_from` (`YYYY-MM-DD`) optional
- `date_to` (`YYYY-MM-DD`) optional
- `employee` optional exact-name filter (case-insensitive)
- `working_only` (`true`/`false`) optional; when true, only rows with start/end times are returned
- `limit` optional (1..500, default 300)

Example:

```text
/api/shifts?date_from=2026-03-10&date_to=2026-03-10&working_only=true
```

Response includes:

- `items`: flat row list from the DB (employee, shift_date, start/end, raw cell, upload metadata)
- `by_date`: grouped workers per date with `worker_count`

### Home Assistant example (REST sensor + notification)

```yaml
sensor:
  - platform: rest
    name: rota_workers_today
    resource_template: >-
      http://YOUR_ADDON_HOST:8099/api/shifts
      ?date_from={{ now().strftime('%Y-%m-%d') }}
      &date_to={{ now().strftime('%Y-%m-%d') }}
      &working_only=true
    method: GET
    scan_interval: 300
    value_template: "{{ value_json.count }}"
    json_attributes:
      - by_date
```

```yaml
automation:
  - alias: "Rota importer: notify who is working today"
    mode: single
    trigger:
      - platform: time
        at: "07:00:00"
    action:
      - service: notify.mobile_app_your_phone
        data:
          title: "Today's rota"
          message: >-
            {% set groups = state_attr('sensor.rota_workers_today', 'by_date') or [] %}
            {% set today = (groups | first) if groups else None %}
            {% if not today %}
              No scheduled shifts found today.
            {% else %}
              {{ today.worker_count }} working today:
              {% for person in today.workers %}
                {{ person.employee }} ({{ person.start_time }}-{{ person.end_time }}){% if not loop.last %}, {% endif %}
              {% endfor %}
            {% endif %}
```

This is API-first (recommended), so automations do not need direct SQLite access.

## Important

The parsing logic in `app/app.py` is intentionally generic. It will probably need tweaking for your exact rota PDF layout.

The key function is:

- `parse_pdf_to_rows()`

That is the bit you tune once you have 2 or 3 real rota samples.
