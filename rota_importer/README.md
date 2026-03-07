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

## Important

The parsing logic in `app/app.py` is intentionally generic. It will probably need tweaking for your exact rota PDF layout.

The key function is:

- `parse_pdf_to_rows()`

That is the bit you tune once you have 2 or 3 real rota samples.
