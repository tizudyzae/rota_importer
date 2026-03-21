#!/usr/bin/with-contenv bashio
set -euo pipefail

export ROTA_DB_PATH=/config/rota.db

BRIDGE_SRC_DIR="/ha_bridge_component/custom_components/rota_importer_bridge"
BRIDGE_DEST_ROOT="/homeassistant/custom_components"
BRIDGE_DEST_DIR="${BRIDGE_DEST_ROOT}/rota_importer_bridge"
HA_CONFIGURATION_FILE="/homeassistant/configuration.yaml"

if [ -d "${BRIDGE_SRC_DIR}" ]; then
  mkdir -p "${BRIDGE_DEST_ROOT}"
  rm -rf "${BRIDGE_DEST_DIR}"
  cp -a "${BRIDGE_SRC_DIR}" "${BRIDGE_DEST_DIR}"

  if [ -f "${HA_CONFIGURATION_FILE}" ]; then
    if ! grep -Eq '^\s*rota_importer_bridge\s*:\s*$' "${HA_CONFIGURATION_FILE}"; then
      {
        echo
        echo "# Added by Rota PDF Importer add-on for authenticated remote API bridge"
        echo "rota_importer_bridge:"
      } >> "${HA_CONFIGURATION_FILE}"
    fi
  fi
else
  echo "[warning] HA bridge component source not found at ${BRIDGE_SRC_DIR}; skipping bridge sync"
fi

cd /app
uvicorn app:app --host 0.0.0.0 --port 8099
