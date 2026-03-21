"""Home Assistant API bridge for the Rota PDF Importer add-on."""

from __future__ import annotations

from homeassistant.core import HomeAssistant

from .http import RotaImporterAskView

DOMAIN = "rota_importer_bridge"


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Set up the bridge endpoint."""
    hass.http.register_view(RotaImporterAskView)
    return True
