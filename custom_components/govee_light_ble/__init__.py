from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from homeassistant.components import bluetooth
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_ADDRESS, CONF_NAME, Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .coordinator import GoveeCoordinator
from .const import DOMAIN

import logging
_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [Platform.LIGHT]


@dataclass
class RuntimeData:
    """Class to hold your data."""

    coordinator: DataUpdateCoordinator
    cancel_update_listener: Callable


async def async_setup_entry(hass: HomeAssistant, config_entry: ConfigEntry) -> bool:
    """Set up Integration from a config entry."""

    hass.data.setdefault(DOMAIN, {})

    # Look for the device the same way the coordinator does. ESPHome-proxied
    # devices may only be available as connectable=True entries.
    device_address = config_entry.data[CONF_ADDRESS]
    ble_device = bluetooth.async_ble_device_from_address(
        hass, device_address, True
    ) or bluetooth.async_ble_device_from_address(hass, device_address, False)
    if not ble_device:
        raise ConfigEntryNotReady(
            f"Could not find LED BLE device with address {device_address}"
        )

    # Initialise the coordinator that manages data updates from your api.
    # This is defined in coordinator.py
    coordinator = GoveeCoordinator(hass, config_entry)

    # Perform an initial data load from api.
    # async_config_entry_first_refresh() is special in that it does not log errors if it fails
    await coordinator.async_config_entry_first_refresh()

    # Initialise a listener for config flow options changes.
    # See config_flow for defining an options setting that shows up as configure on the integration.
    cancel_update_listener = config_entry.add_update_listener(
        _async_update_listener)

    # Add the coordinator and update listener to hass data to make
    hass.data[DOMAIN][config_entry.entry_id] = RuntimeData(
        coordinator, cancel_update_listener
    )

    # Setup platforms (based on the list of entity types in PLATFORMS defined above)
    # This calls the async_setup method in each of your entity type files.
    await hass.config_entries.async_forward_entry_setups(config_entry, PLATFORMS)

    # Return true to denote a successful setup.
    return True


async def _async_update_listener(hass: HomeAssistant, config_entry):
    """Handle config options update."""
    # Reload the integration when the options change.
    await hass.config_entries.async_reload(config_entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, config_entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    # This is called when you remove your integration or shutdown HA.
    # If you have created any custom services, they need to be removed here too.

    # Remove the config options update listener
    hass.data[DOMAIN][config_entry.entry_id].cancel_update_listener()

    # Disconnect the BLE GATT connection cleanly so the ESPHome proxy
    # sends a proper BLE DISCONNECT to the device.  Without this the proxy
    # retains the handle and the device refuses the next connection attempt
    # with ESP_GATT_CONN_FAIL_ESTABLISH after an entry reload or HA restart.
    await hass.data[DOMAIN][config_entry.entry_id].coordinator.close()

    # Unload platforms
    unload_ok = await hass.config_entries.async_unload_platforms(
        config_entry, PLATFORMS
    )

    # Remove the config entry from the hass data object.
    if unload_ok:
        hass.data[DOMAIN].pop(config_entry.entry_id)

    # Return that unloading was successful.
    return unload_ok


async def async_migrate_entry(hass: HomeAssistant, config_entry: ConfigEntry):
    """Migrate old entry."""
    _LOGGER.debug("Migrating configuration from version %s",
                  config_entry.version)

    if config_entry.version == 1:
        unique_id = config_entry.unique_id
        if unique_id is None:
            _LOGGER.error(
                "Cannot migrate govee_light_ble entry without unique_id")
            return False
        hass.config_entries.async_update_entry(config_entry, data={
            CONF_ADDRESS: unique_id.upper(),
            CONF_NAME: config_entry.title,
            "segmented": True
        }, version=2)

    _LOGGER.debug(
        "Migration to configuration version %s successful", config_entry.version)

    return True
