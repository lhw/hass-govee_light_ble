from dataclasses import dataclass
from datetime import timedelta

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_ADDRESS, CONF_NAME
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.components import bluetooth

from .const import DOMAIN
from .api import GoveeAPI

import logging
_LOGGER = logging.getLogger(__name__)


@dataclass
class GoveeApiData:
    """Class to hold api data."""

    state: bool | None = None
    brightness: int | None = None
    color: tuple[int, ...] | None = None


class GoveeCoordinator(DataUpdateCoordinator):
    """My coordinator."""

    data: GoveeApiData

    def __init__(self, hass: HomeAssistant, config_entry: ConfigEntry) -> None:
        """Initialize coordinator."""

        # Set variables from values entered in config flow setup
        self.device_name = config_entry.data[CONF_NAME]
        self.device_address = config_entry.data[CONF_ADDRESS]
        self.device_segmented = config_entry.data["segmented"]

        # get connection to bluetooth device
        ble_device = bluetooth.async_ble_device_from_address(
            hass,
            self.device_address,
            connectable=True
        ) or bluetooth.async_ble_device_from_address(
            hass,
            self.device_address,
            connectable=False
        )
        assert ble_device
        self._api = GoveeAPI(
            ble_device, self._async_push_data, self.device_segmented)

        # Register a BLE advertisement callback so that _ble_device is
        # refreshed immediately whenever the Govee device re-advertises
        # (e.g. after the scanner cache expires or a GATT disconnect).
        # This ensures bleak_retry_connector's ble_device_callback always
        # returns a valid proxy-aware reference even when the scanner cache
        # has expired (devices stop advertising while GATT-connected, so the
        # ~195 s cache window can elapse before a reconnect is needed).
        @callback
        def _on_ble_advertisement(
            service_info: bluetooth.BluetoothServiceInfoBleak,
            change: bluetooth.BluetoothChange,
        ) -> None:
            # Only accept connectable advertisements so we never overwrite the
            # ESPHome proxy-aware (connectable) BLEDevice with a non-connectable
            # passive-scan entry that can't carry GATT traffic.
            if not service_info.connectable:
                return
            self._api.update_ble_device(service_info.device)
            _LOGGER.debug(
                "Refreshed BLE device reference for %s from advertisement"
                " (RSSI %d dBm)",
                self.device_address,
                service_info.rssi,
            )

        config_entry.async_on_unload(
            bluetooth.async_register_callback(
                hass,
                _on_ble_advertisement,
                bluetooth.BluetoothCallbackMatcher(address=self.device_address),
                bluetooth.BluetoothScanningMode.PASSIVE,
            )
        )

        # Initialise DataUpdateCoordinator
        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN} ({config_entry.unique_id})",
            # Set update method to get devices on first load.
            update_method=self._async_update_data,
            # Do not set a polling interval as data will be pushed.
            # You can remove this line but left here for explanatory purposes.
            update_interval=timedelta(seconds=15)
        )

    def _get_data(self):
        return GoveeApiData(
            state=self._api.state,
            brightness=self._api.brightness,
            color=self._api.color
        )

    async def _async_push_data(self):
        self.async_set_updated_data(self._get_data())

    async def _async_update_data(self):
        """Fetch data from API endpoint.

        This is the place to pre-process the data to lookup tables
        so entities can quickly look up their data.
        """
        # Refresh the BLE device reference before each connection attempt so
        # that a stale cached advertisement (typically expires after ~15 min)
        # never prevents reconnection.  Prefer connectable=True so the
        # ESPHome proxy channel is included; fall back to non-connectable
        # only as a last resort.
        fresh_device = bluetooth.async_ble_device_from_address(
            self.hass, self.device_address, connectable=True
        ) or bluetooth.async_ble_device_from_address(
            self.hass, self.device_address, connectable=False
        )
        if fresh_device:
            self._api.update_ble_device(fresh_device)
        else:
            _LOGGER.debug(
                "BLE device %s not in scanner cache; will retry with last known reference",
                self.device_address,
            )

        await self._api.requestStateBuffered()
        await self._api.requestBrightnessBuffered()
        await self._api.requestColorBuffered()
        await self._api.sendPacketBuffer()
        return self._get_data()

    async def setStateBuffered(self, state: bool):
        await self._api.setStateBuffered(state)

    async def setBrightnessBuffered(self, brightness: int):
        await self._api.setBrightnessBuffered(brightness)

    async def setColorBuffered(self, red: int, green: int, blue: int):
        await self._api.setColorBuffered(red, green, blue)

    async def sendPacketBuffer(self):
        # Refresh the BLE device reference for user-initiated commands, just as
        # _async_update_data does for periodic polls.
        fresh_device = bluetooth.async_ble_device_from_address(
            self.hass, self.device_address, connectable=True
        ) or bluetooth.async_ble_device_from_address(
            self.hass, self.device_address, connectable=False
        )
        if fresh_device:
            self._api.update_ble_device(fresh_device)
        await self._api.sendPacketBuffer()
        # Push the optimistic state (set by the setters above) to HA immediately
        # so the UI updates before waiting for the device's confirmation notification.
        self.async_set_updated_data(self._get_data())
