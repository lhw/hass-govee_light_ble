import asyncio
import bleak_retry_connector
from bleak.backends.characteristic import BleakGATTCharacteristic
from bleak import (
    BleakClient,
    BLEDevice
)
from homeassistant.exceptions import HomeAssistantError
from .const import WRITE_CHARACTERISTIC_UUID, READ_CHARACTERISTIC_UUID
from .api_utils import (
    LedPacketHead,
    LedPacketCmd,
    LedColorType,
    LedPacket,
    GoveeUtils
)

import logging
_LOGGER = logging.getLogger(__name__)


class GoveeAPI:
    state: bool | None = None
    brightness: int | None = None
    color: tuple[int, ...] | None = None

    def __init__(
        self,
        ble_device: BLEDevice | None,
        address: str,
        update_callback,
        segmented: bool = False,
    ):
        self._conn = None
        self._address = address
        self._ble_device = ble_device
        self._segmented = segmented
        self._packet_buffer = []
        self._client = None
        self._update_callback = update_callback
        self._ble_lock = asyncio.Lock()

    def update_ble_device(self, ble_device: BLEDevice) -> None:
        """Refresh the BLE device reference (e.g. after proxy reconnect)."""
        self._ble_device = ble_device

    @property
    def address(self):
        return self._ble_device.address if self._ble_device is not None else self._address

    async def _ensureConnected(self):
        """ connects to a bluetooth device """
        if self._client != None and self._client.is_connected:
            return None
        if self._ble_device is None:
            raise HomeAssistantError(
                f"BLE device {self.address} is not currently in Home Assistant's scanner cache"
            )
        await self._connect()

    async def _connect(self):
        def _on_disconnect(client: BleakClient) -> None:
            # Clear the client reference so _ensureConnected reconnects
            # properly instead of reusing a stale "connected" client that
            # cannot carry traffic.  write_gatt_char(response=False) never
            # raises on a dead link, which would otherwise leave the entity
            # stuck at the optimistic state indefinitely.
            if self._client is client:
                self._client = None
                _LOGGER.debug("BLE device %s disconnected", self.address)

        self._client = await bleak_retry_connector.establish_connection(
            BleakClient,
            self._ble_device,
            self.address,
            ble_device_callback=lambda: self._ble_device,
            disconnected_callback=_on_disconnect,
            # Limit to 2 attempts (≈2 BLE CONNECT requests per failure).
            # The original default of 9 caused ~4 500 connection requests
            # over 10 hours when the device was stuck, crashing its BLE
            # stack.  Exponential backoff in the coordinator handles
            # sustained outages.
            max_attempts=2,
        )
        await self._client.start_notify(READ_CHARACTERISTIC_UUID, self._handleReceive)

    async def _transmitPacket(self, packet: LedPacket):
        """ transmit the actiual packet """
        # convert to bytes
        frame = await GoveeUtils.generateFrame(packet)
        # transmit to UUID
        await self._client.write_gatt_char(WRITE_CHARACTERISTIC_UUID, frame, False)

    async def _handleRequest(self, packet: LedPacket):
        """ process received responses """
        match packet.cmd:
            case LedPacketCmd.POWER:
                self.state = packet.payload[0] == 0x01
            case LedPacketCmd.BRIGHTNESS:
                # segmented devices 0-100
                self.brightness = packet.payload[0] / 100 * \
                    255 if self._segmented else packet.payload[0]
            case LedPacketCmd.COLOR:
                red = packet.payload[1]
                green = packet.payload[2]
                blue = packet.payload[3]
                self.color = (red, green, blue)
            case LedPacketCmd.SEGMENT:
                red = packet.payload[2]
                green = packet.payload[3]
                blue = packet.payload[4]
                self.color = (red, green, blue)

    async def _handleReceive(self, characteristic: BleakGATTCharacteristic, frame: bytearray):
        """ receives packets async """
        if not await GoveeUtils.verifyChecksum(frame):
            # Log rather than raise: exceptions raised inside a bleak notify
            # callback are silently swallowed by asyncio and never reach our
            # coordinator, causing the optimistic state to persist with no
            # visible error.
            _LOGGER.warning(
                "Received packet with bad checksum from %s: %s",
                self.address,
                frame.hex(),
            )
            return

        packet = LedPacket(
            head=frame[0],
            cmd=frame[1],
            payload=frame[2:-1]
        )
        # only requests are expected to send a response
        if packet.head == LedPacketHead.REQUEST:
            await self._handleRequest(packet)
            await self._update_callback()

    async def _preparePacket(self, cmd: LedPacketCmd, payload: bytes | list = b'', request: bool = False, repeat: int = 3):
        """ add data to transmission buffer """
        # request data or perform a change
        head = LedPacketHead.REQUEST if request else LedPacketHead.COMMAND
        packet = LedPacket(head, cmd, payload)
        for index in range(repeat):
            self._packet_buffer.append(packet)

    async def _clearPacketBuffer(self):
        """ clears the packet buffer """
        self._packet_buffer = []

    async def sendPacketBuffer(self):
        """ transmits all buffered data """
        if not self._packet_buffer:
            # nothing to do
            return None
        async with self._ble_lock:
            # Drain the buffer immediately so packets added by concurrent
            # callers while we hold the lock are not lost, and so that a
            # failed connection never leaves stale packets that accumulate
            # into a massive burst when the device eventually reconnects.
            packets = self._packet_buffer
            self._packet_buffer = []
            if not packets:
                return None
            try:
                await self._ensureConnected()
                for packet in packets:
                    await self._transmitPacket(packet)
                # Give the device time to send notification responses before
                # we disconnect.  Govee devices respond within ~50–150 ms;
                # 500 ms is a safe margin.  The event loop runs freely during
                # this sleep so _handleReceive is called with state data.
                await asyncio.sleep(0.5)
            finally:
                # Always disconnect after each interaction.
                #
                # The H615A firmware crashes (BLE stack dead, hard reset
                # required) when a GATT connection is held open for more than
                # ~2–4 minutes — likely a firmware memory leak or watchdog
                # issue.  The Govee app follows a short connect–interact–
                # disconnect pattern; maintaining a persistent connection is
                # outside what the device was designed for.
                #
                # Use try/finally so we disconnect even when a write fails,
                # preventing ghost GATT handles in the ESPHome proxy that
                # would cause ESP_GATT_CONN_FAIL_ESTABLISH on the next poll.
                client = self._client
                self._client = None  # pre-clear so disconnected_callback is a no-op
                if client is not None and client.is_connected:
                    try:
                        await client.stop_notify(READ_CHARACTERISTIC_UUID)
                    except Exception:  # noqa: BLE001
                        pass
                    try:
                        await client.disconnect()
                    except Exception:  # noqa: BLE001
                        pass

    async def close(self) -> None:
        """Disconnect the GATT client and release the BLE connection.

        Called when the config entry is unloaded so the ESPHome proxy
        sends a proper BLE DISCONNECT to the device.  Without this the
        proxy retains the GATT handle and the device refuses the next
        connection attempt with ESP_GATT_CONN_FAIL_ESTABLISH.
        """
        async with self._ble_lock:
            client = self._client
            self._client = None  # pre-clear so disconnected_callback is a no-op
            if client is not None and client.is_connected:
                try:
                    await client.disconnect()
                except Exception:  # noqa: BLE001
                    pass

    async def requestStateBuffered(self):
        """ adds a request for the current power state to the transmit buffer """
        # repeat=1: one REQUEST packet is enough; the device always responds.
        # The original default of 3 tripled the GATT write rate for no gain.
        await self._preparePacket(LedPacketCmd.POWER, request=True, repeat=1)

    async def requestBrightnessBuffered(self):
        """ adds a request for the current brightness state to the transmit buffer """
        await self._preparePacket(LedPacketCmd.BRIGHTNESS, request=True, repeat=1)

    async def requestColorBuffered(self):
        """ adds a request for the current color state to the transmit buffer """
        if self._segmented:
            # 0x01 means first segment
            await self._preparePacket(LedPacketCmd.SEGMENT, b'\x01', request=True, repeat=1)
        else:
            # legacy devices
            await self._preparePacket(LedPacketCmd.COLOR, request=True, repeat=1)

    async def setStateBuffered(self, state: bool):
        """ adds the state to the transmit buffer """
        if self.state == state:
            return None  # nothing to do
        self.state = state  # optimistic update so HA reflects the change immediately
        # 0x1 = ON, Ox0 = OFF
        await self._preparePacket(LedPacketCmd.POWER, [0x1 if state else 0x0])
        await self.requestStateBuffered()

    async def setBrightnessBuffered(self, brightness: int):
        """ adds the brightness to the transmit buffer """
        if self.brightness == brightness:
            return None  # nothing to do
        self.brightness = brightness  # optimistic update
        # legacy devices 0-255
        payload = round(brightness)
        if self._segmented:
            # segmented devices 0-100
            payload = round(brightness / 255 * 100)
        await self._preparePacket(LedPacketCmd.BRIGHTNESS, [payload])
        await self.requestBrightnessBuffered()

    async def setColorBuffered(self, red: int, green: int, blue: int):
        """ adds the color to the transmit buffer """
        if self.color == (red, green, blue):
            return None  # nothing to do
        self.color = (red, green, blue)  # optimistic update
        if self._segmented:
            await self._preparePacket(LedPacketCmd.COLOR, [LedColorType.SEGMENTS, 0x01, red, green, blue, 0, 0, 0, 0, 0, 0xff, 0xff])
        else:
            # legacy devices
            await self._preparePacket(LedPacketCmd.COLOR, [LedColorType.SINGLE, red, green, blue])
            await self._preparePacket(LedPacketCmd.COLOR, [LedColorType.LEGACY, red, green, blue])
        await self.requestColorBuffered()
