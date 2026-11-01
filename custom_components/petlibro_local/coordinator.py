"""Coordinator for the Petlibro Local integration."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from collections.abc import Callable
from datetime import datetime
from typing import Any

from homeassistant.components import mqtt
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.event import async_call_later
from homeassistant.util import dt as dt_util

from .const import (
    AVAILABILITY_TIMEOUT,
    CMD_ATTR_GET,
    CMD_DEVICE_START,
    CMD_GET_FEEDING_PLAN,
    CMD_HEARTBEAT,
    CMD_NTP,
    CMD_NTP_SYNC,
    COMMAND_ACK_TIMEOUT,
    CONF_FEEDING_PLAN_REPLY,
    CONF_MODEL,
    CONF_SERIAL,
    DEFAULT_FEEDING_PLAN_REPLY,
    DOMAIN,
    FEEDING_PLAN_REPLY_EMPTY,
    MAINTENANCE_KEYS,
    NTP_SYNC_DRIFT_THRESHOLD,
    NTP_SYNC_MIN_INTERVAL,
    SEEN_EVENT_CACHE_SIZE,
)
from .helpers import (
    PetlibroCommandError,
    new_msg_id,
    publish_command,
    publish_payload,
    time_zone_fields,
)

_LOGGER = logging.getLogger(__name__)


class PetlibroCoordinator:
    """Coordinator for managing shared MQTT subscriptions.

    Subscribes to the device's four outbound topics once and dispatches parsed
    messages to registered listeners, avoiding duplicate subscriptions and
    redundant JSON parsing.

    It also holds the broker side of the protocol the device expects an answer
    to. The device publishes at QoS 1 and retries every event until something
    acks it on ``device/event/sub``, and it asks for the time on
    ``device/ntp/post`` until it gets a reply on ``device/ntp/sub``. Without
    those answers the feeder republishes forever.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        topics: dict[str, str],
    ) -> None:
        """Initialize the coordinator.

        Args:
            hass: Home Assistant instance.
            entry: Config entry for this device.
            topics: Dictionary of MQTT topics.
        """
        self.hass = hass
        self.entry = entry
        self.topics = topics

        self._event_listeners: list[Callable[[dict[str, Any]], None]] = []
        self._heartbeat_listeners: list[Callable[[dict[str, Any]], None]] = []
        self._availability_listeners: list[Callable[[bool], None]] = []
        self._maintenance_listeners: dict[
            str, list[Callable[[datetime | None], None]]
        ] = {key: [] for key in MAINTENANCE_KEYS}

        self._unsubscribes: list[Callable[[], None]] = []
        self._device_state: dict[str, Any] = {}
        self._maintenance: dict[str, datetime | None] = dict.fromkeys(
            MAINTENANCE_KEYS
        )

        # msgId -> future resolved by the device's ack on device/service/post
        self._pending_acks: dict[str, asyncio.Future[dict[str, Any]]] = {}

        # (cmd, msgId, execStep) tuples already dispatched, so a retransmission
        # is acked again but not counted twice.
        self._seen_events: deque[tuple[str, str, str | None]] = deque(
            maxlen=SEEN_EVENT_CACHE_SIZE
        )

        self._available = False
        self._cancel_availability: Callable[[], None] | None = None

        # When the last NTP_SYNC went out (time.monotonic()), for _check_clock.
        self._last_ntp_sync: float | None = None

    @property
    def device_state(self) -> dict[str, Any]:
        """Return the merged device state built from ATTR_PUSH_EVENT pushes."""
        return self._device_state

    @property
    def available(self) -> bool:
        """Return True while the device is heartbeating."""
        return self._available

    @property
    def _feeding_plan_reply(self) -> str:
        """Return how GET_FEEDING_PLAN_EVENT should be answered."""
        return self.entry.options.get(
            CONF_FEEDING_PLAN_REPLY, DEFAULT_FEEDING_PLAN_REPLY
        )

    # ------------------------------------------------------------------
    # Listener registration
    # ------------------------------------------------------------------

    def register_event_listener(
        self, listener: Callable[[dict[str, Any]], None]
    ) -> Callable[[], None]:
        """Register a listener for event messages.

        Args:
            listener: Callback function that receives parsed payload.

        Returns:
            Unsubscribe function.
        """
        return self._register(self._event_listeners, listener)

    def register_heartbeat_listener(
        self, listener: Callable[[dict[str, Any]], None]
    ) -> Callable[[], None]:
        """Register a listener for heartbeat messages.

        Args:
            listener: Callback function that receives parsed payload.

        Returns:
            Unsubscribe function.
        """
        return self._register(self._heartbeat_listeners, listener)

    def register_availability_listener(
        self, listener: Callable[[bool], None]
    ) -> Callable[[], None]:
        """Register a listener notified when the device appears or goes quiet.

        Args:
            listener: Callback function that receives the new availability.

        Returns:
            Unsubscribe function.
        """
        return self._register(self._availability_listeners, listener)

    def register_maintenance_listener(
        self, key: str, listener: Callable[[datetime | None], None]
    ) -> Callable[[], None]:
        """Register a listener for one maintenance timestamp.

        Args:
            key: One of the MAINTENANCE_KEYS.
            listener: Callback function that receives the new value.

        Returns:
            Unsubscribe function.
        """
        return self._register(self._maintenance_listeners[key], listener)

    @staticmethod
    def _register(
        listeners: list[Callable[..., None]], listener: Callable[..., None]
    ) -> Callable[[], None]:
        """Append a listener and return its unsubscribe callback."""
        listeners.append(listener)

        def unsubscribe() -> None:
            if listener in listeners:
                listeners.remove(listener)

        return unsubscribe

    # ------------------------------------------------------------------
    # Maintenance timestamps (Home Assistant side bookkeeping)
    # ------------------------------------------------------------------

    def get_maintenance(self, key: str) -> datetime | None:
        """Return a maintenance timestamp, or None if never recorded."""
        return self._maintenance[key]

    @callback
    def set_maintenance(self, key: str, value: datetime | None) -> None:
        """Set a maintenance timestamp and notify its entity.

        Args:
            key: One of the MAINTENANCE_KEYS.
            value: The new timestamp, or None to clear it.
        """
        self._maintenance[key] = value
        for listener in list(self._maintenance_listeners[key]):
            listener(value)
        _LOGGER.debug("Maintenance timestamp %s set to %s", key, value)

    # ------------------------------------------------------------------
    # Setup / teardown
    # ------------------------------------------------------------------

    async def async_setup(self) -> None:
        """Subscribe to the device's outbound topics."""
        for topic_key, handler in (
            ("event", self._event_received),
            ("heartbeat", self._heartbeat_received),
            ("ntp", self._ntp_received),
            ("service_ack", self._service_ack_received),
        ):
            self._unsubscribes.append(
                await mqtt.async_subscribe(
                    self.hass, self.topics[topic_key], handler, qos=0
                )
            )

        # Seed state. If the device is offline this goes nowhere; the first
        # message from it flips availability and re-requests.
        await self.async_request_state()

    async def async_unload(self) -> None:
        """Unsubscribe from MQTT topics and cancel timers."""
        for unsubscribe in self._unsubscribes:
            unsubscribe()
        self._unsubscribes.clear()

        if self._cancel_availability:
            self._cancel_availability()
            self._cancel_availability = None

        for future in self._pending_acks.values():
            if not future.done():
                future.cancel()
        self._pending_acks.clear()

    # ------------------------------------------------------------------
    # Inbound message handlers
    # ------------------------------------------------------------------

    @callback
    def _event_received(self, msg: mqtt.ReceiveMessage) -> None:
        """Handle a message on device/event/post.

        Every event carrying a msgId is acked on device/event/sub, including
        retransmissions of events we have already seen -- the device retried
        precisely because it did not get the first ack. Only the first copy is
        dispatched to entities.

        The device also posts its answer to ATTR_GET_SERVICE here. That answer
        is not acked: the device takes the ack as a fresh ATTR_GET_SERVICE and
        answers again, about twice a second for as long as we keep acking.
        """
        payload = self._parse(msg)
        if payload is None:
            return

        self._note_message()

        cmd = payload.get("cmd")
        msg_id = payload.get("msgId")
        if cmd and msg_id and cmd != CMD_ATTR_GET:
            self.hass.async_create_task(self._async_ack_event(cmd, payload))

        key = (cmd or "", msg_id or "", payload.get("execStep"))
        if msg_id and key in self._seen_events:
            _LOGGER.debug("Duplicate %s (msgId %s)", cmd, msg_id)
            return
        if msg_id:
            self._seen_events.append(key)

        self._device_state.update(payload)

        if cmd == CMD_DEVICE_START:
            self._handle_device_start(payload)

        for listener in list(self._event_listeners):
            listener(payload)

    @callback
    def _heartbeat_received(self, msg: mqtt.ReceiveMessage) -> None:
        """Handle a message on device/heart/post. Heartbeats are not acked."""
        payload = self._parse(msg)
        if payload is None or payload.get("cmd") != CMD_HEARTBEAT:
            return

        self._note_message()
        self._check_clock(payload)

        for listener in list(self._heartbeat_listeners):
            listener(payload)

    @callback
    def _ntp_received(self, msg: mqtt.ReceiveMessage) -> None:
        """Handle a message on device/ntp/post.

        The device asks for the time on boot and keeps asking until answered,
        so reply with the same shape the vendor's broker did. It also answers
        our NTP_SYNC here, which needs no reply.
        """
        payload = self._parse(msg)
        if payload is None:
            return

        cmd = payload.get("cmd")
        if cmd == CMD_NTP:
            self._note_message()
            self.hass.async_create_task(self._async_reply_ntp())
        elif cmd == CMD_NTP_SYNC:
            self._note_message()
            _LOGGER.debug(
                "Device answered NTP_SYNC (msgId %s, code %s, ts %s)",
                payload.get("msgId"),
                payload.get("code"),
                payload.get("ts"),
            )

    @callback
    def _service_ack_received(self, msg: mqtt.ReceiveMessage) -> None:
        """Handle a message on device/service/post.

        These are the device's acks for commands we sent. Resolving the pending
        future here is what lets a command report success or failure instead of
        silently doing nothing.
        """
        payload = self._parse(msg)
        if payload is None:
            return

        self._note_message()

        msg_id = payload.get("msgId")
        future = self._pending_acks.get(msg_id) if msg_id else None
        if future is not None and not future.done():
            future.set_result(payload)
        else:
            _LOGGER.debug(
                "Unmatched ack for %s (msgId %s, code %s)",
                payload.get("cmd"),
                msg_id,
                payload.get("code"),
            )

    @callback
    def _parse(self, msg: mqtt.ReceiveMessage) -> dict[str, Any] | None:
        """Parse a JSON MQTT payload, or None if it is not a JSON object."""
        try:
            payload = json.loads(msg.payload)
        except (json.JSONDecodeError, TypeError, ValueError):
            _LOGGER.warning(
                "Invalid JSON on %s: %s", msg.topic, msg.payload
            )
            return None

        if not isinstance(payload, dict):
            _LOGGER.warning("Unexpected payload on %s: %s", msg.topic, payload)
            return None

        return payload

    @callback
    def _handle_device_start(self, payload: dict[str, Any]) -> None:
        """Record firmware metadata and re-seed state after a device reboot."""
        self.hass.async_create_task(self.async_request_state())

        updates: dict[str, Any] = {}
        if software_version := payload.get("softwareVersion"):
            updates["sw_version"] = software_version
        if hardware_version := payload.get("hardwareVersion"):
            updates["hw_version"] = hardware_version
        if mac := payload.get("mac"):
            updates["merge_connections"] = {
                (dr.CONNECTION_NETWORK_MAC, dr.format_mac(mac))
            }
        if not updates:
            return

        device_registry = dr.async_get(self.hass)
        device = device_registry.async_get_device(
            identifiers={(DOMAIN, self.entry.data[CONF_SERIAL])}
        )
        if device is not None:
            device_registry.async_update_device(device.id, **updates)

    @callback
    def _check_clock(self, payload: dict[str, Any]) -> None:
        """Push the time with NTP_SYNC if the device clock is off.

        The heartbeat's ts is the device's clock. A sync is sent when it is
        more than NTP_SYNC_DRIFT_THRESHOLD seconds from ours.
        """
        device_ts = payload.get("ts")
        if not isinstance(device_ts, int):
            return

        drift = abs(device_ts / 1000 - time.time())
        if drift <= NTP_SYNC_DRIFT_THRESHOLD:
            return

        now = time.monotonic()
        if (
            self._last_ntp_sync is not None
            and now - self._last_ntp_sync < NTP_SYNC_MIN_INTERVAL
        ):
            return

        self._last_ntp_sync = now
        _LOGGER.debug("Device clock is %.0fs off, sending NTP_SYNC", drift)
        self.hass.async_create_task(self._async_sync_ntp())

    # ------------------------------------------------------------------
    # Outbound protocol answers
    # ------------------------------------------------------------------

    async def _async_ack_event(self, cmd: str, payload: dict[str, Any]) -> None:
        """Ack one device event on device/event/sub.

        Mirrors the vendor broker: the command name, the event's own msgId and
        ``code: 0``, plus ``execStep`` echoed back for GRAIN_OUTPUT_EVENT.
        """
        ack: dict[str, Any] = {
            "cmd": cmd,
            "ts": int(time.time() * 1000),
            "msgId": payload["msgId"],
            "code": 0,
        }

        if exec_step := payload.get("execStep"):
            ack["execStep"] = exec_step

        if cmd == CMD_GET_FEEDING_PLAN:
            # The device is asking what its schedule should be. Answering with
            # a plan list is authoritative: an empty one wipes the schedule
            # stored on the device, which is only correct if Home Assistant
            # owns all feeding. Off by default.
            if self._feeding_plan_reply == FEEDING_PLAN_REPLY_EMPTY:
                ack["plans"] = []

        try:
            await publish_payload(self.hass, self.topics["event_ack"], ack)
        except PetlibroCommandError as err:
            _LOGGER.warning("Could not ack %s: %s", cmd, err)
            return

        _LOGGER.debug("Acked %s (msgId %s)", cmd, payload["msgId"])

    async def _async_reply_ntp(self) -> None:
        """Answer the device's NTP request on device/ntp/sub."""
        reply = {
            "cmd": CMD_NTP,
            "ts": int(time.time() * 1000),
            "code": 0,
            "calibrationTag": True,
            **time_zone_fields(dt_util.now()),
            "linkId": new_msg_id(),
        }

        try:
            await publish_payload(self.hass, self.topics["ntp_reply"], reply)
        except PetlibroCommandError as err:
            _LOGGER.warning("Could not answer NTP request: %s", err)
            return

        _LOGGER.debug("Answered NTP request with %s", reply)

    async def _async_sync_ntp(self) -> None:
        """Push the current time to the device on device/ntp/sub.

        Unlike the NTP reply, this is unsolicited and carries its own msgId,
        which the device echoes back on device/ntp/post. The ts in that echo
        is the device clock from before the sync; the next heartbeat shows
        whether it took.
        """
        sync = {
            "cmd": CMD_NTP_SYNC,
            "ts": int(time.time() * 1000),
            "msgId": new_msg_id(),
            **time_zone_fields(dt_util.now()),
            "linkId": new_msg_id(),
        }

        try:
            await publish_payload(self.hass, self.topics["ntp_reply"], sync)
        except PetlibroCommandError as err:
            _LOGGER.warning("Could not send NTP_SYNC: %s", err)
            return

        _LOGGER.debug("Sent NTP_SYNC %s", sync)

    async def async_request_state(self) -> None:
        """Ask the device for its full attribute state."""
        try:
            await self.async_send_command(CMD_ATTR_GET, expect_ack=False)
        except PetlibroCommandError as err:
            _LOGGER.debug("Could not request device state: %s", err)
            return

        _LOGGER.debug("Requested device state")

    async def async_send_command(
        self,
        cmd: str,
        expect_ack: bool = True,
        **payload: Any,
    ) -> dict[str, Any] | None:
        """Send a command and, by default, wait for the device to ack it.

        Args:
            cmd: Command name.
            expect_ack: Wait for the ack on device/service/post. Set False for
                commands the device cannot answer, such as DEVICE_REBOOT.
            **payload: Additional payload fields.

        Returns:
            The device's ack payload, or None when not waiting for one.

        Raises:
            PetlibroCommandError: If the publish fails, the device does not ack
                within COMMAND_ACK_TIMEOUT, or it acks with a non-zero code.
        """
        msg_id = new_msg_id()

        if not expect_ack:
            await publish_command(
                self.hass, self.topics["command"], cmd, msg_id=msg_id, **payload
            )
            return None

        future: asyncio.Future[dict[str, Any]] = self.hass.loop.create_future()
        self._pending_acks[msg_id] = future
        try:
            await publish_command(
                self.hass, self.topics["command"], cmd, msg_id=msg_id, **payload
            )
            try:
                async with asyncio.timeout(COMMAND_ACK_TIMEOUT):
                    ack = await future
            except TimeoutError as err:
                raise PetlibroCommandError(
                    f"Device did not acknowledge {cmd} within "
                    f"{COMMAND_ACK_TIMEOUT}s -- it is most likely offline or "
                    f"not connected to this broker"
                ) from err
        finally:
            self._pending_acks.pop(msg_id, None)

        code = ack.get("code")
        if code not in (0, None):
            raise PetlibroCommandError(
                f"Device rejected {cmd} with code {code}"
            )

        return ack

    # ------------------------------------------------------------------
    # Availability
    # ------------------------------------------------------------------

    @callback
    def _note_message(self) -> None:
        """Mark the device as alive and restart the silence timer."""
        if self._cancel_availability:
            self._cancel_availability()

        self._cancel_availability = async_call_later(
            self.hass, AVAILABILITY_TIMEOUT, self._availability_timeout
        )

        if self._available:
            return

        self._available = True
        _LOGGER.debug("Device %s is online", self.entry.data[CONF_SERIAL])
        self._notify_availability()

        # We may have missed the boot-time state push while it was away.
        self.hass.async_create_task(self.async_request_state())

    @callback
    def _availability_timeout(self, _now: Any) -> None:
        """Mark the device as unavailable after a period of silence."""
        self._cancel_availability = None
        if not self._available:
            return

        self._available = False
        _LOGGER.debug(
            "No message from %s %s in %ss, marking unavailable",
            self.entry.data[CONF_MODEL],
            self.entry.data[CONF_SERIAL],
            AVAILABILITY_TIMEOUT,
        )
        self._notify_availability()

    @callback
    def _notify_availability(self) -> None:
        """Push the current availability to every listener."""
        for listener in list(self._availability_listeners):
            listener(self._available)
