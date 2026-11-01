"""Helper functions for the Petlibro Local integration."""
from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime
from typing import Any

from homeassistant.components import mqtt
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError

_LOGGER = logging.getLogger(__name__)


class PetlibroCommandError(HomeAssistantError):
    """Error when sending a command to the Petlibro device."""


def new_msg_id() -> str:
    """Return a message ID in the format the vendor cloud uses.

    The cloud sends a UUID with the dashes stripped (32 hex characters). The
    device echoes whatever it is given, but a 36 character value with dashes is
    a format the firmware was never shipped against, so stay on the observed
    one.
    """
    return uuid.uuid4().hex


def time_zone_fields(now: datetime) -> dict[str, int]:
    """Return the time zone fields the vendor cloud sends with NTP and NTP_SYNC.

    Matches the cloud's NTP reply captured from a PLAF103: the current UTC
    offset in seconds, the offset after each of the next two changes with the
    epoch ms it takes effect, and the current offset in whole hours. A zone
    without offset changes repeats the current offset with a time of 0.

    Args:
        now: The current time, aware, in the zone to describe.
    """
    offset = _utc_offset_seconds(now.tzinfo, int(now.timestamp()))
    changes = _next_offset_changes(now, 2)
    while len(changes) < 2:
        changes.append((0, offset))

    return {
        "timezoneOffsetSeconds": offset,
        "nextDSTOffsetSeconds": changes[0][1],
        "nextDSTTransitionTs": changes[0][0] * 1000,
        "secondNextDSTOffsetSeconds": changes[1][1],
        "secondNextDSTTransitionTs": changes[1][0] * 1000,
        "timezone": offset // 3600,
    }


def _utc_offset_seconds(tz: Any, epoch: int) -> int:
    """Return the UTC offset of a zone at an epoch second, in seconds."""
    offset = datetime.fromtimestamp(epoch, tz).utcoffset()
    return int(offset.total_seconds()) if offset else 0


def _next_offset_changes(now: datetime, count: int) -> list[tuple[int, int]]:
    """Find up to ``count`` UTC offset changes after ``now``.

    Steps a day at a time for up to two years, then bisects to the second at
    which the new offset starts.

    Returns:
        (epoch second, new offset in seconds) for each change found.
    """
    day = 86400
    tz = now.tzinfo
    start = int(now.timestamp())
    offset = _utc_offset_seconds(tz, start)
    changes: list[tuple[int, int]] = []

    lo = start
    while len(changes) < count and lo < start + 2 * 366 * day:
        hi = lo + day
        if _utc_offset_seconds(tz, hi) == offset:
            lo = hi
            continue
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if _utc_offset_seconds(tz, mid) == offset:
                lo = mid
            else:
                hi = mid
        offset = _utc_offset_seconds(tz, hi)
        changes.append((hi, offset))
        lo = hi

    return changes


async def publish_payload(
    hass: HomeAssistant,
    topic: str,
    payload: dict[str, Any],
) -> None:
    """Publish a fully built JSON payload to the device.

    Args:
        hass: Home Assistant instance.
        topic: MQTT topic to publish to.
        payload: The payload to publish, serialized as-is.

    Raises:
        PetlibroCommandError: If the MQTT publish fails.
    """
    try:
        await mqtt.async_publish(
            hass,
            topic,
            json.dumps(payload),
            qos=0,
            retain=False,
        )
    except Exception as err:
        _LOGGER.error(
            "Failed to publish %s to %s: %s", payload.get("cmd"), topic, err
        )
        raise PetlibroCommandError(
            f"Failed to send {payload.get('cmd')} to device: {err}"
        ) from err


async def publish_command(
    hass: HomeAssistant,
    topic: str,
    cmd: str,
    msg_id: str | None = None,
    **kwargs: Any,
) -> str:
    """Publish a command to the Petlibro device.

    Automatically adds timestamp and message ID to the payload.

    Args:
        hass: Home Assistant instance.
        topic: MQTT topic to publish to.
        cmd: Command name.
        msg_id: Message ID to use; generated when omitted.
        **kwargs: Additional payload fields.

    Returns:
        The message ID the command was sent with, for correlating the ack.

    Raises:
        PetlibroCommandError: If the MQTT publish fails.
    """
    msg_id = msg_id or new_msg_id()
    payload = {
        "cmd": cmd,
        **kwargs,
        "ts": int(time.time() * 1000),
        "msgId": msg_id,
    }

    await publish_payload(hass, topic, payload)
    return msg_id
