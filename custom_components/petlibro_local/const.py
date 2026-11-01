"""Constants for the Petlibro Local integration."""

from homeassistant.const import Platform

DOMAIN = "petlibro_local"

CONF_MODEL = "model"
CONF_SERIAL = "serial"
CONF_FEEDING_PLAN_REPLY = "feeding_plan_reply"

# How the integration answers the device's GET_FEEDING_PLAN_EVENT.
#   ack_only -> {"code": 0} with no "plans" key: stops the retries without
#               asserting anything about the on-device schedule.
#   empty    -> {"code": 0, "plans": []}: also clears the schedule stored on
#               the device, so only Home Assistant feeds it.
FEEDING_PLAN_REPLY_ACK_ONLY = "ack_only"
FEEDING_PLAN_REPLY_EMPTY = "empty"
FEEDING_PLAN_REPLY_OPTIONS = [FEEDING_PLAN_REPLY_ACK_ONLY, FEEDING_PLAN_REPLY_EMPTY]
DEFAULT_FEEDING_PLAN_REPLY = FEEDING_PLAN_REPLY_ACK_ONLY

TOPIC_BASE = "dl/{model}/{serial}/device"

# Device -> broker
TOPIC_EVENT_SUFFIX = "/event/post"
TOPIC_HEARTBEAT_SUFFIX = "/heart/post"
TOPIC_NTP_SUFFIX = "/ntp/post"
TOPIC_SERVICE_ACK_SUFFIX = "/service/post"

# Broker -> device
TOPIC_COMMAND_SUFFIX = "/service/sub"
TOPIC_EVENT_ACK_SUFFIX = "/event/sub"
TOPIC_NTP_REPLY_SUFFIX = "/ntp/sub"


def get_topics(model: str, serial: str) -> dict[str, str]:
    """Build MQTT topics from model and serial."""
    base = TOPIC_BASE.format(model=model, serial=serial)
    return {
        # inbound (the device publishes here, we subscribe)
        "event": base + TOPIC_EVENT_SUFFIX,
        "heartbeat": base + TOPIC_HEARTBEAT_SUFFIX,
        "ntp": base + TOPIC_NTP_SUFFIX,
        "service_ack": base + TOPIC_SERVICE_ACK_SUFFIX,
        # outbound (we publish here, the device subscribes)
        "command": base + TOPIC_COMMAND_SUFFIX,
        "event_ack": base + TOPIC_EVENT_ACK_SUFFIX,
        "ntp_reply": base + TOPIC_NTP_REPLY_SUFFIX,
    }


SERVICE_SEND_COMMAND = "send_command"
SERVICE_FEED = "feed"
ATTR_CMD = "cmd"
ATTR_PAYLOAD = "payload"
ATTR_PORTIONS = "portions"
ATTR_WAIT_FOR_ACK = "wait_for_ack"

PLATFORMS: list[Platform] = [
    Platform.BUTTON,
    Platform.EVENT,
    Platform.LIGHT,
    Platform.SENSOR,
    Platform.SWITCH,
]

# Commands we send to the device (device/service/sub)
CMD_ATTR_SET = "ATTR_SET_SERVICE"
CMD_ATTR_GET = "ATTR_GET_SERVICE"
CMD_MANUAL_FEEDING = "MANUAL_FEEDING_SERVICE"
CMD_DEVICE_REBOOT = "DEVICE_REBOOT"

# Events the device sends us (device/event/post)
CMD_ATTR_PUSH = "ATTR_PUSH_EVENT"
CMD_GRAIN_OUTPUT = "GRAIN_OUTPUT_EVENT"
CMD_DEVICE_START = "DEVICE_START_EVENT"
CMD_DEVICE_LOG_REPORT = "DEVICE_LOG_REPORT_EVENT"
CMD_GET_FEEDING_PLAN = "GET_FEEDING_PLAN_EVENT"
CMD_DETECTION_EVENT = "DETECTION_EVENT"

# Other
CMD_HEARTBEAT = "HEARTBEAT"
CMD_NTP = "NTP"
CMD_NTP_SYNC = "NTP_SYNC"

# GRAIN_OUTPUT_EVENT execution steps
GRAIN_STEP_START = "GRAIN_START"
GRAIN_STEP_END = "GRAIN_END"

# The device heartbeats about every 60s. Give it three misses before we call
# it gone, so a single dropped packet does not flap every entity.
AVAILABILITY_TIMEOUT = 210

# How long to wait for the device's ack on device/service/post before telling
# the user the command did not land.
COMMAND_ACK_TIMEOUT = 8

# The device runs its feeding schedule on its own clock and only asks for the
# time on boot and when a plan fires, and a PLAF103 on firmware 1.1.1 was seen
# losing about 3.5 seconds a minute. When a heartbeat's ts is further than this
# many seconds from ours, push the time with NTP_SYNC.
NTP_SYNC_DRIFT_THRESHOLD = 60

# Minimum seconds between NTP_SYNC pushes, so a device that ignores them is not
# sent one on every heartbeat.
NTP_SYNC_MIN_INTERVAL = 300

# Number of recently seen (cmd, msgId, execStep) tuples remembered so that the
# device's retransmissions get re-acked but not processed twice.
SEEN_EVENT_CACHE_SIZE = 64

# Maintenance timestamps tracked by Home Assistant (the device has no such
# attribute -- these are bookkeeping for the human).
MAINTENANCE_LAST_CLEANED = "last_cleaned"
MAINTENANCE_LAST_REFILL = "last_refill"
MAINTENANCE_KEYS = [MAINTENANCE_LAST_CLEANED, MAINTENANCE_LAST_REFILL]
