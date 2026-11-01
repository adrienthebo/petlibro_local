# Petlibro Local

Control a Petlibro `PLAF103` pet feeder from Home Assistant over your own MQTT broker, with
no vendor cloud involved.

The feeder speaks plaintext MQTT 3.1 on port 1883 with static credentials and no per-device
authorisation, so a DNS rewrite for `mqtt.us.petlibro.com` pointing at your own broker is
enough to take it over completely. This integration implements the broker side of that
protocol on top of Home Assistant's built-in MQTT integration, so it reuses HA's existing
broker connection rather than opening its own.

> Not affiliated with or endorsed by Petlibro. Reverse engineered from traffic captured on my
> own device, on my own network.

## What you get

| Entity | Platform | Mechanism |
|---|---|---|
| Indicator Lights | `light` | `ATTR_SET_SERVICE` / `lightSwitch` |
| Hardware Button Lock | `switch` | `ATTR_SET_SERVICE` / `disableHardwareButton` |
| Sound | `switch` | `ATTR_SET_SERVICE` / `soundSwitch` |
| Feed | `button` | `MANUAL_FEEDING_SERVICE`, one portion |
| Reboot | `button` | `DEVICE_REBOOT` (never observed on the wire — unverified) |
| Grain Dispensed | `event` | fires once per dispense, on `GRAIN_END` |
| Last cleaned | `sensor` | timestamp, Home Assistant side, survives restarts |
| Last refill | `sensor` | timestamp, Home Assistant side, survives restarts |
| Mark cleaned / Mark refilled | `button` | stamps the matching sensor with now |

The feeder itself reports nothing about cleaning or refilling, so those four are bookkeeping.
The sensors are read-only and carry the `timestamp` device class, so the frontend shows them
as "3 days ago"; pressing the matching button is the only thing that moves them. They survive
restarts and stay available while the feeder is offline — you clean it with the power off.

The `Grain Dispensed` event carries `portions`, `expected_portions`, `short_fed` (a dispense
that fell short means the hopper jammed or ran empty), `feed_type` (`1` scheduled, `2` manual)
and `plan_id`.

## Requirements

- Home Assistant 2024.7 or newer with the **MQTT integration** already set up
- An MQTT broker on your LAN **listening on port 1883** — the port is fixed in the feeder's
  firmware
- A DNS rewrite on your router pointing `mqtt.us.petlibro.com` at that broker
- The feeder's serial number, printed on the device and on the box (it is also the MQTT
  client ID)

## Installation

### HACS (custom repository)

1. HACS → ⋮ → **Custom repositories**
2. Repository: `https://github.com/j4n-e4t/petlibro_local`, category: **Integration**
3. Install **Petlibro Local**, then restart Home Assistant
4. **Settings → Devices & Services → Add Integration → Petlibro Local**

### Manual

Copy `custom_components/petlibro_local` into your Home Assistant `config/custom_components/`
directory and restart.

## Configuration

The config flow asks for the model (`PLAF103`), the serial number, and an optional name. All
MQTT topics are derived from those:

```
dl/<model>/<serial>/device/service/sub     commands out
dl/<model>/<serial>/device/event/post      events in
dl/<model>/<serial>/device/event/sub       event acks out
dl/<model>/<serial>/device/heart/post      heartbeat in
dl/<model>/<serial>/device/ntp/post        time requests in
dl/<model>/<serial>/device/ntp/sub         time replies out
dl/<model>/<serial>/device/service/post    command acks in
```

### Answering the device

The feeder publishes at QoS 1 and **retries every event until something acknowledges it**, and
keeps asking for the time until something answers. A broker that only listens will make it
republish forever. This integration plays the part the vendor cloud used to:

- acks every event on `device/event/sub` with `code: 0`, retransmissions included
- answers `NTP` on `device/ntp/sub` with the current time, your HA time zone's UTC offset and
  its next two DST changes
- sends `NTP_SYNC` on `device/ntp/sub` when a heartbeat shows the device clock more than 60s
  off, at most once every 5 minutes. The feeder runs its own schedule on that clock, and loses
  minutes an hour without it.
- watches `device/service/post` for the device's verdict on commands we send, so a command
  that does not land raises an error instead of silently doing nothing

### Options

**Answer to feeding schedule requests** — on boot the feeder asks the broker what its schedule
should be (`GET_FEEDING_PLAN_EVENT`) and will keep asking until answered. An empty `plans`
array is an *answer*, not an acknowledgement: it erases the schedule stored on the device.

- *Acknowledge only* (default) — answers with a bare `code: 0` and leaves the on-device
  schedule alone.
- *Clear the device schedule* — sends `plans: []`, so only Home Assistant ever feeds the
  device. Choose this if you drive feeding from HA automations and do not want the feeder
  dispensing on its own.

## Actions

`petlibro_local.feed` — dispense a number of portions:

```yaml
action: petlibro_local.feed
target:
  device_id: <device>
data:
  portions: 2
```

`petlibro_local.send_command` — raw escape hatch onto the command topic, for protocol poking
from Developer Tools:

```yaml
action: petlibro_local.send_command
target:
  device_id: <device>
data:
  cmd: ATTR_SET_SERVICE
  payload:
    volume: 30
  wait_for_ack: true
```

`cmd` and `ts`/`msgId` are filled in for you. With `wait_for_ack` on (the default) the action
fails if the device does not acknowledge within 8 seconds; turn it off for commands the device
cannot answer, such as `DEVICE_REBOOT`.

## Troubleshooting

Every device entity goes **unavailable** after 210 seconds without a message. The feeder
heartbeats about once a minute, so unavailable means it is genuinely not talking to your
broker — check the DNS rewrite, that the broker is on 1883, and power-cycle the feeder so it
re-resolves the hostname.

Watch the traffic:

```console
$ mosquitto_sub -h <broker> -v -t 'dl/PLAF103/#'
```

Turn on debug logging:

```yaml
logger:
  logs:
    custom_components.petlibro_local: debug
```

## Icon

HACS serves the integration's icon from `custom_components/petlibro_local/brand/`, so it
shows up as soon as the integration is installed — no external submission needed.

Home Assistant core resolves icons from
[home-assistant/brands](https://github.com/home-assistant/brands) instead. The identical files
under `brands/custom_integrations/petlibro_local/` are sized to that repository's spec
(256×256 and 512×512, square, transparent) and ready to drop into a PR there, which is what
makes the icon appear outside HACS too.

`icon.png` / `icon@2x.png` are the two-tone original. `icon-monochrome-alt.png` /
`icon-monochrome-alt@2x.png` are a single-colour variant — the original's dark grey mark has
very little contrast on Home Assistant's dark theme, and neither HACS nor brands supports
per-theme variants, so swap in the monochrome pair if that bothers you.

Entity and action icons come from `icons.json` and need nothing external.

## Translations

English and German. A custom integration loads `custom_components/petlibro_local/translations/`
at runtime and never reads `strings.json`, which is only the source file, so the two are kept
identical and CI fails if they drift — otherwise entity names silently collapse to the bare
device name.

## Protocol notes

A full write-up of the capture setup and the protocol — topic layout, every observed command,
and which payloads are confirmed on the wire versus inferred — lives in `WRITEUP.md` in the
`mqtt_bridge` research repo alongside the packet captures it is based on.

## License

MIT
