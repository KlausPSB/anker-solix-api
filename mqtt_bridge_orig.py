#!/usr/bin/env python3
import asyncio
import json
from datetime import datetime

from aiohttp import ClientSession
import paho.mqtt.client as mqtt

from api.api import AnkerSolixApi
from api.mqtt import AnkerSolixMqttSession
from api.apitypes import SolarbankUsageMode

LOCAL_MQTT_HOST = "localhost"
LOCAL_MQTT_PORT = 1883

# Anpassen: Zugangsdaten & IDs
EMAIL = "klaus@dres-stadler.de"
PASSWORD = "*Lzh4bb8QQtW"
COUNTRY = "DE"

SITE_ID = "fcea0c4c-799e-4aa3-b580-b6a75a8ab5af"
DEVICE_SN = "APCGQ80E21300388"

# Topics für Kommandos aus Symcon
CMD_TOPIC_USAGE_MODE = f"anker/cmd/{DEVICE_SN}/usage_mode"
CMD_TOPIC_JSON       = f"anker/cmd/{DEVICE_SN}/json"

# Erlaubte REST-APIs (schreibend)
ALLOWED_REST_APIS = {
    "set_sb2_home_load",
    "set_sb2_ac_charge",
    "set_sb2_use_time",
    "set_device_parm",
    "set_device_attributes",
}

# Erlaubte REST-APIs (lesend, Ergebnis wird auf MQTT zurückgegeben)
READ_REST_APIS = {
    "device_pv_energy_daily",
    "get_device_pv_statistics",
    "energy_daily",
    "energy_analysis",
    # "get_device_pv_total_statistics",  # nur, falls in deiner api.py vorhanden
}


def make_forwarding_callback(local_client: mqtt.Client):
    """Callback für Anker-MQTT → lokalen Mosquitto weiterleiten."""

    def callback(
        session: AnkerSolixMqttSession,
        topic: str,
        message: dict,
        data,
        model: str,
        device_sn: str,
        extracted_values: dict,
    ):
        if not device_sn or not model or not isinstance(extracted_values, dict):
            return

        base = f"anker/{model}/{device_sn}"

        # Alle Wergy_analysis",
    # "get_device_pv_total_statistics",  # nur, falls in deinerclient.publish(state_topic, json.dumps(extracted_values), qos=0, retain=False)

        # Einzelne Werte auf flache Topics legen
        for key, value in extracted_values.items():
            local_client.publish(f"{base}/{key}", json.dumps(value), qos=0, retain=False)

    return callback


# ---------------------------------------------------------------------------
# Generischer REST-JSON-Handler (Write + Read, inkl. energy_daily-Erweiterung)
# ---------------------------------------------------------------------------

async def handle_rest_json(api: AnkerSolixApi, cmd: dict, local_client: mqtt.Client) -> None:
    """
    Generischer JSON-REST-Handler.

    Erwartetes JSON auf CMD_TOPIC_JSON (anker/cmd/<SN>/json), z.B.:

    {
      "api": "set_sb2_home_load",
      "params": { "usage_mode": "manual", "preset": 200 },
      "desc": "Symcon: 200W Einspeisung"
    }

    Für Read-APIs (z.B. energy_daily) wird das Ergebnis unter
    anker/api/<SN>/<api_name> als JSON publiziert.
    """
    api_name = cmd.get("api")
    params   = cmd.get("params") or {}
    desc     = cmd.get("desc", f"REST cmd {api_name}")

    if not isinstance(api_name, str):
        print("JSON ohne 'api'-Name:", cmd, flush=True)
        return

    is_write = api_name in ALLOWED_REST_APIS
    is_read  = api_name in READ_REST_APIS

    if not (is_write or is_read):
        print("REST-Methode nicht erlaubt:", api_name, flush=True)
        return

    # siteId / deviceSn standardmäßig ergänzen
    if "siteId" not in params and "site_id" not in params and (is_write or api_name in {"energy_daily", "energy_analysis"}):
        params["siteId"] = SITE_ID
    if "deviceSn" not in params and "device_sn" not in params:
        params["deviceSn"] = DEVICE_SN

    # Funktionsobjekt holen
    func = getattr(api, api_name, None)
    if not callable(func):
        print("REST-Methode nicht im Api-Objekt gefunden:", api_name, flush=True)
        return

    try:
        # Spezielle Behandlung für energy_daily, um date_start/numDays/devTypes zu unterstützen
        if api_name == "energy_daily":
            # Defaults
            site_id   = params.get("siteId", SITE_ID)
            device_sn = params.get("deviceSn", DEVICE_SN)

            # date_start: "YYYY-MM-DD"
            date_start = params.get("date_start")
            if isinstance(date_start, str):
                try:
                    startDay = datetime.strptime(date_start, "%Y-%m-%d")
                except ValueError:
                    print(f"Ungültiges date_start-Format für energy_daily: {date_start}", flush=True)
                    startDay = datetime.today()
            else:
                startDay = datetime.today()

            # numDays
            try:
                numDays = int(params.get("numDays", 1))
            except (TypeError, ValueError):
                numDays = 1

            # dayTotals
            dayTotals = bool(params.get("dayTotals", False))

            # devTypes als Liste ["solarbank", "smartmeter", ...]
            devTypes_list = params.get("devTypes", [])
            if isinstance(devTypes_list, list):
                devTypes = set(devTypes_list)
            else:
                devTypes = set()

            call_params = {
                "siteId": site_id,
                "deviceSn": device_sn,
                "startDay": startDay,
                "numDays": numDays,
                "dayTotals": dayTotals,
                "devTypes": devTypes,
            }

            print(f"REST-Call energy_daily({call_params}) - {desc}", flush=True)
            result = await api.energy_daily(**call_params)

        else:
            # Alle anderen APIs: direkt mit params aufrufen
            print(f"REST-Call {api_name}({params}) - {desc}", flush=True)
            result = await func(**params)

        print("REST-Result:", result, flush=True)

        if is_read:
            # Antwort auf MQTT publizieren
            out_topic = f"anker/api/{DEVICE_SN}/{api_name}"
            try:
                payload = json.dumps(result, ensure_ascii=False)
            except TypeError:
                payload = json.dumps({"error": "result not JSON-serializable"})

            local_client.publish(out_topic, payload, qos=0, retain=False)
            print(f"Read-Result auf {out_topic} publiziert", flush=True)

    except Exception as e:
        print(f"Fehler bei REST-Call {api_name}: {e}", flush=True)


# ---------------------------------------------------------------------------
# MQTT-Command-Handler (usage_mode + JSON)
# ---------------------------------------------------------------------------

async def handle_command_message(api: AnkerSolixApi, msg, local_client: mqtt.Client):
    """MQTT-Command-Handler: usage_mode + generische JSON-REST-Kommandos."""
    topic = msg.topic
    payload_raw = msg.payload.decode().strip()
    payload = payload_raw.lower()

    # 1) Generische JSON-Kommandos → REST-API
    if topic == CMD_TOPIC_JSON:
        try:
            cmd = json.loads(payload_raw)
        except Exception:
            print(f"Ignoriere ungültiges JSON: {payload_raw}", flush=True)
            return

        await handle_rest_json(api, cmd, local_client)
        return

    # 2) Spezialfall: usage_mode (smartmeter / manual / manual_<Watt>)
    if topic == CMD_TOPIC_USAGE_MODE:
        if payload == "smartmeter":
            mode_enum = SolarbankUsageMode.smartmeter
            preset = 200  # gewünschter Home-Load bei Smartmeter (anpassen)
        elif payload == "manual":
            mode_enum = SolarbankUsageMode.manual
            preset = 0  # Manual mit 0 W Einspeisung
        elif payload.startswith("manual_"):
            # z.B. "manual_300" -> Manual mit 300 W
            try:
                val = int(payload.split("_", 1)[1])
                if val < 0:
                    raise ValueError
                mode_enum = SolarbankUsageMode.manual
                preset = val
            except ValueError:
                print(f"Ignoriere ungültigen manual Wert: {payload}", flush=True)
                return
        else:
            print(f"Ignoriere unbekannten usage_mode: {payload}", flush=True)
            return

        try:
            result = await api.set_sb2_home_load(
                siteId=SITE_ID,
                deviceSn=DEVICE_SN,
                usage_mode=mode_enum.value,
                preset=preset,
            )
            print(f"set_sb2_home_load -> {payload}, preset {preset}, result: {result}", flush=True)
        except Exception as e:
            print(f"Fehler beim Setzen von usage_mode: {e}", flush=True)


# ---------------------------------------------------------------------------
# Lokaler MQTT-Client (Bridge + Command-Subscribe)
# ---------------------------------------------------------------------------

def setup_local_mqtt(api: AnkerSolixApi, loop: asyncio.AbstractEventLoop) -> mqtt.Client:
    """Lokalen MQTT-Client einrichten (Bridge + Command-Subscribe)."""
    print("setup_local_mqtt: Starte lokalen MQTT-Client...", flush=True)
    #client = mqtt.Client()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    def on_connect(local_client, userdata, flags, reason_code, properties):
      print(f"on_connect: rc={reason_code}", flush=True)
      if reason_code == 0:
          print(f"Lokaler MQTT verbunden mit {LOCAL_MQTT_HOST}:{LOCAL_MQTT_PORT}", flush=True)
          local_client.subscribe(CMD_TOPIC_USAGE_MODE, qos=1)
          local_client.subscribe(CMD_TOPIC_JSON, qos=1)
          print("Subscribed auf: " + CMD_TOPIC_USAGE_MODE + " und " + CMD_TOPIC_JSON, flush=True)
      else:
          print(f"Lokaler MQTT Connect fehlgeschlagen, rc={reason_code}", flush=True)

    def on_disconnect(local_client, userdata, disconnect_flags, reason_code, properties):
         print(f"on_disconnect: rc={reason_code}", flush=True)

    def on_message(local_client, userdata, msg):
        print(f"on_message: {msg.topic} -> {msg.payload.decode(errors='ignore')}", flush=True)
        loop.call_soon_threadsafe(
            lambda: asyncio.create_task(handle_command_message(api, msg, client))
        )

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    print(f"Verbinde zu lokalem MQTT {LOCAL_MQTT_HOST}:{LOCAL_MQTT_PORT} ...", flush=True)
    client.connect(LOCAL_MQTT_HOST, LOCAL_MQTT_PORT, keepalive=60)
    client.loop_start()
    print("setup_local_mqtt: loop_start() aufgerufen", flush=True)

    return client

# ---------------------------------------------------------------------------
# Hauptprogramm
# ---------------------------------------------------------------------------

async def main():
    loop = asyncio.get_running_loop()

    async with ClientSession() as websession:
        api = AnkerSolixApi(EMAIL, PASSWORD, COUNTRY, websession, None)
        await api.async_authenticate()

        # Sites und Devices holen
        await api.update_sites()
        await api.get_bind_devices()

        # lokaler MQTT-Client (Forwarding + Commands)
        local_client = setup_local_mqtt(api, loop)

        # Anker-MQTT-Session starten
        mqtt_session: AnkerSolixMqttSession | None = await api.startMqttSession()
        if not (mqtt_session and mqtt_session.is_connected()):
            print("MQTT session could not be started", flush=True)
            return

        # Callback für Anker-Daten setzen
        mqtt_session.message_callback(make_forwarding_callback(local_client))

        # Device-Topics zusammenstellen
        topics = set()
        for dev in api.devices.values():
            if prefix := mqtt_session.get_topic_prefix(deviceDict=dev):
                topics.add(f"{prefix}#")

        print("Bridge subscribing topics:", topics, flush=True)
        print("Listening for commands on:", CMD_TOPIC_USAGE_MODE, "and", CMD_TOPIC_JSON, flush=True)

        # Poller starten
        await mqtt_session.message_poller(
            topics=topics,
            trigger_devices=set(),
            msg_callback=None,
            timeout=60,
        )


if __name__ == "__main__":
    asyncio.run(main())
