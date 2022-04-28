import os
import sys

import sense_energy
import json
from sense_energy import Senseable
from sense_energy.sense_api import WS_URL, WSS_TIMEOUT
from influxdb import InfluxDBClient
from websocket import create_connection
from websocket._exceptions import WebSocketTimeoutException


def start_listening(sense_client: sense_energy.Senseable, influx_client: InfluxDBClient) -> None:
    ws = 0
    url = WS_URL % (sense_client.sense_monitor_id, sense_client.sense_access_token)
    try:
        ws = create_connection(
            url, timeout=WSS_TIMEOUT,
        )
        while True:  # hello, features, [updates,] data
            result = json.loads(ws.recv())
            event_type = result.get("type")
            if event_type == "realtime_update":
                item = result["payload"]

                epoch = item['epoch']
                voltage = item['voltage']
                watts = item['channels']
                freq = item['hz']
                c = item['c']

                body = [
                    {
                        "measurement": "sense_mains",
                        "time": epoch,
                        "tags": {"leg": "L1"},
                        "fields": {
                            "voltage": voltage[0],
                            "watts": watts[0],
                        },
                    },
                    {
                        "measurement": "sense_mains",
                        "time": epoch,
                        "tags": {"leg": "L2"},
                        "fields": {
                            "voltage": voltage[1],
                            "watts": watts[1],
                        },
                    },
                    {
                        "measurement": "sense_mains",
                        "time": epoch,
                        "fields": {
                            "c": c,
                            "hz": freq,
                        },
                    },
                ]

                for device in item.get('devices', []):
                    device_point = {
                        "measurement": "sense_devices",
                        "time": epoch,
                        "tags": {
                            "id": device['id'],
                            "name": device['name'],
                        },
                        "fields": {
                            "current_watts": device['w'],
                        }
                    }

                    body.append(device_point)

                influx_client.write_points(body, time_precision='s')
            elif event_type == "new_timeline_event":
                item = result["payload"]
                print(json.dumps(item))

                body = []
                for added in item['items_added']:
                    action_point = {
                        "measurement": "sense_event",
                        "time": added['time'],
                        "tags": {
                            "device_id": added['device_id'],
                            "event_type": "new_timeline",
                            "name": added['body'].replace(" turned on", "").replace(" turned off", ""),
                        },
                        "fields": {
                            "type": added['type'],
                            "icon": added['icon'],
                            "body": added['body'],
                            "device_state": added['device_state'],
                            "user_device_type": added['user_device_type'],
                            "device_transition_from_state": added['device_transition_from_state'],
                        }
                    }

                    body.append(action_point)

                for removed in item['items_removed']:
                    action_point = {
                        "measurement": "sense_event",
                        "time": removed['time'],
                        "tags": {
                            "device_id": removed['device_id'],
                            "event_type": "new_timeline",
                        },
                        "fields": {
                            "type": removed['type'],
                            "icon": removed['icon'],
                            "body": removed['body'],
                            "device_state": removed['device_state'],
                            "user_device_type": removed['user_device_type'],
                            "device_transition_from_state": removed['device_transition_from_state'],
                        }
                    }

                    body.append(action_point)

                for updated in item['items_updated']:
                    action_point = {
                        "measurement": "sense_event",
                        "time": updated['time'],
                        "tags": {
                            "device_id": updated['device_id'],
                            "event_type": "new_timeline",
                        },
                        "fields": {
                            "type": updated['type'],
                            "icon": updated['icon'],
                            "body": updated['body'],
                            "device_state": updated['device_state'],
                            "user_device_type": updated['user_device_type'],
                            "device_transition_from_state": updated['device_transition_from_state'],
                        }
                    }

                    body.append(action_point)

                influx_client.write_points(body, time_precision='s')

            else:
                print(json.dumps(result))
    except WebSocketTimeoutException:
        raise
    finally:
        if ws:
            ws.close()


if __name__ == '__main__':
    sense = Senseable()
    sense_username = os.environ.get("SENSE_USERNAME")
    sense_password = os.environ.get("SENSE_PASSWORD")
    influx_host = os.environ.get("INFLUXDB_HOST")
    influx_port = int(os.environ.get("INFLUXDB_PORT", "8086"))
    influx_user = os.environ.get("INFLUXDB_USERNAME")
    influx_pass = os.environ.get("INFLUXDB_PASSWORD")
    influx_db = os.environ.get("INFLUXDB_DB")

    if not (sense_username and sense_password):
        print("Set SENSE_USERNAME and SENSE_PASSWORD")
        sys.exit(1)

    sense.authenticate(sense_username, sense_password)

    if not (influx_host and influx_port and influx_user and influx_pass and influx_db):
        print("Set INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, and INFLUXDB_DB")
        sys.exit(1)

    ifclient = InfluxDBClient(influx_host, influx_port, influx_user, influx_pass, influx_db)

    while True:
        try:
            start_listening(sense, ifclient)
        except WebSocketTimeoutException:
            print("Connection timeout. Trying again.")
