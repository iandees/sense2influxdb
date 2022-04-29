import os
import sys
import time
import sense_energy
import json
from sense_energy import Senseable
from sense_energy.sense_api import WS_URL, WSS_TIMEOUT
from influxdb import InfluxDBClient
from websocket import create_connection
from websocket._exceptions import WebSocketTimeoutException
from threading import Thread


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
                voltage = item.get('voltage')
                if voltage is None or len(voltage) != 2:
                    print("Missing voltage:", json.dumps(result))
                    continue

                watts = item.get('channels')
                if watts is None or len(watts) != 2:
                    print("Missing watts:", json.dumps(result))
                    continue

                freq = item.get('hz')
                if freq is None:
                    print("Missing freq:", json.dumps(result))
                    continue

                c = item.get('c')
                if c is None:
                    print("Missing c:", json.dumps(result))
                    continue

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


def poll_devices(sense_client: sense_energy.Senseable, influx_client: InfluxDBClient) -> None:
    devices = sense_client.get_discovered_device_data()

    body = []
    # Process "always on" first
    data = sense_client.get_device_info('always_on')
    usage = data['usage']
    device = data['device']

    usage_fields = ["avg_monthly_KWH", "avg_monthly_pct", "avg_watts", "yearly_KWH", "yearly_text", "yearly_cost", "avg_monthly_cost", "current_ao_wattage"]

    point = {
        "measurement": "sense_devices",
        # "time": device['last_state_time'], There's no time on always_on
        "tags": {
            "device_id": device['id'],
            "name": device['name'],
        },
        "fields": {
            "icon": "always_on",
        }
    }

    for field in usage_fields:
        if field_value := usage.get(field):
            if field == 'yearly_cost':
                point['fields'][field] = field_value/100.0
            else:
                point['fields'][field] = field_value

    body.append(point)

    # Now the rest of the devices
    for device in devices:
        device_id = device.get('id')

        if device_id in ('always_on', 'other', 'unknown'):
            # Process these separately
            continue

        data = sense_client.get_device_info(device.get('id'))
        usage = data['usage']
        device = data['device']

        point = {
            "measurement": "sense_devices",
            "time": device['last_state_time'],
            "tags": {
                "device_id": device['id'],
                "name": device['name'],
            },
            "fields": {
                'last_state': device['last_state'],
            }
        }

        usage_fields = [
            "current_month_runs",
            "current_month_KWH",
            "avg_monthly_runs",
            "avg_monthly_KWH",
            "avg_monthly_pct",
            "avg_watts",
            "avg_duration",
            "yearly_KWH",
            "yearly_text",
            "yearly_cost",
            "current_month_cost",
            "avg_monthly_cost",
            "current_ao_wattage",
        ]

        for field in usage_fields:
            if field_value := usage.get(field):
                if field == 'yearly_cost':
                    point['fields'][field] = field_value/100.0
                else:
                    point['fields'][field] = field_value

        body.append(point)

    influx_client.write_points(body, time_precision='s')


if __name__ == '__main__':
    sense = Senseable()
    sense_username = os.environ.get("SENSE_USERNAME")
    sense_password = os.environ.get("SENSE_PASSWORD")
    influx_host = os.environ.get("INFLUXDB_HOST")
    influx_port = int(os.environ.get("INFLUXDB_PORT", "8086"))
    influx_user = os.environ.get("INFLUXDB_USERNAME")
    influx_pass = os.environ.get("INFLUXDB_PASSWORD")
    influx_db = os.environ.get("INFLUXDB_DB")
    device_poll_time_sec = int(os.environ.get("DEVICE_POLL_TIME", "60"))

    if not (sense_username and sense_password):
        print("Set SENSE_USERNAME and SENSE_PASSWORD")
        sys.exit(1)

    sense.authenticate(sense_username, sense_password)

    if not (influx_host and influx_port and influx_user and influx_pass and influx_db):
        print("Set INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, and INFLUXDB_DB")
        sys.exit(1)

    ifclient = InfluxDBClient(influx_host, influx_port, influx_user, influx_pass, influx_db)

    def run_websocket(s, i):
        while True:
            try:
                start_listening(s, i)
            except WebSocketTimeoutException:
                print("Connection timeout. Trying again.")

    def run_device_poll(s, i):
        while True:
            start_time = time.time()

            try:
                poll_devices(sense, ifclient)
            except Exception as e:
                print("Polling failed: ", e)

            end_time = time.time()
            elapsed = (end_time - start_time)
            sleep_time = device_poll_time_sec - elapsed

            print(f"Sleeping {sleep_time:0.1f} secs")
            time.sleep(sleep_time)


    websocket_thread = Thread(target=run_websocket, args=(sense, ifclient))
    poll_thread = Thread(target=run_device_poll, args=(sense, ifclient))

    websocket_thread.start()
    poll_thread.start()

    websocket_thread.join()
    poll_thread.join()
