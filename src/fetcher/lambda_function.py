# Honestly could improve this code a lot on the syntax, but it works for now.

import os
import requests
import json
import boto3
import uuid
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
from datetime import datetime, timezone

ALERT_CAUSE = {
    1: "UNKNOWN_CAUSE", 2: "OTHER_CAUSE", 3: "TECHNICAL_PROBLEM",
    4: "STRIKE", 5: "DEMONSTRATION", 6: "ACCIDENT", 7: "HOLIDAY",
    8: "WEATHER", 9: "MAINTENANCE", 10: "CONSTRUCTION",
    11: "POLICE_ACTIVITY", 12: "MEDICAL_EMERGENCY"
}

ALERT_EFFECT = {
    1: "NO_SERVICE", 2: "REDUCED_SERVICE", 3: "SIGNIFICANT_DELAYS",
    4: "DETOUR", 5: "ADDITIONAL_SERVICE", 6: "MODIFIED_SERVICE",
    7: "OTHER_EFFECT", 8: "UNKNOWN_EFFECT", 9: "STOP_MOVED"
}

kinesis_client = boto3.client("kinesis")

def lambda_handler(event, context):
    URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"
    KINESIS_STREAM_NAME = os.environ.get('KINESIS_STREAM_NAME')

    response = requests.get(URL)

    if response.status_code != 200:
        print(f"Error fetching data: {response.status_code}")

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    records_to_send = []
    timestamp = datetime.now(timezone.utc).isoformat()

    for entity in feed.entity: # for each entity from the api call
        record_payload = None
        partition_key = None

        if entity.HasField('trip_update'):
            trip_update_dict = MessageToDict(entity.trip_update)
            record_payload = {
                'event_type': 'TRIP_UPDATE',
                'ingestion_timestamp': timestamp,
                'event_id': str(uuid.uuid4()),
                'data': trip_update_dict
            }
            partition_key = trip_update_dict.get('trip', {}).get('tripId')

        elif entity.HasField('vehicle'):
            vehicle_dict = MessageToDict(entity.vehicle)
            record_payload = {
                'event_type': 'VEHICLE_POSITION',
                'ingestion_timestamp': timestamp,
                'event_id': str(uuid.uuid4()),
                'data': vehicle_dict
            }
            partition_key = vehicle_dict.get('trip', {}).get('tripId')

        elif entity.HasField('alert'):
            alert_dict = MessageToDict(entity.alert)
            if not alert_dict.get('informedEntity'):
                continue
            if 'cause' in alert_dict:
                alert_dict['cause'] = ALERT_CAUSE.get(alert_dict['cause'], "UNKNOWN")
            if 'effect' in alert_dict:
                alert_dict['effect'] = ALERT_EFFECT.get(alert_dict['effect'], "UNKNOWN")
            record_payload = {
                'event_type': 'SERVICE_ALERT',
                'ingestion_timestamp': timestamp,
                'event_id': str(uuid.uuid4()),
                'data': alert_dict
            }
            partition_key = entity.id
        if record_payload and partition_key:
            records_to_send.append({
                'Data': json.dumps(record_payload),
                'PartitionKey': partition_key
            })
    if not records_to_send:
        print("No trips found")
        return {'statusCode': 204}

    print("Sending Data to Kinesis")
    try:
        for i in range(0, len(records_to_send), 500):
            batch = records_to_send[i:i + 500]
            kinesis_client.put_records(
                StreamName=KINESIS_STREAM_NAME,
                Records=batch
            )
        return {'statusCode': 200, 'body': json.dumps(f'Sent {len(records_to_send)} records')}
    except Exception as e:
        print(f"Error sending data to Kinesis: {e}")
        raise e