import os
import json
import base64
import boto3
from datetime import datetime, time, timezone, timedelta
from pytz import timezone as py_tz

dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')
ALERTS_TABLE_NAME = os.environ.get('ALERTS_TABLE_NAME')
TRIPS_TABLE_NAME = os.environ.get('TRIPS_TABLE_NAME')
VEHICLE_TABLE_NAME = os.environ.get('VEHICLE_TABLE_NAME')
STOPS_TABLE_NAME = os.environ.get('STOPS_TABLE_NAME')
STOP_TIMES_TABLE_NAME = os.environ.get('STOP_TIMES_TABLE_NAME')

alerts_table = dynamodb.Table(ALERTS_TABLE_NAME)
trips_table = dynamodb.Table(TRIPS_TABLE_NAME)
vehicles_table = dynamodb.Table(VEHICLE_TABLE_NAME)
stops_table = dynamodb.Table(STOPS_TABLE_NAME)
stop_times_table = dynamodb.Table(STOP_TIMES_TABLE_NAME)

stop_name_cache = {}
schedule_cache = {}
DELAY_THRESHOLD_SECONDS = 300

# Get stop names from the stops table
def get_stop_name(stop_id):
    if stop_id in stop_name_cache:
        return stop_name_cache[stop_id]
    try: 
        response = stops_table.get_item(Key={'stop_id':stop_id})
        if 'Item' in response:
            stop_name = response['Item']['stop_name']
            stop_name_cache[stop_id] = stop_name
            return stop_name
        return None
    except Exception as e: 
        print(f"Error getting stop name for {stop_id}")
        return None

# Maps stop ids to its arrival times.
def get_schedule_map_for_trip(trip_id):
    if trip_id in schedule_cache:
        return schedule_cache[trip_id]

    try:
        response = stop_times_table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('trip_id').eq(trip_id)
        )
        items = response.get('Items', [])
        if items:
            schedule_map = {item['stop_id']: item['arrival_time'] for item in items}
            schedule_cache[trip_id] = schedule_map # Cache the result
            return schedule_map
        return None
    except Exception as e:
        print(f"Error querying schedule for trip {trip_id}: {str(e)}")
        return None

# Calculate delays based on the available schedule
def calculate_delay(predicted_unix_ts, scheduled_time_str, trip_start_date_str):
    if not all([predicted_unix_ts, scheduled_time_str, trip_start_date_str]):
        return None
        
    try:
        h, m, s = map(int, scheduled_time_str.split(':'))
        day_offset = 0
        if h >= 24:
            h -= 24
            day_offset = 1

        scheduled_time_obj = time(h, m, s)
        trip_date = datetime.strptime(trip_start_date_str, '%Y%m%d').date()
        scheduled_dt_naive = datetime.combine(trip_date, scheduled_time_obj) + timedelta(days=day_offset)

        ny_tz = py_tz('America/New_York')
        scheduled_dt_aware = ny_tz.localize(scheduled_dt_naive)
        predicted_dt_utc = datetime.fromtimestamp(int(predicted_unix_ts), tz=timezone.utc)
        
        delay_timedelta = predicted_dt_utc - scheduled_dt_aware
        return int(delay_timedelta.total_seconds())
        
    except Exception as e:
        print(f"Error calculating delay for {predicted_unix_ts} vs {scheduled_time_str}: {str(e)}")
        return None

def lambda_handler(event, context):
    trip_updates_to_process = []
    trip_updates_to_write = []
    vehicle_positions_map = {}
    alerts_to_write = []

    # For every entry in the event, group them into trip_update, vehicle_position, or service_alert and process them.
    for record in event['Records']:
        try:
            payload_decoded = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            event_payload = json.loads(payload_decoded)
            event_type = event_payload.get('event_type')

            if event_type == 'TRIP_UPDATE':
                trip_updates_to_process.append(event_payload)
            elif event_type == 'VEHICLE_POSITION':
                vehicle_data = event_payload.get('data', {})
                trip_id = vehicle_data.get('trip', {}).get('tripId')
                if trip_id:
                    vehicle_positions_map[trip_id] = vehicle_data
            elif event_type == 'SERVICE_ALERT':
                alert_data = event_payload.get('data', {})
                if alert_data:
                    item_to_save = {'ingestion_timestamp': event_payload.get('ingestion_timestamp'), 'event_id': event_payload.get('event_id')}
                    item_to_save.update(alert_data)
                    alerts_to_write.append(item_to_save)
        except Exception as e:
            print(f"Error decoding/sorting record: {e}")

    for trip_event in trip_updates_to_process:
        trip_update = trip_event.get('data', {})
        trip_id = trip_update.get('trip', {}).get('tripId')
        route_id = trip_update.get('trip', {}).get('routeId')
        trip_start_date = trip_update.get('trip', {}).get('startDate', '').replace('-', '')

        if not all([trip_id, trip_start_date]): continue

        schedule_map = get_schedule_map_for_trip(trip_id)
        
        modified_stop_updates = []
        for stop_update in trip_update.get('stopTimeUpdate', []):
            stop_id = stop_update.get('stopId')
            stop_name = get_stop_name(stop_id)
            predicted_arrival_unix = stop_update.get('arrival', {}).get('time')
            scheduled_arrival_str = schedule_map.get(stop_id) if schedule_map else None
            
            calculated_delay_seconds = calculate_delay(
                predicted_arrival_unix, scheduled_arrival_str, trip_start_date
            )
            alert_sent_for_this_trip = False
            if not alert_sent_for_this_trip and calculated_delay_seconds is not None and calculated_delay_seconds > DELAY_THRESHOLD_SECONDS:
                message = (
                    f"MTA Delay Alert:\n"
                    f"Route: {route_id}\n"
                    f"Train with Trip ID: {trip_id}\n"
                    f"Is now predicted to be {round(calculated_delay_seconds / 60)} minutes late "
                    f"for station: {stop_name}."
                )
                
                # Publish the message to the SNS topic
                try:
                    sns.publish(
                        TopicArn=SNS_TOPIC_ARN,
                        Message=message,
                        Subject=f"MTA Delay Alert: Route {route_id}"
                    )
                    alert_sent_for_this_trip = True
                    print(f"Successfully published delay alert for trip {trip_id}.")
                except Exception as e:
                    print(f"ERROR: Failed to publish to SNS: {str(e)}")
            
            modified_stop_updates.append({
                'stop_id': stop_id,
                'stop_name': stop_name,
                'predicted_arrival_time': predicted_arrival_unix,
                'scheduled_arrival_time': scheduled_arrival_str,
                'calculated_delay_seconds': calculated_delay_seconds
            })

        final_event = {
            'event_id': trip_event['event_id'],
            'ingestion_timestamp': trip_event['ingestion_timestamp'],
            'trip_id': trip_id,
            'route_id': route_id,
            'stop_predictions': modified_stop_updates,
            'live_position': vehicle_positions_map.get(trip_id) 
        }
        trip_updates_to_write.append(final_event)

    # Batch Write to all three tables
    if trip_updates_to_write:
        with trips_table.batch_writer() as batch:
            for item in trip_updates_to_write: batch.put_item(Item=item)
    if vehicle_positions_map:
        with vehicles_table.batch_writer() as batch:
            for trip_id, vehicle_data in vehicle_positions_map.items():
                item = {
                    'trip_id': trip_id,
                    'position_timestamp': int(vehicle_data.get('timestamp')),
                    'route_id': vehicle_data.get('trip', {}).get('routeId'),
                    'current_status': vehicle_data.get('currentStatus', 'IN_TRANSIT_TO'),
                    'current_stop_sequence': vehicle_data.get('currentStopSequence'),
                    'stop_id': vehicle_data.get('stopId')
                }
                batch.put_item(Item=item)
    if alerts_to_write:
        with alerts_table.batch_writer() as batch:
            for item in alerts_to_write: batch.put_item(Item=item)
