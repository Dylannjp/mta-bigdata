import csv
import boto3

STOPS_FILE = './gtfs_subway/stops.txt'
STOP_TIMES_FILE = './gtfs_subway/stop_times.txt'
STOPS_TABLE_NAME = 'mta-static-stops-table'
STOP_TIMES_TABLE_NAME = 'mta-static-stop-times-table'

dynamodb = boto3.resource('dynamodb')

stops_table = dynamodb.Table(STOPS_TABLE_NAME)
with open(STOPS_FILE, mode='r', encoding="utf-8") as infile:
    reader = csv.DictReader(infile)
    with stops_table.batch_writer() as batch:
        for row in reader:
            batch.put_item(
                Item={
                    'stop_id': row['stop_id'],
                    'stop_name': row['stop_name']
                }
            )

stop_times_table = dynamodb.Table(STOP_TIMES_TABLE_NAME)
with open(STOP_TIMES_FILE, mode='r', encoding='utf-8') as infile:
    reader = csv.DictReader(infile)
    with stop_times_table.batch_writer() as batch:
        # Print progress
        count = 0
        for row in reader:
            static_id = row['trip_id']
            realtime_id = None
            parts = static_id.split('_', 1)
            if len(parts) > 1:
                realtime_id = parts[1]
            else:
                continue
            batch.put_item(
                Item={
                    'trip_id': realtime_id,
                    'stop_id': row['stop_id'],
                    'stop_sequence': int(row['stop_sequence']),
                    'arrival_time': row['arrival_time'],
                    'departure_time': row['departure_time']
                }
            )
            count += 1
            if count % 100000 == 0:
                print(f"Processed {count} stop time records")

print("Data loading successful")