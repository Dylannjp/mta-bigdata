import os
import json
from datetime import datetime, timezone, timedelta
import boto3
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource('dynamodb')
TABLE_NAME = os.environ.get('TRIPS_TABLE_NAME', 'mta-pipeline-trips-table')
table = dynamodb.Table(TABLE_NAME)

# Triggered by API Gateway, scans table and checks for delays in within a specific recent timeframe
def lambda_handler(event, context):

    print(f"Request received: {event}")
    time_window = datetime.now(timezone.utc) - timedelta(minutes=15)
    time_window_iso = time_window.isoformat()
    
    try:
        response = table.scan(
            FilterExpression=Attr('ingestion_timestamp').gt(time_window_iso) # Only trips within the past 15 minutes 
        )
        items = response.get('Items', [])
        
        delayed_trains = []
        for item in items:
            for prediction in item.get('stop_predictions', []):
                delay = prediction.get('calculated_delay_seconds')
                
                # Check if a significant delay exists for this stop prediction
                if delay is not None and delay > 300:
                    delayed_trains.append({
                        'route_id': item.get('route_id'),
                        'trip_id': item.get('trip_id'),
                        'stop_name': prediction.get('stop_name'),
                        'delay_minutes': round(delay / 60)
                    })

        # Create a summary payload for our API response
        summary = {
            'last_updated_utc': datetime.now(timezone.utc).isoformat(),
            'monitoring_window_minutes': 15,
            'total_delayed_trains_found': len(delayed_trains),
            'delayed_trains': delayed_trains
        }

        # Return an API Gateway response
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(summary)
        }

    except Exception as e:
        print(f"ERROR: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'An internal error occurred.'})
        }