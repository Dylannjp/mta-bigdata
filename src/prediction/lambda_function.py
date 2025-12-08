import os
import json
import boto3
from datetime import datetime

sagemaker_runtime = boto3.client('sagemaker-runtime')
SAGEMAKER_ENDPOINT_NAME = os.environ.get('SAGEMAKER_ENDPOINT_NAME')

# Load station name mapping before we run the actual lambda handler
try:
    with open('feature_mappings.json', 'r') as f:
        mappings = json.load(f)
        FEATURE_MAPS = {
            'start_station': mappings['station_map'],
            'end_station': mappings['station_map'],
            'route_id': mappings['route_map']
        }
    print("Successfully loaded feature mappings.")
except Exception as e:
    print(f"CRITICAL: Could not load feature_mappings.json. Error: {str(e)}")
    FEATURE_MAPS = None

# Takes in input (Station ID) and outputs a prediction of trip length
def lambda_handler(event, context):
    if not FEATURE_MAPS:
        return {'statusCode': 500, 'body': json.dumps({'error': 'Feature mappings are not loaded.'})}
        
    try:
        body = json.loads(event.get('body', '{}'))
        
        start_station = body.get('start_station')
        end_station = body.get('end_station')
        route_id = body.get('route_id')
        
        now = datetime.now() # Not really how you're supposed to do this, but it works fine for now.
        hour_of_day = now.hour
        day_of_week = now.weekday()
        
        if not all([start_station, end_station, route_id]):
            return {
                'statusCode': 400,
                'body': json.dumps({'error': "Request body must contain 'start_station', 'end_station', and 'route_id'."})
            }
            
        # Translate user input to numerical features for the model
        try:
            start_station_code = FEATURE_MAPS['start_station'][start_station]
            end_station_code = FEATURE_MAPS['end_station'][end_station]
            route_id_code = FEATURE_MAPS['route_id'][route_id]
        except KeyError as e:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': f"Invalid input. Could not find mapping for: {str(e)}"})
            }
            
        features = [
            start_station_code,
            end_station_code,
            route_id_code,
            hour_of_day,
            day_of_week
        ]
        
        # Format payload and invoke SageMaker endpoint
        payload = ','.join(map(str, features))
        response = sagemaker_runtime.invoke_endpoint(
            EndpointName=SAGEMAKER_ENDPOINT_NAME,
            ContentType='text/csv',
            Body=payload
        )
        
        # Decode the prediction and return it
        result = response['Body'].read().decode('utf-8')
        predicted_seconds = float(result.strip())
        
        api_response = {
            'inputs': {
                'start_station': start_station,
                'end_station': end_station,
                'route_id': route_id
            },
            'predicted_travel_time_seconds': round(predicted_seconds, 2)
        }
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps(api_response)
        }

    except Exception as e:
        print(f"ERROR: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': 'An internal error occurred.'})}