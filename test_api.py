import requests
import json

url = "https://mg7rmmfjs6.execute-api.us-east-2.amazonaws.com/predict-travel-time"

payload = {
    "start_station": "633N", 
    "end_station": "631N", 
    "route_id": "6"
}

headers = {
    "Content-Type": "application/json"
}

try:
    response = requests.post(url, json=payload, headers=headers)
    print(f"Status Code: {response.status_code}")
    print("Response Body:")
    print(json.dumps(response.json(), indent=2))
except Exception as e:
    print(f"Error: {e}")