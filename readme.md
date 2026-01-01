A project for CSP554 - Big Data Technologies.

The goal of the project is to demonstrate usage and understanding of tools that could process a very large amount of data effectively.

For the data we utilized the data from the New York MTA Transit system.

The data is gathered by:
1. Polling the API link every 1 minute
2. Categorizing the data into Trip Updates, Vehicles, and Alerts
3. Calculating extra features and modifying features for readability
4. Saves the data into DynamoDB

There are multiple features that we made with the data that we gathered:
1. Calculating delays of the trains in each stop and adding that into the data that we gathered from the API
2. An alert system that sends an email whenever a delay of x minutes happens to a stop.
3. A model that predicts when a train would arrive based on past trips.
4. A dashboard that displays the information of the trips.

Diagram of the tools that are used:
<img width="1033" height="661" alt="image" src="https://github.com/user-attachments/assets/ef113168-4073-4acd-b588-0ee585946052" />

A couple graphs we made with the data we gathered:
<img width="1226" height="867" alt="image" src="https://github.com/user-attachments/assets/3496a70c-e181-40a0-b7aa-60976b3bcc48" />
<img width="1008" height="636" alt="image" src="https://github.com/user-attachments/assets/c66ecd0e-96e0-4dcb-a666-49d5fc8e6cba" />
<img width="1184" height="684" alt="image" src="https://github.com/user-attachments/assets/e18f6ed6-c770-4a73-a812-1797b2969962" />
<img width="1008" height="636" alt="image" src="https://github.com/user-attachments/assets/b7fe6111-cf2b-4d68-98eb-11e02844de14" />
<img width="1184" height="684" alt="image" src="https://github.com/user-attachments/assets/d11bdc4c-7fe4-452a-a8c7-fb885aa611c0" />
