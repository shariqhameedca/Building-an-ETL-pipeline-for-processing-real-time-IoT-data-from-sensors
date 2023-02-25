import boto3
import json
import time
import mysql.connector
from mysql.connector import Error
import time
import connect_to_db

def extract(sensor_data_dict):
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['All'],
        MessageAttributeNames=['All'],
        MaxNumberOfMessages=10,
        VisibilityTimeout=20,
        WaitTimeSeconds=20
    )

    # Check if any messages were received
    if 'Messages' in response:
        # Iterate over the messages
        for message in response['Messages']:
            # Parse the message body
            message_body = json.loads(message['Body'])
            message_id = message['MessageId']
            temperature = message_body["temperature"]
            humidity = message_body["humidity"]
            timestamp = message_body["timestamp"]

            sensor_data["MessageId"].append(message_id)
            sensor_data["temperature"].append(temperature)
            sensor_data["humidity"].append(humidity)
            sensor_data["timestamp"].append(timestamp)
            
            print("extracted", temperature, humidity, timestamp)
            
            
def transform(data_dict):
    record = {}
    for sensor_name, sensor_readings in data_dict.items():
        if sensor_name not in ["MessageId", "timestamp"]:
            avg_reading = sum(sensor_readings)/len(sensor_readings)
            record[sensor_name] = avg_reading
    
    record["MessageId"] = data_dict["MessageId"][-1]
    record["timestamp"] = data_dict["timestamp"][-1]
    
    print("transformed to get", record)
    return record


def load(record, cur):
    data_ = (record["MessageId"], record["temperature"], record["humidity"], record["timestamp"])
    cur.execute("INSERT INTO DATA VALUES (%s, %s, %s, %s)", data_)
    connection.commit()
    print("stored", data_)
    
    
connection, curconnect_to_db("localhost", "test_project", "root", "")
sqs = boto3.client('sqs', region_name='us-east-1')
queue_url = 'https://sqs.us-east-1.amazonaws.com/332120909569/IOTQueue.fifo'
sensor_data = {"MessageId":[], "temperature":[], "humidity":[], "timestamp":[]}

# Set up the start time
start_time = time.time()

# Set up the time interval for data aggregation
interval = 60  # 1 minute

while True:
    # Receive messages from the queue
    extract(sensor_data)
    current_time = time.time()
    elapsed_time = current_time - start_time
    if elapsed_time >= interval:
        record = transform(sensor_data)
        load(record, cur)
        start_time = current_time