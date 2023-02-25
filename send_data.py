import boto3
import json
from datetime import datetime
import read_sensors

# Connect to sqs queue
sqs = boto3.client('sqs', region_name='us-east-1')
queue_url = 'https://sqs.us-east-1.amazonaws.com/332120909569/IOTQueue.fifo'

while True:
    # Read data from sensors
    temperature, humidity = read_sensors(4)

    # Create message in JSON format
    message_body = {
        "temperature": temperature,
        "humidity": humidity,
        "timestamp": datetime.now().strftime("%y-%m-%d %H:%M:%S")
    }

    # Send message to the queue
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message_body),
        MessageGroupId='SensorsData'
    )

    print(f"Message sent. Message ID: {response['MessageId']}")
