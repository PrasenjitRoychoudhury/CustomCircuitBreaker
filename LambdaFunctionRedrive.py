import json
import urllib.request
import boto3
from botocore.exceptions import BotoCoreError, ClientError

# SQS Configuration
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/accountId/EventBridgeSigTermDLQ"  # Replace with your SQS URL
sqs_client = boto3.client('sqs', region_name='us-east-1')

# SaaS API Endpoint
API_URL = "http://my-load-balancer-saas-FIS2-accountId.us-east-1.elb.amazonaws.com/next-prime"  # Replace with your API URL

def lambda_handler(event, context):
    try:
        messages_processed = 0

        while True:
            # Receive up to 10 messages from SQS
            response = sqs_client.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=10,  # Process up to 10 messages at a time
                WaitTimeSeconds=10,  # Long polling
                VisibilityTimeout=30  # Time before message becomes visible again
            )

            # Check if messages exist
            if "Messages" not in response:
                print(f"All messages processed. Total messages handled: {messages_processed}")
                return {"statusCode": 200, "body": f"All messages processed: {messages_processed}"}

            for message in response["Messages"]:
                receipt_handle = message["ReceiptHandle"]
                body = json.loads(message["Body"])  # Extract message body
                random_number = body.get("input_number")  # Extract the number from the message

                if random_number is None:
                    print("Invalid message format. Deleting from queue.")
                    delete_message(receipt_handle)
                    continue

                print(f"Processing message with number: {random_number}")

                # Make API call to SaaS service
                api_response = call_saas_api(random_number)

                # If API call is successful, delete the message from SQS
                if api_response["statusCode"] == 200:
                    print(f"API call successful for {random_number}, deleting message from SQS.")
                    delete_message(receipt_handle)
                    messages_processed += 1
                else:
                    print(f"API call failed for {random_number}, keeping message in SQS.")

    except (BotoCoreError, ClientError) as e:
        print(f"Error processing SQS messages: {str(e)}")
        return {"statusCode": 500, "body": "Error processing SQS messages"}

def call_saas_api(random_number):
    """Calls the SaaS API and returns response"""
    try:
        full_url = f"{API_URL}?number={random_number}"

        with urllib.request.urlopen(full_url) as response:
            response_data = json.loads(response.read().decode('utf-8'))

            return {
                "statusCode": response.status,
                "body": json.dumps({
                    "input_number": random_number,
                    "saas_response": response_data  # SaaS API response
                })
            }

    except Exception as e:
        print(f"Failed to call SaaS API: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": "Failed to reach SaaS API"})}

def delete_message(receipt_handle):
    """Deletes the processed message from SQS"""
    try:
        sqs_client.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
        print("Message deleted from SQS.")
    except (BotoCoreError, ClientError) as e:
        print(f"Error deleting message from SQS: {str(e)}")
