from flask import Flask, request, jsonify
import math
import logging
import json
import urllib.request
import random
import time
from datetime import datetime, timedelta
import threading
import boto3
from botocore.exceptions import BotoCoreError, ClientError

# Configure logging to print to stdout (so that Docker picks it up)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

app = Flask(__name__)

# Circuit Breaker States
CIRCUIT_CLOSED = 'closed'
CIRCUIT_HALF_OPEN = 'half_open'
CIRCUIT_OPEN = 'open'

# Initialize SNS client
sns_client = boto3.client('sns', region_name='us-east-1')

SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:accountId:CircuitBreakerClosedCircuit'

# Initialize Circuit Breaker variables
circuit_state = CIRCUIT_CLOSED
failure_timestamps = []
last_failure_time = None

# SaaS API URL
API_URL = "http://my-load-balancer-saas-FIS2-accountId.us-east-1.elb.amazonaws.com"

# Initialize SQS client
sqs_client = boto3.client('sqs', region_name='us-east-1')
SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/accountId/EventBridgeSigTermDLQ'

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify(status="healthy"), 200

def is_prime(n):
    if n <= 1:
        return False
    if n <= 3:
        return True
    if n % 2 == 0 or n % 3 == 0:
        return False
    i = 5
    while i * i <= n:
        if n % i == 0 or n % (i + 2) == 0:
            return False
        i += 6
    return True

def next_prime(n):
    candidate = n + 1
    while not is_prime(candidate):
        candidate += 1
    return candidate

def send_to_sqs(message_body):
    try:
        response = sqs_client.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(message_body)
        )
        logger.info(f"Message sent to SQS: {response.get('MessageId')}")
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Failed to send message to SQS {str(e)}")

def update_circuit_state():
    global circuit_state, failure_timestamps, last_failure_time
    current_time = datetime.now()

    old_circuit_state = circuit_state
    # Remove failures older than 1 minute
    failure_timestamps = [ts for ts in failure_timestamps if current_time - ts < timedelta(minutes=1)]

    if circuit_state != CIRCUIT_OPEN:
        if len(failure_timestamps) >= 4:
            circuit_state = CIRCUIT_OPEN
            last_failure_time = current_time
        elif len(failure_timestamps) >= 2 and (current_time - failure_timestamps[-2]) < timedelta(seconds=30):
            circuit_state = CIRCUIT_HALF_OPEN
        else:
            circuit_state = CIRCUIT_CLOSED

    if old_circuit_state != circuit_state:
        logger.info(f"Circuit state updated to {circuit_state}")
        logger.info(f"old_circuit_state is  {old_circuit_state}")

    if old_circuit_state != circuit_state:
        logger.info(f"Debug 1 ")

        if old_circuit_state in [CIRCUIT_OPEN, CIRCUIT_HALF_OPEN] and circuit_state == CIRCUIT_CLOSED:
            logger.info(f"Debug 2 ")
            logger.info("Circuit transitioned back to CLOSED. Triggering SNS notification.")
            publish_sns_message()

def publish_sns_message():
    try:
        response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps({'event': 'CircuitClosed', 'timestamp': datetime.now().isoformat()}),
            Subject='Circuit Closed Notification'
        )
        logger.info(f"SNS message published: {response['MessageId']}")
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Failed to publish SNS message: {str(e)}")


def saas_call(random_number):
    global circuit_state, failure_timestamps, last_failure_time

    update_circuit_state()

    if circuit_state == CIRCUIT_OPEN:
        logger.info("Circuit is OPEN. Skipping SaaS call.")
        send_to_sqs({
            "error": "Circuit is open. SaaS call skipped.",
            "timestamp": datetime.now().isoformat(),
            "circuit_state": circuit_state,
            "input_number": random_number
        })
        return {
            "statusCode": 503,
            "body": json.dumps({"error": "Circuit is open. SaaS call skipped."})
        }

    if circuit_state == CIRCUIT_HALF_OPEN and random.choice([True, False]) == False:
        logger.info("Circuit is HALF-OPEN. Skipping this SaaS call as part of 50% logic.")
        send_to_sqs({
            "error": "Circuit is half-open. SaaS call skipped for this request.",
            "timestamp": datetime.now().isoformat(),
            "circuit_state": circuit_state,
            "input_number": random_number
        })
        return {
            "statusCode": 503,
            "body": json.dumps({"error": "Circuit is half-open. SaaS call skipped for this request."})
        }

    # Construct the API URL
    api_url = f"{API_URL}/upload-number"

    # Create JSON payload
    payload = json.dumps({"number": random_number}).encode("utf-8")

    # Define headers
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    # Retry logic with exponential backoff
    retry_delays = [3, 5, 8]  # Exponential backoff intervals in seconds

    for attempt, delay in enumerate(retry_delays + [None], start=1):
        try:
            logger.info(f"Attempt {attempt}: Calling SaaS API with number {random_number} using POST")

            # Create the POST request
            request = urllib.request.Request(api_url, data=payload, headers=headers, method="POST")

            # Send request and handle response
            with urllib.request.urlopen(request, timeout=10) as response:
                if response.status == 200:
                    data = json.loads(response.read().decode("utf-8"))
                    next_prime_number = data.get("next_prime")
                    logger.info("SaaS call successful. Resetting failure count.")
                    failure_timestamps.clear()  # Reset failures on success
                    circuit_state = CIRCUIT_CLOSED  # Close the circuit after a successful call
                    update_circuit_state()
                    return {
                        "statusCode": 200,
                        "body": json.dumps({
                            "input_number": random_number,
                            "next_prime": next_prime_number
                        })
                    }
                else:
                    logger.error(f"API returned an error: {response.status}")
                    raise Exception(f"Error from API: {response.read().decode('utf-8')}")

        except Exception as e:
            logger.error(f"Attempt {attempt}: Failed to reach API. Error: {str(e)}")
            failure_timestamps.append(datetime.now())
            update_circuit_state()
            send_to_sqs({
                "error": "Failed to reach SaaS API.",
                "attempt": attempt,
                "input_number": random_number,
                "timestamp": datetime.now().isoformat(),
                "circuit_state": circuit_state
            })

            if delay is not None:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error("Max retries reached for this service call. Returning error.")
                return {
                    "statusCode": 500,
                    "body": json.dumps({"error": "Failed to reach SaaS API after multiple retries."})
                }

def periodic_health_check():
    global circuit_state  # Ensure we're accessing the global variable
    while True:
        time.sleep(120)  # Wait for 2 minutes
        if circuit_state != CIRCUIT_CLOSED:
            try:
                logger.info("Performing periodic health check on SaaS API.")
                with urllib.request.urlopen(f"{API_URL}/health", timeout=10) as response:
                    if response.status == 200:
                        circuit_state = CIRCUIT_CLOSED  # Close the circuit on success
                        publish_sns_message()
                        logger.info(f"Circuit state updated to {circuit_state}")
                        logger.info("SaaS API health check successful.")
                    else:
                        logger.warning(f"SaaS API health check returned non-200 status: {response.status}")
            except Exception as e:
                logger.error(f"Periodic SaaS health check failed: {str(e)}")

# Start the periodic health check in a separate thread
health_check_thread = threading.Thread(target=periodic_health_check, daemon=True)
health_check_thread.start()

@app.route('/next-prime', methods=['GET'])
def get_next_prime():
    try:
        number = int(request.args.get('number', 0))
        next_prime_number = next_prime(number)
        logger.info(f"Received API request for next prime of {number}")

        logger.info(f"Calling SaaS")
        saas_response = saas_call(number)
        logger.info(f"Returned from SaaS with response: {saas_response}")

        return jsonify({"next_prime": next_prime_number, "saas_response": saas_response}), 200
    except ValueError:
        logger.error("Invalid input. Please provide an integer.")
        return jsonify({"error": "Invalid input. Please provide an integer."}), 400

if __name__ == "__main__":
    logger.info("Starting Flask application")
    logger.info(f"Circuit state updated to Closed") 
    app.run(host='0.0.0.0', port=80)