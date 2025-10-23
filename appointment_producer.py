import json
import os
import time
import random
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.admin import KafkaAdminClient, NewTopic
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to ensure topic exists
def ensure_topic_exists(bootstrap_servers, topic_name, security_protocol, sasl_mechanism, sasl_plain_username, sasl_plain_password):
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers.split(','),
            security_protocol=security_protocol,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password
        )

        topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        logging.info(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        logging.warning(f"Topic '{topic_name}' might already exist or failed to create: {e}")
    finally:
        if 'admin_client' in locals():
            admin_client.close()


# Pre-generate 5 doctors from different departments
DEPARTMENTS = ["Cardiology", "Neurology", "Orthopedics", "Oncology", "Pediatrics"]
DOCTORS = []
for dept in DEPARTMENTS:
    DOCTORS.append({
        "doctor_id": str(uuid.uuid4()),
        "name": f"Dr. {uuid.uuid4().hex[:8]}",
        "department": dept
    })



def generate_appointment_data(event_type):
    now = datetime.now()
    patient_id = str(uuid.uuid4())
    appointment_id = str(uuid.uuid4())
    # randomly select a pre-generated doctor
    doctor = random.choice(DOCTORS)

    if event_type == "appointment_created":
        appointment_time = now + timedelta(days=random.randint(1, 30))
        data = {
            "event_type": "appointment_created",
            "appointment_id": appointment_id,
            "patient_id": patient_id,
            "doctor_id": doctor["doctor_id"],
            "doctor_name": doctor["name"],
            "scheduled_time": appointment_time.isoformat(),
            "department": doctor["department"],
            "reason": "Regular checkup",
            "status": "scheduled"
        }
    elif event_type == "appointment_cancelled":
        data = {
            "event_type": "appointment_cancelled",
            "appointment_id": appointment_id,
            "patient_id": patient_id,
            "cancellation_time": now.isoformat(),
            "reason": "Patient cancelled",
            "status": "cancelled"
        }
    else:
        raise ValueError("Invalid event type")

    return data


def delivery_report(err, msg):
    if err is not None:
        logging.error(f'Message delivery failed: {err}')
    else:
        logging.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def create_kafka_producer(bootstrap_servers, security_protocol, sasl_mechanism, sasl_plain_username, sasl_plain_password):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(','),
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_plain_username=sasl_plain_username,
        sasl_plain_password=sasl_plain_password,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(0, 11, 5)  # Specify the API version
    )
    return producer

def main():
    bootstrap_servers = os.environ.get("BOOTSTRAP_SERVERS")
    security_protocol = os.environ.get("SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    sasl_mechanism = os.environ.get("SASL_MECHANISM", "SCRAM-SHA-512")
    sasl_plain_username = os.environ.get("SASL_USERNAME")
    sasl_plain_password = os.environ.get("SASL_PASSWORD")
    output_topic = os.environ.get("OUTPUT_TOPIC")
    interval_ms = int(os.environ.get("INTERVAL_MS", "1000"))
    dlt_topic = os.environ.get("DLT_TOPIC", "appointment_dlt")  # Dead-letter topic

    if not all([bootstrap_servers, sasl_plain_username, sasl_plain_password, output_topic]):
        if not bootstrap_servers:
            logging.error("BOOTSTRAP_SERVERS environment variable is not set.")
        if not sasl_plain_username:
            logging.error("SASL_USERNAME environment variable is not set.")
        if not sasl_plain_password:
            logging.error("SASL_PASSWORD environment variable is not set.")     
        if not output_topic:
            logging.error("OUTPUT_TOPIC environment variable is not set.")  
        logging.error("Missing required environment variables. Please set BOOTSTRAP_SERVERS, SASL_USERNAME, SASL_PASSWORD, and OUTPUT_TOPIC.")
        return

    # Ensure the output topic exists
    ensure_topic_exists(bootstrap_servers, output_topic, security_protocol, sasl_mechanism, sasl_plain_username, sasl_plain_password)

    # Ensure the dead-letter topic exists
    ensure_topic_exists(bootstrap_servers, dlt_topic, security_protocol, sasl_mechanism, sasl_plain_username, sasl_plain_password)


    producer = create_kafka_producer(bootstrap_servers, security_protocol, sasl_mechanism, sasl_plain_username, sasl_plain_password)

    event_types = ["appointment_created", "appointment_cancelled"]

    try:
        while True:
            event_type = random.choice(event_types)
            data = generate_appointment_data(event_type)

            try:
                producer.send(output_topic, value=data).add_callback(delivery_report).add_errback(lambda err: logging.error(f"Failed to send message: {err}"))
                producer.flush() # Ensure message is sent immediately
                logging.info(f"Sent event: {data}")
            except KafkaError as e:
                logging.error(f"Failed to send message to main topic: {e}.  Sending to DLT.")
                try:
                     producer.send(dlt_topic, value=data).add_callback(delivery_report).add_errback(lambda err: logging.error(f"Failed to send message to DLT: {err}"))
                     producer.flush()
                except KafkaError as dlt_e:
                    logging.error(f"Failed to send message to DLT: {dlt_e}. Message lost.")


            time.sleep(interval_ms / 1000)

    except KeyboardInterrupt:
        logging.info("Shutting down producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
