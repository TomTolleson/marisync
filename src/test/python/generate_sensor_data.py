from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_sensor_reading():
    return {
        "timestamp": datetime.now().isoformat(),
        "location": f"sensor_{random.randint(1,5)}",
        "co2_level": random.uniform(350, 500),
        "temperature": random.uniform(15, 30),
        "humidity": random.uniform(30, 70),
        "particulate_matter": random.uniform(0, 50)
    }

def send_sensor_data():
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        logger.info("Connected to Kafka")

        while True:
            try:
                reading = create_sensor_reading()
                producer.send('environmental-sensors', reading)
                producer.flush()  # Force the message to be sent
                logger.info(f"Sent reading: {reading}")
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error sending message: {e}")
                time.sleep(1)

    except Exception as e:
        logger.error(f"Error connecting to Kafka: {e}")
    finally:
        if 'producer' in locals():
            producer.close()

if __name__ == "__main__":
    send_sensor_data() 