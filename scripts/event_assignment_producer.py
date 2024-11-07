import json
import os
import uuid
from datetime import datetime, timedelta
from dotenv import load_dotenv
from faker import Faker
from kafka import KafkaProducer
from pathlib import Path
from time import sleep


dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

KAFKA_HOST = os.getenv("KAFKA_HOST")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_NAME")

producer = KafkaProducer(bootstrap_servers=f"{KAFKA_HOST}:9092")
faker = Faker()


class DataGenerator(object):

    @staticmethod
    def get_data():
        """
        Generate data using faker

        Returns:
            list: list generater faker data
        """
        now = datetime.now()
        return [
            uuid.uuid4().__str__(),
            faker.random_int(min=1, max=100),
            faker.name(),
            faker.random_element(elements=("Chair", "Table", "Desk", "Desk", "Sofa", "Bed", "Vas", "Lamp", "Plate", "Spoon", "Fork")),
            faker.safe_color_name(),
            faker.random_int(min=100, max=150000),
            faker.unix_time(
                start_datetime=now - timedelta(minutes=60), end_datetime=now
            )
        ]
    

# Generate message using faker
while True:
    columns = [
        "order_id",
        "customer_id",
        "customer_name",
        "furniture",
        "color",
        "price",
        "timestamp"
    ]
    generated_data_list = DataGenerator.get_data()
    json_data = dict(zip(columns, generated_data_list))
    encoded_payload_data = json.dumps(json_data).encode("utf-8")
    print(encoded_payload_data, flush=True)
    print("=" * 5, flush=True)
    response = producer.send(topic=KAFKA_TOPIC, value=encoded_payload_data)
    print(response.get())
    print("=" * 20, flush=True)
    sleep(3)