from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
import json

consumer = KafkaConsumer(
    "product_view",
    bootstrap_servers="46.202.167.130:9094,46.202.167.130:9194,46.202.167.130:9294",
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username="kafka",
    sasl_plain_password="UnigapKafka@2024",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)
print("done consumer")

producer =  KafkaProducer(
    bootstrap_servers="localhost:9094",
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username="kafka",
    sasl_plain_password="admin",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"))

print("done producer")
for message in consumer:
    event = message.value
    producer.send(topic="product_views", value=event)
    print(f"Inserted: {event}")

# mongo_client = MongoClient('mongodb://localhost:27017/')
# db = mongo_client['glamira']
# collection = db['events']

# for msg in consumer:
#     event = msg.value
#     collection.insert_one(event)
#     print(f"Inserted: {event}")


