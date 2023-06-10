import time
from kafka import KafkaProducer, KafkaConsumer
import uuid
import json

TOPIC_NAME = "demo"
SERVER = "kafka-demo-vincent-7a76.aivencloud.com:11525"

producer = KafkaProducer(
    bootstrap_servers=SERVER,
    security_protocol="SSL",
    ssl_cafile="ca.pem",
    ssl_certfile="service.cert",
    ssl_keyfile="service.key",
)

message = {
    'transactionType': 'PAYMENT',
    'amount': 17.5,
    'status': 'VALIDATED',
    'date': time.time()
}

producer.send(TOPIC_NAME, json.dumps(message).encode('utf-8'),
              json.dumps({'id': str(uuid.uuid4())}).encode('utf-8'))
print(f"Message sent: {message}")

producer.close()
