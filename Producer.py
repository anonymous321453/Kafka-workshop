import random
from kafka import KafkaProducer
import json
import time


# initializing the Variables
topic_name = "inputTopic1"



# Creating the Producer

producer = KafkaProducer(
    bootstrap_servers=f"kafka-16de8312-searce-c25e.aivencloud.com:13011",
    security_protocol="SSL",
    ssl_cafile="ca.pem",
    ssl_certfile="service.cert",
    ssl_keyfile="service.key",
)
Message = ["My name is Deepak","My last name is Mishra", " Deepak Love Kafka", "Kafka is distributed Open Source event streaming platform", " Kafka can be used for buikding real time streaming pipeline"]
# Sending Messages to kafka topic
while True:
    message = random.choice(Message)
    Msg = json.dumps(message)
    msg = Msg.encode("utf-8")
    producer.send(topic_name, value=msg)
    print(f"Message sent: {message}")
    time.sleep(1)