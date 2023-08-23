from kafka import KafkaConsumer
import json
from kafka import TopicPartition


# Initializing the Variables
topic_name = "inputTopic3"

# Creating a consumer
consumer = KafkaConsumer(
    auto_offset_reset="earliest",
    bootstrap_servers= f"kafka-16de8312-searce-c25e.aivencloud.com:13011",
    group_id = "sample-group",
    security_protocol="SSL",
    ssl_cafile="ca.pem",
    ssl_certfile="service.cert",
    ssl_keyfile="service.key",
)

consumer.assign([TopicPartition("inputTopic3",partition=0),TopicPartition("inputTopic1",partition=1)])


#Reading and displaying the messages in Consumer
for msg in consumer:
    Message = msg.value.decode("UTF-8")
    print("Message: ", Message)

