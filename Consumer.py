from kafka import KafkaConsumer
import json



# Initializing the Variables
topic_name = "inputTopic1"

# Creating a consumer
consumer = KafkaConsumer(
    topic_name,
    auto_offset_reset="earliest",
    bootstrap_servers= f"kafka-16de8312-searce-c25e.aivencloud.com:13011",
    group_id = "sample-group",
    security_protocol="SSL",
    ssl_cafile="ca.pem",
    ssl_certfile="service.cert",
    ssl_keyfile="service.key",
)
i=0
#Reading and displaying the messages in Consumer
for msg in consumer:
    i=i+1
    Message = msg.value.decode("UTF-8")
    print("Message: ", Message)


