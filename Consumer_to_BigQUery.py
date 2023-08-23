from kafka import KafkaConsumer
import json
from google.cloud import bigquery
import sys
import os


# initializing the variable
topic_name = "realtime"
table_id = ""


# Auth Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service account json file"
# initializing the bigquery client object
client = bigquery.Client()

# Creating the Kafka Consumer
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

# Reading messages from kafka topic
for msg in consumer:
    print("Message: ", msg.value)
    temp = msg.value
    flag = temp.decode()
    output = json.loads(flag)
    row_to_insert = [output]
    # Inserting it into Bigquery
    error = client.insert_rows_json(table_id, row_to_insert)
    if (error == []):
        print(output)
        print("Messeage inserted into BigQuery")
    else:
        print("error", error)





