import random
from kafka import KafkaProducer
import json
import time


# initializing the Variables
topic_name = "inputTopic3"



# Creating the Producer

producer = KafkaProducer(
    bootstrap_servers=f"kafka-16de8312-searce-c25e.aivencloud.com:13011",
    security_protocol="SSL",
    ssl_cafile="ca.pem",
    ssl_certfile="service.cert",
    ssl_keyfile="service.key",
)

i=0


while i<1000000:
  ans = i%2
  message = {'Key' : '{}'.format(ans), 'Value': 'Inserted Num is {}'.format(i)}
  print(message)
  # Sending Messages to specific partion of a kafka topic
  producer.send(topic_name,json.dumps(message['Value']).encode('utf-8'),partition=int(message['Key']))
  print("Message sent")
  i=i+1
  time.sleep(1)