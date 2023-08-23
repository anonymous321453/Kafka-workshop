import random
from kafka import KafkaProducer
import json
import time


# Reading config file for connection parameters

topic_name="inputTopic4"

# Creating Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=f"kafka-16de8312-searce-c25e.aivencloud.com:13011",
    security_protocol="SSL",
    ssl_cafile="ca.pem",
    ssl_certfile="service.cert",
    ssl_keyfile="service.key",
)

i = 0

Order_Type = ["Pre_Paid","COD"]
pizza_names = [
            "Margherita",
            "Marinara",
            "Diavola",
            "Mari & Monti",
            "Salami",
            "Peperoni",
        ]
toppings = [
            "tomato",
            "blue cheese",
            "egg",
            "green peppers",
            "hot pepper",
            "bacon",
            "olives",
            "garlic",
            "tuna",
            "onion",
            "pineapple",
            "strawberry",
            "banana",
        ]

#  Logic for Generating messages
while (i<10000000) :
    print(i)
    message = { 'Order_id' : '{}'.format(i),'Order_Type' : '{}'.format(random.choice(Order_Type)),'pizza_name' : '{}'.format(random.choice(pizza_names)),'toppings' : '{}'.format(random.choice(toppings)),'Order_Size' : '{}'.format(random.randint(1,4))}
    Message = json.dumps(message)
    msg= Message.encode("utf-8")
    # Sending messages to kafka topic
    producer.send(topic_name ,value = msg )
    print(f"Message sent: {Message}")
    i=i+1
    time.sleep(1)