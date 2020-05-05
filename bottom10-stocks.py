from kafka import KafkaConsumer

topic = "bottom10"
consumer = KafkaConsumer(topic)

for message in consumer:
    print(message)