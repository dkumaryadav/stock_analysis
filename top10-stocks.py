from kafka import KafkaConsumer

topic = "top10"
consumer = KafkaConsumer(topic)

for message in consumer:
    print(message)