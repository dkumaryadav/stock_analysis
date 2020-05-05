import os
from kafka import KafkaConsumer

topic = "data-available"
dataPersistTopic = "data-persisted"

success_key = b"SUCCESS"

consumer = KafkaConsumer(topic)
producer = KafkaProducer(bootstrap_servers="localhost:9092")

for message in consumer:
	if message.key == success_key:
		print("Data is present at: ",message.value)
        # Loading data from HDFS to HBase
        os.system("sh ingest-and-analyze-data.sh "+message.value)
        #producer.send(topic, dataIngested)
