import os
from kafka import KafkaConsumer
from kafka import KafkaProducer

topic = "data-available"

success_key = b"SUCCESS"

consumer = KafkaConsumer(topic, auto_offset_reset='latest',  group_id=None)

for message in consumer:
	if message.key == success_key:
		print("Data is present at: ",message.value)
        # Loading data from HDFS to HBase
        os.system("sh ingest-and-analyze-data.sh "+message.value)
        #producer.send(topic, dataIngested)
