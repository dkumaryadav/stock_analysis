#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 28 18:08:25 2020
@author: deepakkumaryadav
"""
import os
from kafka import KafkaConsumer
from kafka import KafkaProducer

topic = "data-available"
top10Topic = "Top10"
bottom10Topic = "Bottom10"

success_key = b"SUCCESS"
destinationDir = "/user/hadoop/stocks/"

consumer = KafkaConsumer(topic, auto_offset_reset='latest',  group_id=None)
producer = KafkaProducer(bootstrap_servers="localhost:9092")

for message in consumer:
	if message.key == success_key:
		print("Data is present at: ",message.value)
        
        # Loading data from HDFS to HBase
        top10 = destinationDir+"top10/"+(message.value.replace("stockPrice","top10"))
        bottom10 = destinationDir+"bottom10/"+(message.value.replace("stockPrice","bottom10"))
        os.system("sh ingest-and-analyze-data.sh "+message.value+" "+top10+" "+bottom10)
        
        #Publis to top10 kafka topic
        producer.send(top10Topic, key=b"SUCCESS", value=b""+top10)
        
        #Publish to bottom10 kafka topic
        producer.send(bottom10Topic, key=b"SUCCESS", value=b""+bottom10)