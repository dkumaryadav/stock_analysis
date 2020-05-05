#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 03 11:18:27 2020

@author: deepakkumaryadav
"""
from kafka import KafkaConsumer
import os

topic = "bottom10"
success_key = b"SUCCESS"
destinationDir = "/user/hadoop/stocks/"

consumer = KafkaConsumer(topic, auto_offset_reset='latest',  group_id=None)

for message in consumer:
    if message.key == success_key:
        print("="*50)
        print("\t\t\t\t\tBottom 10 stocks")
        print("="*50)
        os.system("hadoop fs -cat "+ destinationDir+"bottom10/"+message.value)       		