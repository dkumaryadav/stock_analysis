#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 13 15:08:15 2020

@author: deepakkumaryadav
"""
from iexfinance.stocks import Stock
from iexfinance.refdata import get_symbols
import pandas as pds
import time
from datetime import datetime
import os
from kafka import KafkaProducer
import sys
reload(sys)
sys.setdefaultencoding('utf8')
###############################################################################
#               
#               API TOKEN FROM https://iexcloud.io/console
#
###############################################################################

API_KEY = "sk_90b2bbb101954dbc8672a90d1349566b"
destinationFile = "stockPrice_"
destinationDir = "/user/hadoop/stocks/"
requestSize = 100
startIndex = 0
endIndex = requestSize
topic = "data-available"
producer = KafkaProducer(bootstrap_servers="localhost:9092")
stockPriceDF = pds.DataFrame(columns=["StockSymbol","StockName","StockPrice", "Change", "ChangePercentage", "52WHigh", "52WLow", "Time"])

# Get available stock symbols
startTime = time.time()
availableSymbols = get_symbols(output_format='pandas', token=API_KEY)
availableSymbols = availableSymbols[ availableSymbols["isEnabled"] == True ]
print("\nLoading of stock symbols COMPLTED in: ", (time.time()- startTime), "seconds")

# Divide them in batches as API can handle max 100 stock symboles / request in one go
deltaBatches = len(availableSymbols)%requestSize
if deltaBatches > 0:
    totalBatches = (len(availableSymbols)-deltaBatches) / requestSize
else:
    totalBatches = len(availableSymbols) / requestSize

# Recheck if all stocks are being covered
if ((totalBatches*requestSize)+deltaBatches) != len(availableSymbols) :
    print("Recheck batch division logic \n Expected :",len(availableSymbols),"\n Found :",((totalBatches*requestSize)+deltaBatches) )

batch = 1
startTime = time.time()

# Get stock $ data in batches and parse JSON to DataFrame
while batch <= totalBatches:
    stockData = Stock( availableSymbols['symbol'][startIndex:endIndex].values.tolist() , token = API_KEY)
    stockJSON = stockData.get_quote()

    # Updating the stockPrice DF
    for stock in stockJSON:
        stockName = stockJSON[stock]['companyName'].replace(",","")
        stockName = stockName.replace("+","")
        if str(stockJSON[stock]['latestTime']) == 'None':
            stockTime = ""
        else:
            stockTime = stockJSON[stock]['latestTime'].replace(",","")
        stockPriceDF = stockPriceDF.append({"StockSymbol"       : stockJSON[stock]['symbol'],
                                            "StockName"         : stockName,
                                            "StockPrice"        : stockJSON[stock]['latestPrice'],
                                            "Change"            : stockJSON[stock]['change'],
                                            "ChangePercentage"  : stockJSON[stock]['changePercent'],
                                            "52WHigh"           : stockJSON[stock]['week52High'],
                                            "52WLow"            : stockJSON[stock]['week52Low'],
                                            "Time"              : stockTime } , ignore_index = True)
    time.sleep(1) # Just to make sure next API call doesn't fail
    startIndex = endIndex
    endIndex += requestSize
    batch +=1

# Get stock data of deltaBatches
endIndex = endIndex-requestSize
stockData = Stock( availableSymbols['symbol'][endIndex:(endIndex+deltaBatches)].values.tolist() , token = API_KEY)
stockJSON = stockData.get_quote()

# Updating the stockPrice DF
for stock in stockJSON:
    stockName = stockJSON[stock]['companyName'].replace(",","")
    stockName = stockName.replace("+","")
    stockName = stockName.replace("'","")
    if str(stockJSON[stock]['latestTime']) == 'None':
        stockTime = ""
    else:
        stockTime = stockJSON[stock]['latestTime'].replace(",","")
    stockPriceDF = stockPriceDF.append({"StockSymbol"       : stockJSON[stock]['symbol'],
                                        "StockName"         : stockName,
                                        "StockPrice"        : stockJSON[stock]['latestPrice'],
                                        "Change"            : stockJSON[stock]['change'],
                                        "ChangePercentage"  : stockJSON[stock]['changePercent'],
                                        "52WHigh"           : stockJSON[stock]['week52High'],
                                        "52WLow"            : stockJSON[stock]['week52Low'],
                                        "Time"              : stockTime } , ignore_index = True)

print("\nLoading of stock financial data COMPLTED in: ", (time.time()- startTime), "seconds")

#
## SAVE Data to DISK
currentTime = datetime.now()
destinationFile = destinationFile + currentTime.strftime("%d%m%Y_%H%M%S") + ".csv"
stockPriceDF.to_csv(destinationFile, index=False)

#
## MOVE DATA TO HDFS
os.system("hadoop fs -put "+destinationFile+" "+destinationDir)

## Send data download confirmation message in the queue
producer.send(topic, key=b"SUCCESS", value=b""+destinationDir+destinationFile)
producer.flush()
print("Data ingested to HDFS and notified in kafka")

## Clean up local FS
os.system("rm -rf "+destinationFile) 
