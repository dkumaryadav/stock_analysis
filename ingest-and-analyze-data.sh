#!/bin/sh
##################################################################
#Author: Yawei Zhu
#Date: 05/01/2020
# $1 : file in data-landing
# $2 : File name for top10 stocks 
# $3 : File name for bottom10 stocks
#
#
################################################################## 

database="StockMarket"
table="StockData"
basePath="/user/hadoop/stocks/"
ingestionFile=$basePath"/data-landing/"
processedFile=$basePath"/processed/"
top10=$basePath"/top10/"
bottom10=$basePath"/bottom10/"

echo "Ingestion File is $ingestionFile"
echo "Top10 file will be saved as $top10"
echo "Bottom10 file will be saved as $bottom10"

echo "Creating Database"
hive -e "CREATE DATABASE IF NOT EXISTS $database;"

echo "Creating External Table"
hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS $database.$table(stock_symbol STRING, stock_name STRING, stock_price DOUBLE, change DOUBLE, change_percentage DOUBLE, 52WHigh DOUBLE, 52WLow DOUBLE, Time DOUBLE ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '$ingestionFile';"
hadoop fs -mv $ingestionFile$1 $processedFile

echo "Finding top 10 and storing as $2 in HDFS"
hive -e "set hive.cli.print.header=true; SELECT * FROM $database.$table ORDER BY change_percentage DESC LIMIT 10;" | sed 's/[\t]/,\t\t/g'  > $2
hadoop fs -put $2 $top10

echo "Finding bottom 10 and storing as $3 in HDFS"
hive -e "set hive.cli.print.header=true; SELECT * FROM $database.$table ORDER BY change_percentage ASC NULLS LAST LIMIT 10;" | sed 's/[\t]/,\t\t/g'  > $3
hadoop fs -put $3 $bottom10
