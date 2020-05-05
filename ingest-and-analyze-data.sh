#!/bin/sh
##################################################################
#Author: Yawei Zhu
#Date: 05/01/2020
################################################################## 

database="StockMarket"
table="StockData"
resultsPath="/home/hadoop/stocks"

echo "Creating Database"
hive -e "CREATE DATABASE IF NOT EXISTS $database;"

echo "Creating External Table"
hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS $database.$table(stock_symbol STRING, stock_name STRING, stock_price DOUBLE, change DOUBLE, change_percentage DOUBLE, 52WHigh DOUBLE, 52WLow DOUBLE, Time DOUBLE ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '$1';"

echo "Finding top 10"
hive -e "set hive.cli.print.header=true; SELECT * FROM $database.$table ORDER BY change_percentage DESC LIMIT 10;" | sed 's/[\t]/,/g'  > top.csv
printf "********** Top 10 stocks based on changePercentage value **********\n"
cat top.csv

echo "Finding bottom 10"
hive -e "set hive.cli.print.header=true; SELECT * FROM $database.$table ORDER BY change_percentage ASC NULLS LAST LIMIT 10;" | sed 's/[\t]/,/g'  > bottom.csv
printf "********** Last 10 stocks based on changePercentage value **********\n"
cat bottom.csv
