#!/bin/sh
##################################################################
#Author: Yawei Zhu
#Date: 05/01/2020
################################################################## 
database="StockMarket"
table="StockData"
resultsPath="/home/hadoop/stock"
hive -e "create database $database;"
hive -e "CREATE TABLE IF NOT EXISTS $database.$table(stock_symbol STRING, stock_name STRING, stock_price DOUBLE, change DOUBLE, change_percentage DOUBLE, 52WHigh DOUBLE, 52WLow DOUBLE, Time DOUBLE ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;"
hive -e "LOAD DATA LOCAL INPATH $1 OVERWRITE INTO TABLE  $database.$table;"
hive -e "set hive.cli.print.header=true; SELECT * FROM $database.$table ORDER BY change_percentage DESC LIMIT 10;" | sed 's/[\t]/,/g'  > $resultsPath/top.csv
printf "********** Top 10 stocks based on changePercentage value **********\n"
cat $resultsPath/top.csv
hive -e "set hive.cli.print.header=true; SELECT * FROM MyProject.stockInfo ORDER BY change_percentage ASC NULLS LAST LIMIT 10;" | sed 's/[\t]/,/g'  > $resultPath/bottom.csv
printf "********** Last 10 stocks based on changePercentage value **********\n"
cat $resultsPath/bottom.csv