#!/bin/sh
##################################################################
#Author: Yawei Zhu
#Date: 05/01/2020
################################################################## 
hive -e "set hive.cli.print.header=true; SELECT * FROM MyProject.stockInfo ORDER BY change_percentage DESC LIMIT 10;" | sed 's/[\t]/,/g'  > /home/hadoop/top.csv
printf "********** Top 10 stocks based on changePercentage value **********\n"
cat /home/hadoop/top.csv
hive -e "set hive.cli.print.header=true; SELECT * FROM MyProject.stockInfo ORDER BY change_percentage ASC NULLS LAST LIMIT 10;" | sed 's/[\t]/,/g'  > /home/hadoop/bottom.csv
printf "********** Last 10 stocks based on changePercentage value **********\n"
cat /home/hadoop/bottom.csv