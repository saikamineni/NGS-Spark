#------------------customer_retention.py------------------#
from datetime import datetime, timedelta
import sys
from pyspark import SparkContext

#creating spark context
sc = SparkContext()

#loading data from input file
ini_Data = sc.textFile("file:/home/training/Downloads/12_dashboard_tables/data_input/shopify/orders_export.csv")

#removing rows which don't start with orderID
reqd_Rows = ini_Data.filter(lambda line: line.startswith('#'))

#extracting orderID, product name, date
reqd_Cols = reqd_Rows.map(lambda line: (line.split(',')[1], line.split(',')[15])).distinct()

#make product name and date as key
key_Data = reqd_Cols.map(lambda line: (line[0],line[1][:8]+'01')).distinct()

#add a column with date exactly 365 days back
year_date = key_Data.map(lambda line: (line[0], line[1], (str)(datetime.now() - timedelta(days=365))[:10]))

#calculate days between dates
days_Between = year_date.map(lambda line: (line[0], line[1], (datetime.strptime(line[1], "%Y-%m-%d")-datetime.strptime(line[2], "%Y-%m-%d")).days))

#get the emails in last year
year_Data = days_Between.filter(lambda line: line[2]>=0 & line[2] <=365).map(lambda line: (line[0])).distinct()

#gets the list of customers who ordered in last 20 days
days_20 = key_Data.map(lambda line: (line[0], line[1], (str)(datetime.now() - timedelta(days=20))[:10]))\
.map(lambda line: (line[0], line[1], (datetime.strptime(line[1], "%Y-%m-%d")-datetime.strptime(line[2], "%Y-%m-%d")).days))\
.filter(lambda line: line[2]>=0 & line[2] <=20).map(lambda line: (line[0], 1)).reduceByKey(lambda v1, v2: v1+v2)

#gets the list of customers who ordered in last 30 days more than once
days_30 = key_Data.map(lambda line: (line[0], line[1], (str)(datetime.now() - timedelta(days=30))[:10]))\
.map(lambda line: (line[0], line[1], (datetime.strptime(line[1], "%Y-%m-%d")-datetime.strptime(line[2], "%Y-%m-%d")).days))\
.filter(lambda line: line[2]>=0 & line[2] <=30).map(lambda line: (line[0], 1)).reduceByKey(lambda v1, v2: v1+v2).filter(lambda line: line[1]>1)

#creating a list
lis = [days_30.count(), days_20.count(), year_Data.count()]
lis .append(lis[0]/float(lis[2]))
lis .append(lis[1]/float(lis[2]))

#list to RDD
final_Data = sc.parallelize(lis)

final_Data.saveAsTextFile("file:/home/training/Documents/NGS-Spark/cust_retention")
