#-----------------------------week_unfulfilled_orders.py----------------------
import datetime as DT
from datetime import datetime
import csv
import StringIO
import sys
from pyspark import SparkContext

#creating spark context
sc = SparkContext()

#loading data from input file
ini_Data = sc.textFile("file:/home/training/Downloads/12_dashboard_tables/data_input/shopify/orders_export.csv")

#removing rows which don't start with orderID 
reqd_Rows = ini_Data.filter(lambda line: line.startswith('#'))

#function to convert data into utf-8 form and split data on commas
def loadRecord(line):
    line = line.encode('utf-8')
    input = StringIO.StringIO(line)
    reader = csv.reader(input)
    return (reader.next())

#split all the columns and save data to memory by caching
cols_Data = reqd_Rows.map(loadRecord)
cols_Data.cache()

#extracting orderID, fulfillment status, date
reqd_Cols = cols_Data.map(lambda line: (line[0], line[4], line[15][:10])).distinct()

#filter data havinf any fulfillment status
fulfil_Data = reqd_Cols.filter(lambda line: line[1]!='')

#get todays date
today = DT.date.today()
#dt = today - DT.timedelta(days=7) #Wednesday date
dt = (today - DT.timedelta(days=300))

#filter orders before particular date
ord_Data = fulfil_Data.filter(lambda x: datetime.strptime(x[2], '%Y-%m-%d').date() > dt)

#function to replace string 'unfulfilled' with 1
def replaceFun(line):
    if line[1] == 'unfulfilled':
        return (line[2], 1)
    else:
        return (line[2], 0)


#count total orders on a day
count_Data = ord_Data.map(lambda line: (line[2], 1)).reduceByKey(lambda a,b: a+b)

#count number of unfulfilled orders
unfulfill_Data = ord_Data.map(replaceFun).reduceByKey(lambda a,b: a+b)

#calculate unfulfilled %
final_Data = unfulfill_Data.join(count_Data).map(lambda line: (line[0], line[1][0]/float(line[1][1])))

final_Data.saveAsTextFile("file:/home/training/Documents/NGS-Spark/week_unfulfilled_orders")
