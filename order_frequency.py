#-----------------------order_frequency.py--------------------------
import csv
import StringIO
from datetime import datetime
import numpy as np
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

#extracting email, date
reqd_Cols = cols_Data.map(lambda line: (line[1], line[15][:10])).distinct()

#sort by email and date
sorted_Data = reqd_Cols.map(lambda line: ((line[0], line[1]), (0, 0))).sortByKey()

#temporary data with first row pushed to last
reqd_Data = sc.parallelize(sorted_Data.map(lambda line: line[0]).collect()[1:]+(sorted_Data.map(lambda line: line[0]).take(1)))

#data format required for computation
format_Data = sorted_Data.zip(reqd_Data)

#function to calculate difference between two dates
def calcDiff(line):
    if (line[0][0][0] == line[1][0]):
        dif = (datetime.strptime(line[1][1], "%Y-%m-%d")-datetime.strptime(line[0][0][1], "%Y-%m-%d")).days
        return (line[0][0][0], 1, dif)
    else:
        return (line[0][0][0], 1, 0)


#calculate difference
calc_Data = format_Data.map(calcDiff)

#count data
count_Data = calc_Data.map(lambda line: (line[0], line[1])).reduceByKey(lambda a,b: a+b)

#difference data
diff_Data = calc_Data.map(lambda line: (line[0], line[2])).reduceByKey(lambda a,b: a+b)

#compute average days and retain average > 0
avg_Data = count_Data.join(diff_Data).map(lambda line: (line[0], line[1][1]/ float(line[1][0]))).filter(lambda line: line[1]!=0)

#list of all average values
a = np.asarray(avg_Data.map(lambda line: line[1]).collect())

#calculate all the ranges and create an RDD
final_Data = sc.parallelize([('0 to 4', np.compress((0 < a) & (a < 4), a).size), ('4 to 7', np.compress((4 <= a) & (a < 7), a).size), \
    ('7 to 15', np.compress((7 <= a) & (a < 15), a).size), ('15 to 30', np.compress((15 <= a) & (a < 30), a).size), \
    ('30 to 60', np.compress((30 <= a) & (a < 60), a).size), ('60 to 100', np.compress((60 <= a) & (a < 100), a).size), \
    ('100+', np.compress((100 <= a), a).size)])


final_Data.saveAsTextFile("file:/home/training/Documents/NGS-Spark/order_frequency")
