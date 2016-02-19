#------------------------------products.py---------------------
import datetime as DT
from datetime import datetime
import csv
import StringIO
import sys
from pyspark import SparkContext

#creating spark context
sc = SparkContext()

#loading data from input file
ini_Data = sc.textFile("file:/home/training/Downloads/12_dashboard_tables/data_input/vend/vend-total_revenue-for-product_variant-by-month.csv")

#remove unnecessary rows
reqd_Rows = sc.parallelize(ini_Data.collect()[1:-6])

#function to convert data into utf-8 form and split data on commas
def loadRecord(line):
    line = line.encode('utf-8')
    input = StringIO.StringIO(line)
    reader = csv.reader(input)
    return (reader.next())

#split all the columns and save data to memory by caching
cols_Data = reqd_Rows.map(loadRecord)
cols_Data.cache()

#take required columns
reqd_Cols = cols_Data.map(lambda line: ((line[0], line[3], line[5]), line[7:-6], line[-5], line[-3]))

#function to calculate cmgr and period
def cmgrCalc(line):
    t = -1
    for num in line[1]:
        t = t + 1
        if(float(num)!=0):
            break
    n = len(line[1])-t
    m=n**(-1)
    y=float(line[2])/float(num)
    return (line[0][0], line[0][1], line[0][2], float(line[2]), float(line[3]), (pow(y,m)-1)*100, n)


cmgr_Data = reqd_Cols.map(cmgrCalc)


#total revenue
tot_rev = sum(cmgr_Data.map(lambda line: line[3]).collect())
#average revenue
avg_rev = tot_rev/float(cmgr_Data.count())
#total gross profit
tot_gp = sum(cmgr_Data.map(lambda line: line[4]).collect())
#average gross profit
avg_gp = tot_gp/float(cmgr_Data.count())

#calculate all the variables and integrate data
final_Data = cmgr_Data.map(lambda line: (line[0], line[1], line[2], line[3], line[4], line[5], line[6], (line[3]*100/tot_rev), avg_rev, avg_gp,\
 ((line[3]-avg_rev)*100/avg_rev), (line[4]*100/tot_gp)))

final_Data.saveAsTextFile("file:/home/training/Documents/NGS-Spark/products")
