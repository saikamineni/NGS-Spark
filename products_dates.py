#--------------------------------products_dates.py----------------------------------
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

#extracting product name, sku, date, revenue
reqd_Cols = cols_Data.map(lambda line: (line[17].upper(), line[20].upper(), line[15][:10], float(line[16])*float(line[18]))).distinct()

def changeDate(line):
    if (datetime.strptime(line[2] , '%Y-%m-%d').date().strftime("%A") == 'Wednesday'):
        return ((line[0], line[1], (datetime.strptime(line[2] , '%Y-%m-%d').date() - DT.timedelta(days=6)).strftime('%Y-%m-%d')), line[3])
    if (datetime.strptime(line[2] , '%Y-%m-%d').date().strftime("%A") == 'Tuesday'):
        return ((line[0], line[1], (datetime.strptime(line[2] , '%Y-%m-%d').date() - DT.timedelta(days=5)).strftime('%Y-%m-%d')), line[3])
    if (datetime.strptime(line[2] , '%Y-%m-%d').date().strftime("%A") == 'Monday'):
        return ((line[0], line[1], (datetime.strptime(line[2] , '%Y-%m-%d').date() - DT.timedelta(days=4)).strftime('%Y-%m-%d')), line[3])
    if (datetime.strptime(line[2] , '%Y-%m-%d').date().strftime("%A") == 'Sunday'):
        return ((line[0], line[1], (datetime.strptime(line[2] , '%Y-%m-%d').date() - DT.timedelta(days=3)).strftime('%Y-%m-%d')), line[3])
    if (datetime.strptime(line[2] , '%Y-%m-%d').date().strftime("%A") == 'Saturday'):
        return ((line[0], line[1], (datetime.strptime(line[2] , '%Y-%m-%d').date() - DT.timedelta(days=2)).strftime('%Y-%m-%d')), line[3])
    if (datetime.strptime(line[2] , '%Y-%m-%d').date().strftime("%A") == 'Friday'):
        return ((line[0], line[1], (datetime.strptime(line[2] , '%Y-%m-%d').date() - DT.timedelta(days=1)).strftime('%Y-%m-%d')), line[3])
    if (datetime.strptime(line[2] , '%Y-%m-%d').date().strftime("%A") == 'Thursday'):
        return ((line[0], line[1], (datetime.strptime(line[2] , '%Y-%m-%d').date() - DT.timedelta(days=0)).strftime('%Y-%m-%d')), line[3])


#data with weekly dates and aggregated revenue by product and week
week_Data = reqd_Cols.map(changeDate).reduceByKey(lambda a, b: a+b).sortByKey().map(lambda line: ((line[0][0], line[0][1]), (line[0][2], line[1])))

#data with monthly dates and aggregated revenue by product and month
month_Data = reqd_Cols.map(lambda line: ((line[0], line[1], line[2][:8]+"01"), line[3])).reduceByKey(lambda a, b: a+b).sortByKey()

#data from all the time and aggregated revenue by product
prod_Data = reqd_Cols.map(lambda line: ((line[0], line[1]), line[3])).reduceByKey(lambda a, b: a+b).sortByKey()

#all months
months = month_Data.map(lambda line: (line[0][2],1)).distinct().sortByKey().keys().collect()

#all the products and thier sku
lis = prod_Data.keys().collect()

pro = []
mon = []
for li in lis:
    mon = mon + months
    for i in range(0, len(months)):
        pro.append(li)

#an empty RDD with all combinations of months and products
empty_Data = sc.parallelize(pro).zip(sc.parallelize(mon)).map(lambda line: ((line[0][0], line[0][1], line[1]) ,0))

def getRev(line):
    if (line[1][1] > 0):
        return (line[0], line[1][1])
    else:
        return (line[0], 0)


#filled revenue for all the months
all_months_Data = empty_Data.leftOuterJoin(month_Data).map(getRev).sortByKey().map(lambda line: ((line[0][0], line[0][1]), line[1]))

def f(x): return x
def add(a, b): return a +"," + str(b)
months_rev_Data = all_months_Data.combineByKey(str, add, add).sortByKey().join(prod_Data).map(lambda line: (line[0], line[1][1], line[1][0]))

#function to calculate cmgr and period
def cmgrCalc(line):
    t = -1
    for num in line[2].split(','):
        t = t + 1
        if(float(num)!=0):
            break
    n = len(months)-t
    m=n**(-1)
    y=line[1]/float(num)
    return (line[0], ((pow(y,m)-1)*100, n))


cmgr_Data = months_rev_Data.map(cmgrCalc)

#combine weekly data and cmgr and reorder
comb_Data = week_Data.leftOuterJoin(cmgr_Data).map(lambda line: (line[0][0], line[0][1], line[1][0][0], line[1][0][1], line[1][1][0], line[1][1][1]))

#total revenue
tot_rev = sum(comb_Data.map(lambda line: line[3]).collect())

#average revenue
avg_rev = tot_rev/float(comb_Data.count())

#combine all the columns and calculate %total revenue
final_Data = comb_Data.map(lambda line: (line[0], line[1], line[2], line[3], line[4], line[5], (line[3]*100)/float(tot_rev), avg_rev))

final_Data.saveAsTextFile("file:/home/training/Documents/NGS-Spark/products_dates")
