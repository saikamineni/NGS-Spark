#-------------------------------------revenue_by_type.py-----------------------------
from datetime import datetime
import sys
from pyspark import SparkContext

#creating spark context
sc = SparkContext()

#loading data from input file
#revenue
ini_Data = sc.textFile("file:/home/training/Downloads/12_dashboard_tables/data_input/vend/vend-total_revenue-for-type-by-month.csv")
#gross profit
ini_Data1 = sc.textFile("file:/home/training/Downloads/12_dashboard_tables/data_input/vend/vend-gross_profit-for-type-by-month.csv")

#get the first row and change the dates
month_Dates = sc.parallelize(ini_Data.map(lambda line: line.split(",")).take(1)[0][1:-5])\
.map(lambda line: (datetime.strptime(line, '%b %Y')).date().strftime("%Y-%m-%d"))

#seperate types data
#revenue
type_Data = sc.parallelize(ini_Data.collect()[1:-6])
#gross profit
type_Data1 = sc.parallelize(ini_Data1.collect()[1:-6])


#count of months and types
types = type_Data.count()
months = month_Dates.count()

#repeat all the months for types number of times
mt = []
for i in range(0, types):
    mt = mt + month_Dates.collect()


all_months_Data = sc.parallelize(mt)

#types with months number of times
#all the revenues and gross profits type wise
tp = []
rev = []
gp = []
for i in range(0, types):
    name = type_Data.map(lambda line: line.split(",")).collect()[i][0]
    rev = rev + type_Data.map(lambda line: line.split(",")).collect()[i][1:months+1]
    gp = gp + type_Data1.map(lambda line: line.split(",")).collect()[i][1:months+1]
    for j in range(0, months):
        tp.append(name)


all_types_Data = sc.parallelize(tp)
rev_Data = sc.parallelize(rev)
gp_Data = sc.parallelize(gp)

#combine dates, revenue, gross profit and types
final_Data = all_months_Data.zip(rev_Data).map(lambda line: (line[0], float(line[1]))).zip(gp_Data)\
.map(lambda line: (line[0][0], line[0][1], float(line[1]))).zip(all_types_Data).map(lambda line: (line[0][0], line[0][1], line[0][2], line[1]))

final_Data.saveAsTextFile("file:/home/training/Documents/NGS-Spark/revenue_by_type")
