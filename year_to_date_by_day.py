#-----------------year_to_date_by_day.py-------------------------------
from datetime import datetime
import sys
from pyspark import SparkContext

#creating spark context
sc = SparkContext()

#loading data from input file
ini_Data = sc.textFile("file:/home/training/Downloads/12_dashboard_tables/data_input/vend/vend-total_revenue-sales_summary-by-day.csv")

#get only dates from the data
dates_Data = sc.parallelize(ini_Data.map(lambda line: line.split(",")).take(1)[0][1:-5])

#change the date into required format
form_date_Data = dates_Data.map(lambda line: line.split(' ')).map(lambda line: line[1][:-2]+" "+line[2]+" "+line[3])\
.map(lambda line: (datetime.strptime(line, '%d %b %Y')).date().strftime("%Y-%m-%d"))

#revenue for the given dates
rev_Data = sc.parallelize(ini_Data.map(lambda line: line.split(",")).collect()[3][1:])

#cost of goods for the given dates
cog_Data = sc.parallelize(ini_Data.map(lambda line: line.split(",")).collect()[4][1:])

#gross profit for the given dates
gp_Data = sc.parallelize(ini_Data.map(lambda line: line.split(",")).collect()[5][1:])

#margin for the given dates
marg_Data = sc.parallelize(ini_Data.map(lambda line: line.split(",")).collect()[6][1:])

#join the RDDs row wise and formatting them at each join
final_Data = form_date_Data.zip(rev_Data).map(lambda line: (line[0], float(line[1]))).zip(cog_Data)\
.map(lambda line: (line[0][0], line[0][1], float(line[1]))).zip(gp_Data).map(lambda line: (line[0][0], line[0][1], line[0][2], float(line[1])))\
.zip(marg_Data).map(lambda line: (line[0][0], line[0][1], line[0][2], line[0][3], float(line[1])))

final_Data.saveAsTextFile("file:/home/training/Documents/NGS-Spark/year_to_date_by_day")

