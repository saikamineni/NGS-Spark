#---------------monthly_bought_together.py----------------------#
import sys
from pyspark import SparkContext

#creating spark context
sc = SparkContext()

#loading data from input file
ini_Data = sc.textFile("file:/home/training/Downloads/12_dashboard_tables/data_input/shopify/orders_export.csv")

#removing rows which don't start with orderID
reqd_Rows = ini_Data.filter(lambda line: line.startswith('#'))

#extracting orderID, product name, date
reqd_Cols = reqd_Rows.map(lambda line: (line.split(',')[0], line.split(',')[17], line.split(',')[15])).distinct()

#make product name and date as key
key_Data = reqd_Cols.map(lambda line: ((line[1], line[2][:8]+'01'), line[0]))

#grouping all the orders of a product in a month
grpd_Data = key_Data.groupByKey()

#applying cartesian product
cross_Data = grpd_Data.cartesian(grpd_Data)

#removing same products
drop_Same = cross_Data.filter(lambda line: line[0][0]!=line[1][0])

#get same month purchases
mnth_Data = drop_Same.filter(lambda (((Prod1, Date1), lis1), ((Prod2, Date2), lis2)): Date1==Date2)

#count the orders in which both the products are present
count_data = mnth_Data.map(lambda (((Prod1, Date1), lis1), ((Prod2, Date2), lis2)): (Prod1, Prod2, Date1, len(list(set(list(lis1)).intersection(list(lis2))))))

#removing rows with count = 0
reqd_Count = count_data.filter(lambda line: line[3]!=0)

#sorting the product names and remove duplicates
rem_Dup = reqd_Count.map(lambda line: (line[1], line[0], line[2], line[3]) if line[0]> line[1] else (line[0], line[1], line[2], line[3])).distinct()

#sorting the data by count
final_Data = rem_Dup.map(lambda (prod1, prod2, Date, count): (count, (prod1, prod2, Date))).sortByKey(False)\
.map(lambda (count, (prod1, prod2, Date)): (Date, prod1, prod2, count))

#save data to text file
final_Data.saveAsTextFile("file:/home/training/Documents/NGS-Spark/monthly_bought_together")

