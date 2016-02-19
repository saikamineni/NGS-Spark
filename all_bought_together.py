#---------------all_bought_together.py----------------------#
import sys
from pyspark import SparkContext

#creating spark context
sc = SparkContext()

#loading data from input file
ini_Data = sc.textFile("file:/home/training/Downloads/12_dashboard_tables/data_input/shopify/orders_export.csv")

#removing rows which don't start with orderID
reqd_Rows = ini_Data.filter(lambda line: line.startswith('#'))

#extracting orderID, product name
reqd_Cols = reqd_Rows.map(lambda line: (line.split(',')[0], line.split(',')[17])).distinct()

#grouping all the orders of a product
grpd_Data = reqd_Cols.map(lambda (orderID, prodName): (prodName, orderID)).groupByKey()

#applying cartesian product
cross_Data = grpd_Data.cartesian(grpd_Data)

#removing same products
drop_Same = cross_Data.filter(lambda line: line[0][0]!=line[1][0])

#count the orders in which both the products are present
count_data = drop_Same.map(lambda ((Prod1, lis1), (Prod2, lis2)): (Prod1, Prod2, len(list(set(list(lis1)).intersection(list(lis2))))))

#removing rows with count = 0
reqd_Count = count_data.filter(lambda line: line[2]!=0)

#sorting the product names and remove duplicates
rem_Dup = reqd_Count.map(lambda line: (line[1], line[0], line[2]) if line[0]> line[1] else (line[0], line[1], line[2])).distinct()

#sorting the data by count
final_Data = rem_Dup.map(lambda (prod1, prod2, count): (count, (prod1, prod2))).sortByKey(False).map(lambda (count, (prod1, prod2)): (prod1, prod2, count))

#save data to text file
final_Data.saveAsTextFile("file:/home/training/Documents/NGS-Spark/all_bought_together")
