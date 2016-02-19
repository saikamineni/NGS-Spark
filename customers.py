#---------------------------------customers.py-----------------
import datetime as DT
from datetime import datetime
import re
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

#day_hour_purchases.py
#extracting orderID, fulfillment status, date
reqd_Cols = cols_Data.map(lambda line: (line[0], line[1], line[15])).distinct()


def getHours(line):
    matchobj = re.match(r'(.*) (.*) (.*).*',line[2])
    data = matchobj.group(2)
    num = (datetime.strptime(data, '%H:%M:%S')).hour
    if(num < 2):
        return (line[0], line[1], line[2], 0)
    elif((num >= 2) & (num < 4)):
        return (line[0], line[1], line[2], 2)
    elif((num >= 4) & (num < 6)):
        return (line[0], line[1], line[2], 4)
    elif((num >= 6) & (num < 8)):
        return (line[0], line[1], line[2], 6)
    elif((num >= 8) & (num < 10)):
        return (line[0], line[1], line[2], 8)
    elif((num >= 10) & (num < 12)):
        return (line[0], line[1], line[2], 10)
    elif((num >= 12) & (num < 14)):
        return (line[0], line[1], line[2], 12)
    elif((num >= 14) & (num < 16)):
        return (line[0], line[1], line[2], 14)
    elif((num >= 16) & (num < 18)):
        return (line[0], line[1], line[2], 16)
    elif((num >= 18) & (num < 20)):
        return (line[0], line[1], line[2], 18)
    elif((num >= 20) & (num < 22)):
        return (line[0], line[1], line[2], 20)
    elif(num >= 22):
        return (line[0], line[1], line[2], 22)

hrs_Data = reqd_Cols.map(getHours)

day_Data = hrs_Data.map(lambda line: (line[0], line[1], (datetime.strptime(line[2][:10], '%Y-%m-%d')).date().strftime("%A"), line[3])).distinct()

#methods to return 1 when purchase is made on respective day or hour
def sundayRdd(line):
    if (line[2] == 'Sunday'):
        return (line[1], 1)
    else:
        return (line[1], 0)


def mondayRdd(line):
    if (line[2] == 'Monday'):
        return (line[1], 1)
    else:
        return (line[1], 0)


def tuesdayRdd(line):
    if (line[2] == 'Tuesday'):
        return (line[1], 1)
    else:
        return (line[1], 0)


def wednesdayRdd(line):
    if (line[2] == 'Wednesday'):
        return (line[1], 1)
    else:
        return (line[1], 0)


def thursdayRdd(line):
    if (line[2] == 'Thursday'):
        return (line[1], 1)
    else:
        return (line[1], 0)


def fridayRdd(line):
    if (line[2] == 'Friday'):
        return (line[1], 1)
    else:
        return (line[1], 0)


def saturdayRdd(line):
    if (line[2] == 'Saturday'):
        return (line[1], 1)
    else:
        return (line[1], 0)


def hrs_0_Rdd(line):
    if (line[3] == 0):
        return (line[1], 1)
    else:
        return (line[1], 0)


def hrs_2_Rdd(line):
    if (line[3] == 2):
        return (line[1], 1)
    else:
        return (line[1], 0)


def hrs_4_Rdd(line):
    if (line[3] == 4):
        return (line[1], 1)
    else:
        return (line[1], 0)


def hrs_6_Rdd(line):
    if (line[3] == 6):
        return (line[1], 1)
    else:
        return (line[1], 0)


def hrs_8_Rdd(line):
    if (line[3] == 8):
        return (line[1], 1)
    else:
        return (line[1], 0)


def hrs_10_Rdd(line):
    if (line[3] == 10):
        return (line[1], 1)
    else:
        return (line[1], 0)


def hrs_12_Rdd(line):
    if (line[3] == 12):
        return (line[1], 1)
    else:
        return (line[1], 0)


def hrs_14_Rdd(line):
    if (line[3] == 14):
        return (line[1], 1)
    else:
        return (line[1], 0)


def hrs_16_Rdd(line):
    if (line[3] == 16):
        return (line[1], 1)
    else:
        return (line[1], 0)


def hrs_18_Rdd(line):
    if (line[3] == 18):
        return (line[1], 1)
    else:
        return (line[1], 0)


def hrs_20_Rdd(line):
    if (line[3] == 20):
        return (line[1], 1)
    else:
        return (line[1], 0)


def hrs_22_Rdd(line):
    if (line[3] == 22):
        return (line[1], 1)
    else:
        return (line[1], 0)


#aggregating all the values in same day or hour
sunday_Data = day_Data.map(sundayRdd).reduceByKey(lambda a,b: a+b)
monday_Data = day_Data.map(mondayRdd).reduceByKey(lambda a,b: a+b)
tuesday_Data = day_Data.map(tuesdayRdd).reduceByKey(lambda a,b: a+b)
wednesday_Data = day_Data.map(wednesdayRdd).reduceByKey(lambda a,b: a+b)
thursday_Data = day_Data.map(thursdayRdd).reduceByKey(lambda a,b: a+b)
friday_Data = day_Data.map(fridayRdd).reduceByKey(lambda a,b: a+b)
saturday_Data = day_Data.map(saturdayRdd).reduceByKey(lambda a,b: a+b)
Hrs0_Data = day_Data.map(hrs_0_Rdd).reduceByKey(lambda a,b: a+b)
Hrs2_Data = day_Data.map(hrs_2_Rdd).reduceByKey(lambda a,b: a+b)
Hrs4_Data = day_Data.map(hrs_4_Rdd).reduceByKey(lambda a,b: a+b)
Hrs6_Data = day_Data.map(hrs_6_Rdd).reduceByKey(lambda a,b: a+b)
Hrs8_Data = day_Data.map(hrs_8_Rdd).reduceByKey(lambda a,b: a+b)
Hrs10_Data = day_Data.map(hrs_10_Rdd).reduceByKey(lambda a,b: a+b)
Hrs12_Data = day_Data.map(hrs_12_Rdd).reduceByKey(lambda a,b: a+b)
Hrs14_Data = day_Data.map(hrs_14_Rdd).reduceByKey(lambda a,b: a+b)
Hrs16_Data = day_Data.map(hrs_16_Rdd).reduceByKey(lambda a,b: a+b)
Hrs18_Data = day_Data.map(hrs_18_Rdd).reduceByKey(lambda a,b: a+b)
Hrs20_Data = day_Data.map(hrs_20_Rdd).reduceByKey(lambda a,b: a+b)
Hrs22_Data = day_Data.map(hrs_22_Rdd).reduceByKey(lambda a,b: a+b)

#join all the aggregated RDDs
joined_Data = sunday_Data.join(monday_Data).join(tuesday_Data).join(wednesday_Data).join(thursday_Data).join(friday_Data)\
.join(saturday_Data).join(Hrs0_Data).join(Hrs2_Data).join(Hrs4_Data).join(Hrs6_Data).join(Hrs8_Data).join(Hrs10_Data)\
.join(Hrs12_Data).join(Hrs14_Data).join(Hrs16_Data).join(Hrs18_Data).join(Hrs20_Data).join(Hrs22_Data)

#reordering the RDD
day_hr_Data = joined_Data.map(lambda (v1, ((((((((((((((((((v3, v4), v5), v6), v7), v8), v9), v10), v11), v12), v13), v14), v15), v16), v17), v18), v19), v20)\
    , v21)):(v1, (v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19, v20, v21)))


#info_segments.py
#take email and quantity*price
reqd_Cols = cols_Data.map(lambda line: (line[1], float(line[16])*float(line[18]))).reduceByKey(lambda x,y: x+y)

#orderID and email columns
ord_Data = cols_Data.map(lambda line: (line[0],line[1])).distinct()

#count number of orders by each customer
count_Data = ord_Data.map(lambda line: (line[1],1)).reduceByKey(lambda x,y: x+y)

#calculate basket value
bv_Data = count_Data.join(reqd_Cols).map(lambda line: (line[0], (line[1][1], line[1][1]/float(line[1][0]))))

#customers file
cust_Data = sc.parallelize(sc.textFile("file:/home/training/Downloads/12_dashboard_tables/data_input/shopify/customers_export.csv").collect()[1:])

#split all the columns and save data to memory by caching
cust_split = cust_Data.map(loadRecord)

#take all the required fields in customers
cust_det_Data = cust_split.map(lambda line: (line[2], (line[0]+" "+line[1], line[4]+" "+line[5]+" "+line[6], line[12])))

#segments file
seg_Data = sc.parallelize(sc.textFile("file:/home/training/Downloads/12_dashboard_tables/data_input/enodos/segments.csv").collect()[1:]).map(loadRecord)\
.map(lambda line: (line[0], line[1]))

#rearrange all the columns
segments_Data = bv_Data.join(cust_det_Data).leftOuterJoin(seg_Data)\
.map(lambda line: (line[0], (line[1][0][0][0], line[1][0][0][1], line[1][1], line[1][0][1][0], line[1][0][1][1], line[1][0][1][2])))


#latest_order_days.py
#take email and date columns
reqd_Cols = cols_Data.map(lambda line: (line[1], line[15][:10])).distinct()

#days from the order
days_Data = reqd_Cols.map(lambda line: ((line[0], (DT.date.today() - datetime.strptime(line[1], "%Y-%m-%d").date()).days),1)).sortByKey(False)

#email and the latest order
latest_order_Data = days_Data.map(lambda line: (line[0][0], line[0][1])).groupByKey().map(lambda x : (x[0], list(x[1])))\
.map(lambda line: (line[0], line[1][0]))


#order_frequency_customers.py
#orderID, email, date columns
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
order_frequency_Data = count_Data.join(diff_Data).map(lambda line: (line[0], (line[1][0], line[1][1]/ float(line[1][0])))).filter(lambda line: line[1]!=0)


#customer.py
temp_join_Data = latest_order_Data.join(order_frequency_Data).join(day_hr_Data).join(segments_Data)

#quantity and revenue
rev_Data = cols_Data.map(lambda line: (float(line[16]), float(line[16])*float(line[18])))

#total emails
email_count = temp_join_Data.map(lambda line: line[0]).distinct().count()
#total revenue
rev_total = sum(rev_Data.map(lambda line: line[1]).collect())
#total quantity
quan_total = sum(rev_Data.map(lambda line: line[0]).collect())

#read customer score file and reorder the RDD
cust_score = sc.parallelize(sc.textFile("file:/home/training/Downloads/12_dashboard_tables/11_monthly_segments/cust_score.csv").collect()[1:]).map(loadRecord)\
.map(lambda line: (line[0], (line[1:])))


final_Data = temp_join_Data.join(cust_score).map(lambda a: (a[1][0][1][3], a[1][0][1][0], a[1][0][1][1], a[1][0][1][2], a[1][0][1][4], a[1][0][1][5],\
 a[1][0][0][0][1][0], a[1][0][0][0][1][1], a[0], a[1][0][0][0][0], a[1][0][0][1][0], a[1][0][0][1][1], a[1][0][0][1][2], a[1][0][0][1][3], a[1][0][0][1][4],\
  a[1][0][0][1][5], a[1][0][0][1][6], a[1][0][0][1][7], a[1][0][0][1][8], a[1][0][0][1][9], a[1][0][0][1][10], a[1][0][0][1][11], a[1][0][0][1][12],\
   a[1][0][0][1][13], a[1][0][0][1][14], a[1][0][0][1][15], a[1][0][0][1][16], a[1][0][0][1][17], a[1][0][0][1][18], float(rev_total)/float(email_count),\
    float(quan_total)/float(email_count), a[1][1]))

final_Data.saveAsTextFile("file:/home/training/Documents/NGS-Spark/customers")