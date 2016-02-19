#-----------------------customer_dates.py--------------------------
from datetime import datetime
import re
import datetime as DT
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

#extracting orderID, product name, date
reqd_Cols = cols_Data.map(lambda line: (line[0], line[1], line[15], line[16], line[18])).distinct()

#calculate revenue
rev_Data = reqd_Cols.map(lambda line: (line[1], line[2], int(line[3])*float(line[4])))

#get required date and hour
date_Data = rev_Data.map(lambda line: (line[0], line[1][:10], line[1])).distinct()

#seperate revenue
rev_only_Data = rev_Data.map(lambda line: (line[0], line[1][:10], line[2], line[2]))


def getHours(line):
    matchobj = re.match(r'(.*) (.*) (.*).*',line[2])
    data = matchobj.group(2)
    num = (datetime.strptime(data, '%H:%M:%S')).hour
    if(num < 2):
        return (line[0], line[1], 0)
    elif((num >= 2) & (num < 4)):
        return (line[0], line[1], 2)
    elif((num >= 4) & (num < 6)):
        return (line[0], line[1], 4)
    elif((num >= 6) & (num < 8)):
        return (line[0], line[1], 6)
    elif((num >= 8) & (num < 10)):
        return (line[0], line[1], 8)
    elif((num >= 10) & (num < 12)):
        return (line[0], line[1], 10)
    elif((num >= 12) & (num < 14)):
        return (line[0], line[1], 12)
    elif((num >= 14) & (num < 16)):
        return (line[0], line[1], 14)
    elif((num >= 16) & (num < 18)):
        return (line[0], line[1], 16)
    elif((num >= 18) & (num < 20)):
        return (line[0], line[1], 18)
    elif((num >= 20) & (num < 22)):
        return (line[0], line[1], 20)
    elif(num >= 22):
        return (line[0], line[1], 22)

def getDay(line):
    return (line[0], line[1], line[2], (datetime.strptime(line[1], '%Y-%m-%d')).date().strftime("%A"))

def changeDate(line):
    if (datetime.strptime(line[1] , '%Y-%m-%d').date().strftime("%A") == 'Wednesday'):
        return (line[0], line[2], line[3], (datetime.strptime(line[1] , '%Y-%m-%d').date() - DT.timedelta(days=6)).strftime('%Y-%m-%d'))
    if (datetime.strptime(line[1] , '%Y-%m-%d').date().strftime("%A") == 'Tuesday'):
        return (line[0], line[2], line[3], (datetime.strptime(line[1] , '%Y-%m-%d').date() - DT.timedelta(days=5)).strftime('%Y-%m-%d'))
    if (datetime.strptime(line[1] , '%Y-%m-%d').date().strftime("%A") == 'Monday'):
        return (line[0], line[2], line[3], (datetime.strptime(line[1] , '%Y-%m-%d').date() - DT.timedelta(days=4)).strftime('%Y-%m-%d'))
    if (datetime.strptime(line[1] , '%Y-%m-%d').date().strftime("%A") == 'Sunday'):
        return (line[0], line[2], line[3], (datetime.strptime(line[1] , '%Y-%m-%d').date() - DT.timedelta(days=3)).strftime('%Y-%m-%d'))
    if (datetime.strptime(line[1] , '%Y-%m-%d').date().strftime("%A") == 'Saturday'):
        return (line[0], line[2], line[3], (datetime.strptime(line[1] , '%Y-%m-%d').date() - DT.timedelta(days=2)).strftime('%Y-%m-%d'))
    if (datetime.strptime(line[1] , '%Y-%m-%d').date().strftime("%A") == 'Friday'):
        return (line[0], line[2], line[3], (datetime.strptime(line[1] , '%Y-%m-%d').date() - DT.timedelta(days=1)).strftime('%Y-%m-%d'))
    if (datetime.strptime(line[1] , '%Y-%m-%d').date().strftime("%A") == 'Thursday'):
        return (line[0], line[2], line[3], (datetime.strptime(line[1] , '%Y-%m-%d').date() - DT.timedelta(days=0)).strftime('%Y-%m-%d'))


day_hr_Data = date_Data.map(getHours).map(getDay).map(changeDate)

#new RDD with email, date and revenue
rev_email_Data = rev_only_Data.map(changeDate).map(lambda line: ((line[0], line[3]), line[2]))

#find total orders in a week
tot_orders_Data = day_hr_Data.map(lambda line: ((line[0], line[3]), 1)).reduceByKey(lambda v1, v2: v1+v2)

#new RDD with email, date, day, hour
details_Data = day_hr_Data.map(lambda line: (line[0], line[3], line[2], line[1]))

#calculate total revenue
tot_Rev = rev_email_Data.reduceByKey(lambda v1, v2: v1+v2)

#calculate basket value
basket_Data = tot_Rev.join(tot_orders_Data).map(lambda ((v1, v2), (v3, v4)): ((v1, v2), v3/float(v4)))

#methods to return 1 when purchase is made on respective day or hour
def sundayRdd(line):
    if (line[2] == 'Sunday'):
        return ((line[0], line[1]), 1)
    else:
        return ((line[0], line[1]), 0)


def mondayRdd(line):
    if (line[2] == 'Monday'):
        return ((line[0], line[1]), 1)
    else:
        return ((line[0], line[1]), 0)


def tuesdayRdd(line):
    if (line[2] == 'Tuesday'):
        return ((line[0], line[1]), 1)
    else:
        return ((line[0], line[1]), 0)


def wednesdayRdd(line):
    if (line[2] == 'Wednesday'):
        return ((line[0], line[1]), 1)
    else:
        return ((line[0], line[1]), 0)


def thursdayRdd(line):
    if (line[2] == 'Thursday'):
        return ((line[0], line[1]), 1)
    else:
        return ((line[0], line[1]), 0)


def fridayRdd(line):
    if (line[2] == 'Friday'):
        return ((line[0], line[1]), 1)
    else:
        return ((line[0], line[1]), 0)


def saturdayRdd(line):
    if (line[2] == 'Saturday'):
        return ((line[0], line[1]), 1)
    else:
        return ((line[0], line[1]), 0)


def hrs_0_Rdd(line):
    if (line[3] == 0):
        return ((line[0], line[1]), 1)
    else:
        return ((line[0], line[1]), 0)


def hrs_2_Rdd(line):
    if (line[3] == 2):
        return ((line[0], line[1]), 1)
    else:
        return ((line[0], line[1]), 0)


def hrs_4_Rdd(line):
    if (line[3] == 4):
        return ((line[0], line[1]), 1)
    else:
        return ((line[0], line[1]), 0)


def hrs_6_Rdd(line):
    if (line[3] == 6):
        return ((line[0], line[1]), 1)
    else:
        return ((line[0], line[1]), 0)


def hrs_8_Rdd(line):
    if (line[3] == 8):
        return ((line[0], line[1]), 1)
    else:
        return ((line[0], line[1]), 0)


def hrs_10_Rdd(line):
    if (line[3] == 10):
        return ((line[0], line[1]), 1)
    else:
        return ((line[0], line[1]), 0)


def hrs_12_Rdd(line):
    if (line[3] == 12):
        return ((line[0], line[1]), 1)
    else:
        return ((line[0], line[1]), 0)


def hrs_14_Rdd(line):
    if (line[3] == 14):
        return ((line[0], line[1]), 1)
    else:
        return ((line[0], line[1]), 0)


def hrs_16_Rdd(line):
    if (line[3] == 16):
        return ((line[0], line[1]), 1)
    else:
        return ((line[0], line[1]), 0)


def hrs_18_Rdd(line):
    if (line[3] == 18):
        return ((line[0], line[1]), 1)
    else:
        return ((line[0], line[1]), 0)


def hrs_20_Rdd(line):
    if (line[3] == 20):
        return ((line[0], line[1]), 1)
    else:
        return ((line[0], line[1]), 0)


def hrs_22_Rdd(line):
    if (line[3] == 22):
        return ((line[0], line[1]), 1)
    else:
        return ((line[0], line[1]), 0)


#aggregating all the values in same day or hour
sunday_Data = details_Data.map(sundayRdd).reduceByKey(lambda a,b: a+b)
monday_Data = details_Data.map(mondayRdd).reduceByKey(lambda a,b: a+b)
tuesday_Data = details_Data.map(tuesdayRdd).reduceByKey(lambda a,b: a+b)
wednesday_Data = details_Data.map(wednesdayRdd).reduceByKey(lambda a,b: a+b)
thursday_Data = details_Data.map(thursdayRdd).reduceByKey(lambda a,b: a+b)
friday_Data = details_Data.map(fridayRdd).reduceByKey(lambda a,b: a+b)
saturday_Data = details_Data.map(saturdayRdd).reduceByKey(lambda a,b: a+b)
Hrs0_Data = details_Data.map(hrs_0_Rdd).reduceByKey(lambda a,b: a+b)
Hrs2_Data = details_Data.map(hrs_2_Rdd).reduceByKey(lambda a,b: a+b)
Hrs4_Data = details_Data.map(hrs_4_Rdd).reduceByKey(lambda a,b: a+b)
Hrs6_Data = details_Data.map(hrs_6_Rdd).reduceByKey(lambda a,b: a+b)
Hrs8_Data = details_Data.map(hrs_8_Rdd).reduceByKey(lambda a,b: a+b)
Hrs10_Data = details_Data.map(hrs_10_Rdd).reduceByKey(lambda a,b: a+b)
Hrs12_Data = details_Data.map(hrs_12_Rdd).reduceByKey(lambda a,b: a+b)
Hrs14_Data = details_Data.map(hrs_14_Rdd).reduceByKey(lambda a,b: a+b)
Hrs16_Data = details_Data.map(hrs_16_Rdd).reduceByKey(lambda a,b: a+b)
Hrs18_Data = details_Data.map(hrs_18_Rdd).reduceByKey(lambda a,b: a+b)
Hrs20_Data = details_Data.map(hrs_20_Rdd).reduceByKey(lambda a,b: a+b)
Hrs22_Data = details_Data.map(hrs_22_Rdd).reduceByKey(lambda a,b: a+b)

#join all the aggregated RDDs
joined_Data = sunday_Data.join(monday_Data).join(tuesday_Data).join(wednesday_Data).join(thursday_Data).join(friday_Data)\
.join(saturday_Data).join(Hrs0_Data).join(Hrs2_Data).join(Hrs4_Data).join(Hrs6_Data).join(Hrs8_Data).join(Hrs10_Data)\
.join(Hrs12_Data).join(Hrs14_Data).join(Hrs16_Data).join(Hrs18_Data).join(Hrs20_Data).join(Hrs22_Data).join(basket_Data).join(tot_orders_Data).join(tot_Rev)

#reordering the RDD
reorder_Data = joined_Data.map(lambda ((v1, v2), (((((((((((((((((((((v3, v4), v5), v6), v7), v8), v9), v10), v11), v12), v13), v14), v15), v16), v17), v18)\
    , v19), v20)\, v21), v22), v23), v24)):(v1, (v2, v23, v24, v22, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19, v20, v21)))

#customers file
cust_Data = sc.textFile("file:/home/training/Downloads/12_dashboard_tables/data_input/shopify/customers_export.csv")

#split all the columns and save data to memory by caching
cust_split = cust_Data.map(loadRecord)

#take all the required fields in customers
cust_det_Data = cust_split.map(lambda line: (line[2], (line[0]+" "+line[1], line[4]+" "+line[5]+" "+line[6], line[12])))

#calculate total quantity
avg_bs_Data = reqd_Cols.map(lambda line: ('a', int(line[3]))).reduceByKey(lambda a,b: a+b)

#calculate total revenue
avg_rev_Data = rev_Data.map(lambda line: ('a', float(line[2]))).reduceByKey(lambda a,b: a+b)

#calculate total number of email ids
email_Data = reqd_Cols.map(lambda line: line[1]).distinct()

#values of average basket value and average revenue
avg_bs_value = avg_bs_Data.values().collect()[0]/float(email_Data.count())
avg_rev_value = avg_rev_Data.values().collect()[0]/float(email_Data.count())

#rearrange all the columns
final_Data = cust_det_Data.join(reorder_Data).map(lambda line: (line[1][0][0], line[1][0][1], line[1][0][2], line[0], line[1][1][0], line[1][1][1],\
 line[1][1][2], line[1][1][3], line[1][1][4], line[1][1][5], line[1][1][6], line[1][1][7], line[1][1][8], line[1][1][9], line[1][1][10], line[1][1][11],\
  line[1][1][12], line[1][1][13], line[1][1][14], line[1][1][15], line[1][1][16], line[1][1][17], line[1][1][18], line[1][1][19], line[1][1][20],\
   line[1][1][21], line[1][1][22], avg_bs_value, avg_rev_value))

final_Data.saveAsTextFile("file:/home/training/Documents/NGS-Spark/customer_dates")
