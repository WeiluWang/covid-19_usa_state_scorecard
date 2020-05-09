# -*- coding: utf-8 -*-
"""
Created on Sun May  3 00:43:35 2020

@author: Moon
"""

import csv
import sys
import os
#import pickle
import math


# Open Spark

spark_home = os.environ.get('SPARK_HOME', None)
if not spark_home:
    raise ValueError('SPARK_HOME environment variable is not set')
sys.path.insert(0, os.path.join(spark_home, 'python'))
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.10.8.1-src.zip'))
comm = os.path.join(spark_home, 'python/lib/py4j-0.10.8.1-src.zip')
print('start spark....', comm)
exec(open(os.path.join(spark_home, 'python/pyspark/shell.py')).read())


from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.functions import log
from pyspark.sql import functions as F

sqlContext = SQLContext(sc)

WOE_ICU = spark.read\
               .format("csv")\
               .option("header", "true")\
               .load("C:/Users/Moon/Desktop/WOE_ICU.csv")

WOE_trends_mask = spark.read\
                       .format("csv")\
                       .option("header", "true")\
                       .load("C:/Users/Moon/Desktop/WOE_trends_mask.csv")
                       
WOE_trends_face_mask = spark.read\
                            .format("csv")\
                            .option("header", "true")\
                            .load("C:/Users/Moon/Desktop/WOE_trends_face_mask.csv")                       

WOE_trends_covid_19 = spark.read\
                           .format("csv")\
                           .option("header", "true")\
                           .load("C:/Users/Moon/Desktop/WOE_trends_covid_19.csv")
                           
WOE_trends_coronavirus = spark.read\
                           .format("csv")\
                           .option("header", "true")\
                           .load("C:/Users/Moon/Desktop/WOE_trends_coronavirus.csv")
                           
WOE_trends_sanitizer = spark.read\
                            .format("csv")\
                            .option("header", "true")\
                            .load("C:/Users/Moon/Desktop/WOE_trends_sanitizer.csv")
                           
WOE_air_avg = spark.read\
                   .format("csv")\
                   .option("header", "true")\
                   .load("C:/Users/Moon/Desktop/WOE_air_avg.csv")




WOE_ICU = WOE_ICU.withColumn("IV_ICU", (WOE_ICU.Distribution_of_Goods-WOE_ICU.Distribution_of_Bads)*WOE_ICU.WOE)

WOE_trends_mask = WOE_trends_mask.withColumn("IV_trends_mask", (WOE_trends_mask.Distribution_of_Goods-WOE_trends_mask.Distribution_of_Bads)*WOE_trends_mask.WOE)

WOE_trends_face_mask = WOE_trends_face_mask.withColumn("IV_trends_face_mask", (WOE_trends_face_mask.Distribution_of_Goods-WOE_trends_face_mask.Distribution_of_Bads)*WOE_trends_face_mask.WOE)

WOE_trends_covid_19 = WOE_trends_covid_19.withColumn("IV_trends_covid_19", (WOE_trends_covid_19.Distribution_of_Goods-WOE_trends_covid_19.Distribution_of_Bads)*WOE_trends_covid_19.WOE)

WOE_trends_coronavirus = WOE_trends_coronavirus.withColumn("IV_trends_coronavirus", (WOE_trends_coronavirus.Distribution_of_Goods-WOE_trends_coronavirus.Distribution_of_Bads)*WOE_trends_coronavirus.WOE)

WOE_trends_sanitizer = WOE_trends_sanitizer.withColumn("IV_trends_sanitizer", (WOE_trends_sanitizer.Distribution_of_Goods-WOE_trends_sanitizer.Distribution_of_Bads)*WOE_trends_sanitizer.WOE)

WOE_air_avg = WOE_air_avg.withColumn("IV_air_avg", (WOE_air_avg.Distribution_of_Goods-WOE_air_avg.Distribution_of_Bads)*WOE_air_avg.WOE)





sum_number0 = WOE_ICU.agg({"IV_ICU":"sum"}).collect()[0][0]
sum_number1 = WOE_trends_mask.agg({"IV_trends_mask":"sum"}).collect()[0][0]
sum_number2 = WOE_trends_face_mask.agg({"IV_trends_face_mask":"sum"}).collect()[0][0]
sum_number3 = WOE_trends_covid_19.agg({"IV_trends_covid_19":"sum"}).collect()[0][0]
sum_number4 = WOE_trends_coronavirus.agg({"IV_trends_coronavirus":"sum"}).collect()[0][0]
sum_number5 = WOE_trends_sanitizer.agg({"IV_trends_sanitizer":"sum"}).collect()[0][0]
sum_number6 = WOE_air_avg.agg({"IV_air_avg":"sum"}).collect()[0][0]
print("sum_number0 = ", sum_number0)
print("sum_number1 = ", sum_number1)
print("sum_number2 = ", sum_number2)
print("sum_number3 = ", sum_number3)
print("sum_number4 = ", sum_number4)
print("sum_number5 = ", sum_number5)
print("sum_number6 = ", sum_number6)


IV_value = sc.parallelize([Row(
                               IV = sum_number0,
                               Factor = "ICU",
                              ),
                           Row(
                               IV = sum_number1,
                               Factor = "trends_mask",
                              ),
                           Row(
                               IV = sum_number2,
                               Factor = "trends_face_mask",
                              ),
                           Row(
                               IV = sum_number3,
                               Factor = "trends_covid_19",
                              ),
                           Row(
                               IV = sum_number4,
                               Factor = "trends_coronavirus",
                              ),
                           Row(
                               IV = sum_number5,
                               Factor = "trends_sanitizer",
                              ),
                           Row(
                               IV = sum_number6,
                               Factor = "air_avg",
                              )])



IV_value = IV_value.toDF()
IV_value.show()


IV_value.coalesce(1)\
        .write\
        .option("header", "true")\
        .csv("IV_value.csv")
