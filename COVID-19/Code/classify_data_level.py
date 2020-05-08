# -*- coding: utf-8 -*-
"""
Created on Sat May  2 14:00:05 2020

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


sqlContext = SQLContext(sc)
#from pyspark.sql.functions import lit
#from pyspark.sql.functions import col, expr, when




main_df = spark.read\
               .format("csv")\
               .option("header", "true")\
               .load("C:/Users/ED/Desktop/covid/COVID-19/Date/main_df.csv")


main_df = main_df.withColumnRenamed("face mask", "face_mask")
main_df = main_df.withColumnRenamed("covid-19", "covid_19")
#main_df = main_df.withColumn("mix_mask", main_df.mask + main_df.face_mask)
#main_df = main_df.withColumn("mix_covid_19", main_df.covid_19 + main_df.coronavirus)

main_df.show(300)

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.functions import log

#----------------------------------------------------------------------------------------------------------------------

def func_MR(mr):
    if float(mr) <= 0.01:
        return 1
    elif float(mr) > 0.01: # and float(mr) <= 0.2:
        return 2
    #else:
       # return 3
    
    
func_udf = udf(func_MR, IntegerType())

main_df = main_df.withColumn('level_mr',func_udf(main_df.Mortality_Rate))
 
#----------------------------------------------------------------------------------------------------------------------

def func_ICU_per_60(bed):
    if float(bed) <= 750.0:
        return 1
    elif float(bed) > 750.0 and float(bed) <= 1000.0:
        return 2
    else:
        return 3
    

func_udf2 = udf(func_ICU_per_60, IntegerType())

main_df = main_df.withColumn('level_ICU_per_60',func_udf2(main_df.Residents_Aged_60_plus_Per_Each_ICU_Bed))

#----------------------------------------------------------------------------------------------------------------------

def func_trends_mask(mask):
    if float(mask) <= 6.0:
        return 1
    elif float(mask) > 6.0 and float(mask) <= 12.0:
        return 2
    else:
        return 3
    

func_udf3 = udf(func_trends_mask, IntegerType())

main_df = main_df.withColumn('level_trends_mask',func_udf3(main_df.mask))

#----------------------------------------------------------------------------------------------------------------------

def func_trends_face_mask(face_mask):
    if float(face_mask) <= 2.0:
        return 1
    elif float(face_mask) > 2.0 and float(face_mask) <= 5.0:
        return 2
    else:
        return 3
    

func_udf4 = udf(func_trends_face_mask, IntegerType())

main_df = main_df.withColumn('level_trends_face_mask',func_udf4(main_df.face_mask))

#----------------------------------------------------------------------------------------------------------------------

def func_trends_covid_19(covid_19):
    if float(covid_19) <= 3.0:
        return 1
    elif float(covid_19) > 3.0 and float(covid_19) <= 4.0:
        return 2
    else:
        return 3
    

func_udf5 = udf(func_trends_covid_19, IntegerType())

main_df = main_df.withColumn('level_trends_covid_19',func_udf5(main_df.covid_19))

#----------------------------------------------------------------------------------------------------------------------

def func_trends_coronavirus(coronavirus):
    if float(coronavirus) <= 77.0:
        return 1
    elif float(coronavirus) > 77.0 and float(coronavirus) <= 85.0:
        return 2
    else:
        return 3
    

func_udf6 = udf(func_trends_coronavirus, IntegerType())

main_df = main_df.withColumn('level_trends_coronavirus',func_udf6(main_df.coronavirus))

#----------------------------------------------------------------------------------------------------------------------

def func_trends_sanitizer(sanitizer):
    if float(sanitizer) <= 1.0:
        return 1
    elif float(sanitizer) > 1.0 and float(sanitizer) <= 2.0:
        return 2
    else:
        return 3
    

func_udf7 = udf(func_trends_sanitizer, IntegerType())

main_df = main_df.withColumn('level_trends_sanitizer',func_udf7(main_df.sanitizer))

#----------------------------------------------------------------------------------------------------------------------

def func_air_avg(air_avg):
    if float(air_avg) <= 32.0:
        return 1
    elif float(air_avg) > 32.0 and float(air_avg) <= 48.0:
        return 2
    else:
        return 3
    

func_udf8 = udf(func_air_avg, IntegerType())

main_df = main_df.withColumn('level_air_avg',func_udf8(main_df.Air_value_avg_perday))

#----------------------------------------------------------------------------------------------------------------------



all_level = main_df.select('Province_State', 'level_mr', 'level_ICU_per_60', 'level_trends_mask','level_trends_face_mask','level_trends_covid_19','level_trends_coronavirus','level_trends_sanitizer','level_air_avg')

#all_level.show(300)
main_df.coalesce(1)\
         .write\
         .option("header", "true")\
         .csv("C:/Users/ED/Desktop/covid/COVID-19/Date/main_df_with_level.csv")

def build_WOE_table(first_column, second_column):
    
    first_1_second_1 = all_level.filter(first_column == 1)\
                              .filter(second_column == 1)\
                              .count()
    first_1_second_2 = all_level.filter(first_column == 1)\
                              .filter(second_column == 2)\
                              .count()
    first_1_second_3 = all_level.filter(first_column == 1)\
                              .filter(second_column == 3)\
                              .count() 
    first_2_second_1 = all_level.filter(first_column == 2)\
                              .filter(second_column == 1)\
                              .count() 
    first_2_second_2 = all_level.filter(first_column == 2)\
                              .filter(second_column == 2)\
                              .count() 
    first_2_second_3 = all_level.filter(first_column == 2)\
                              .filter(second_column == 3)\
                              .count() 


                              
    WOE_table = sc.parallelize([Row(Count = first_1_second_1 + first_2_second_1, Goods = first_1_second_1, Bads = first_2_second_1),
                                   Row(Count = first_1_second_2 + first_2_second_2, Goods = first_1_second_2, Bads = first_2_second_2),
                                   Row(Count = first_1_second_3 + first_2_second_3, Goods = first_1_second_3, Bads = first_2_second_3)
                                 ])
    
    WOE_table = WOE_table.toDF()
    
    
    num_1 = WOE_table.groupBy().sum().collect()[0][0]
    num_2 = WOE_table.groupBy().sum().collect()[0][1]
    num_3 = WOE_table.groupBy().sum().collect()[0][2]
    #print(num_1)
    #print(num_2)
    #print(num_3)
              
    WOE_table = WOE_table.withColumn("Distribution_of_Count", WOE_table.Count/ num_2)\
                         .withColumn("Distribution_of_Goods", WOE_table.Goods/ num_3)\
                         .withColumn("Distribution_of_Bads", WOE_table.Bads/ num_1)\
                         .withColumn("Odds", WOE_table.Goods/ WOE_table.Bads)\
                         
    WOE_table = WOE_table.withColumn("Goods_divide_Bads", WOE_table.Distribution_of_Goods / WOE_table.Distribution_of_Bads)
    WOE_table = WOE_table.withColumn("WOE", log(col("Goods_divide_Bads")))
 
    return WOE_table    
       
'''

print("WOE_ICU")       
WOE_ICU = build_WOE_table (all_level.level_mr, all_level.level_ICU_per_60)               
WOE_ICU.show()

print(" ")
print("WOE_trends_mask")
WOE_trends_mask = build_WOE_table (all_level.level_mr, all_level.level_trends_mask)               
WOE_trends_mask.show()

print(" ")
print("WOE_trends_face_mask")
WOE_trends_face_mask = build_WOE_table (all_level.level_mr, all_level.level_trends_face_mask)               
WOE_trends_face_mask.show()

print(" ")
print("WOE_trends_covid_19")
WOE_trends_covid_19 = build_WOE_table (all_level.level_mr, all_level.level_trends_covid_19)               
WOE_trends_covid_19.show()

print(" ")
print("WOE_trends_coronavirus")
WOE_trends_coronavirus = build_WOE_table (all_level.level_mr, all_level.level_trends_coronavirus)               
WOE_trends_coronavirus.show()

print(" ")
print("WOE_trends_sanitizer")
WOE_trends_sanitizer = build_WOE_table (all_level.level_mr, all_level.level_trends_sanitizer)               
WOE_trends_sanitizer.show()

print(" ")
print("WOE_air_avg")
WOE_air_avg = build_WOE_table (all_level.level_mr, all_level.level_air_avg)               
WOE_air_avg.show()



WOE_ICU.coalesce(1)\
        .write\
        .option("header", "true")\
        .csv("WOE_ICU.csv")

WOE_trends_mask.coalesce(1)\
        .write\
        .option("header", "true")\
        .csv("WOE_trends_mask.csv")
        
WOE_trends_face_mask.coalesce(1)\
        .write\
        .option("header", "true")\
        .csv("WOE_trends_face_mask.csv")
        
WOE_trends_covid_19.coalesce(1)\
        .write\
        .option("header", "true")\
        .csv("WOE_trends_covid_19.csv")
        
WOE_trends_coronavirus.coalesce(1)\
        .write\
        .option("header", "true")\
        .csv("WOE_trends_coronavirus.csv")
        
WOE_trends_sanitizer.coalesce(1)\
        .write\
        .option("header", "true")\
        .csv("WOE_trends_sanitizer.csv")

WOE_air_avg.coalesce(1)\
        .write\
        .option("header", "true")\
        .csv("WOE_air_avg.csv")
'''