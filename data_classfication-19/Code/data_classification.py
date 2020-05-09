
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 22 01:08:14 2020

@author: Moon
"""
import csv
import sys
import os
import pickle

# Open Spark

spark_home = os.environ.get('SPARK_HOME', None)
if not spark_home:
    raise ValueError('SPARK_HOME environment variable is not set')
sys.path.insert(0, os.path.join(spark_home, 'python'))
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.10.8.1-src.zip'))
comm = os.path.join(spark_home, 'python/lib/py4j-0.10.8.1-src.zip')
print('start spark....', comm)
exec(open(os.path.join(spark_home, 'python/pyspark/shell.py')).read())

from pyspark.sql.functions import lit
from pyspark.sql.functions import col, expr, when

# 讀檔 確診資料
# Path
data_confirmed = spark.read\
                      .format("csv")\
                      .option("header", "true")\
                      .load("C:/Users/ED/Desktop/covid/COVID-19/Date/time_series_covid19_confirmed_US.csv")

# 讀檔 死亡資料
# Path
data_death = spark.read\
            .format("csv")\
            .option("header", "true")\
            .load("C:/Users/ED/Desktop/covid/COVID-19/Date/time_series_covid19_deaths_US.csv")





# all
def join_every_day(year, month, dayStart, dayEnd, dataframe_for_union):

    for day in range(dayStart, dayEnd+1):
        confirmed_column_name = "sum(" + str(year) + "/" + str(month) + "/" + str(day)+")"
        #print("confirmed_column_name = ", confirmed_column_name)
        data_confirmed_temp = data_confirmed.groupBy("Province_State")\
                                            .agg({str(year) + "/" + str(month) + "/" + str(day): "sum"})\
                                            .withColumnRenamed(confirmed_column_name, "confirmed")
        death_column_name = "sum(" + str(year) + "/" + str(month) + "/" + str(day)+")"
        #print("death_column_name = ", death_column_name)
        data_death_temp = data_death.groupBy("Province_State_Death")\
                                    .agg({str(year) + "/" + str(month) + "/" + str(day): "sum"})\
                                    .withColumnRenamed(death_column_name, "deaths")
        data_join_temp = data_confirmed_temp.join(data_death_temp, data_confirmed_temp.Province_State == data_death_temp.Province_State_Death)

        data_join_add_date = data_join_temp.withColumn("Date", lit(str(year) + "/" + str(month) + "/" + str(day)))

        dataframe_for_union = dataframe_for_union.union(data_join_add_date)

    return dataframe_for_union



def join_first_day(f_year, f_month, f_day):

    # confirmed
    confirmed_first_day = data_confirmed.groupBy("Province_State")\
                                        .agg({str(f_year) + "/" + str(f_month) + "/" + str(f_day):"sum"})\
                                        .withColumnRenamed("sum(" + str(f_year) + "/" + str(f_month) + "/" + str(f_day)+")", "confirmed")
    #confirmed_first_day.show()


    # deaths

    death_first_day = data_death.groupBy("Province_State_Death")\
                                .agg({str(f_year) + "/" + str(f_month) + "/" + str(f_day):"sum"})\
                               .withColumnRenamed("sum(" + str(f_year) + "/" + str(f_month) + "/" + str(f_day)+")", "deaths")
    #death_first_day.show()


    # 合併 first day 的 confirmed、deaths
    data_join_first_day = confirmed_first_day.join(death_first_day, confirmed_first_day.Province_State == death_first_day.Province_State_Death)

    data_first_one = data_join_first_day.withColumn("Date", lit(str(f_year) + "/" + str(f_month) + "/" + str(f_day)))

    return data_first_one


# ex: 2020/1/15 ~ 2020/1/28 ==> join_every_day(2020,1,16,28, join_first_day(2020,1,15))
January = join_every_day(2020,1,23,31, join_first_day(2020,1,22))
February = join_every_day(2020,2,2,29, join_first_day(2020,2,1))
March = join_every_day(2020,3,2,31, join_first_day(2020,3,1))
April = join_every_day(2020,4,2,20,join_first_day(2020,4,1))

# 把重複的欄位刪掉
January = January.drop("Province_State_Death")
February = February.drop("Province_State_Death")
March = March.drop("Province_State_Death")
April = April.drop("Province_State_Death")


# 讀檔 icu資料
# Path
data_icu_beds = spark.read\
                      .format("csv")\
                      .option("header", "true")\
                      .load("C:/Users/ED/Desktop/covid/COVID-19/Date/data-FPBfZ.csv")

# 取 ICU Beds
sum_icu_beds = data_icu_beds.groupBy("State")\
                              .agg({"ICU Beds":"sum"})\
                              .withColumnRenamed("sum(ICU Beds)", "ICU_Beds")
# 取 Total Population
sum_total_population = data_icu_beds.groupBy("State")\
                                    .agg({"Total Population":"sum"})\
                                    .withColumnRenamed("sum(Total Population)", "Total_Population")

sum_Population_Aged_60_plus = data_icu_beds.groupBy("State")\
                                           .agg({"Population Aged 60+":"sum"})\
                                           .withColumnRenamed("sum(Population Aged 60+)", "Population_Aged_60_plus")

# Path
data_latitude = spark.read\
                      .format("csv")\
                      .option("header", "true")\
                      .load("C:/Users/ED/Desktop/covid/COVID-19/Date/usa_states_latitude.csv")


data_latitude = data_latitude.select("usa_state", "usa_state_latitude")
data_latitude = data_latitude.filter(data_latitude["usa_state"] != "Puerto Rico")  #刪除 Puerto Rico
#data_latitude.show(60)


def drop_state (month):
    # 把重複的欄位刪掉
    month = month.join(sum_icu_beds, month.Province_State == sum_icu_beds.State)

    month = month.drop("State")

    month = month.join(sum_total_population, month.Province_State == sum_total_population.State)

    month = month.drop("State")

    month = month.join(sum_Population_Aged_60_plus, month.Province_State == sum_Population_Aged_60_plus.State)

    month = month.drop("State")

    month = month.join(data_latitude, month.Province_State == data_latitude.usa_state)

    month = month.drop("usa_state")

    return month


January = drop_state(January)
February = drop_state(February)
March = drop_state(March)
April = drop_state(April)


'''
print("show January, "+str(1)+"月")
January.show(60)

print("show February "+str(2)+"月")
February.show(60)

print("show March "+str(3)+"月")
March.show(60)
'''
# 合併每個月份
month_all = January.union(February)
month_all = month_all.union(March)
month_all = month_all.union(April)

# 增加兩個column，分別為死亡率及ICU病床除以60歲以上的人口
new_column_func = when(col("confirmed") == 0.0, 0.0).otherwise(month_all.deaths/month_all.confirmed)

month_all = month_all.withColumn("Mortality_Rate", new_column_func)

month_all = month_all.withColumn("Residents_Aged_60_plus_Per_Each_ICU_Bed", month_all.Population_Aged_60_plus/month_all.ICU_Beds)

#month_all.show(300)

#-------------------------------------------------------------------------------------------------------------------------------

# 讀取 grocery_visits、hospital_visits、usa_states_latitude
# Path
data_counties = spark.read\
                     .format("csv")\
                     .option("header", "true")\
                     .load("C:/Users/ED/Desktop/covid/COVID-19/Date/counties.csv")

data_grocery_visits = spark.read\
                           .format("csv")\
                           .option("header", "true")\
                           .load("C:/Users/ED/Desktop/covid/COVID-19/Date/grocery_visits.csv")

data_hospital_visits = spark.read\
                           .format("csv")\
                           .option("header", "true")\
                           .load("C:/Users/ED/Desktop/covid/COVID-19/Date/hospital_visits.csv")

usa_states_latitude = spark.read\
                           .format("csv")\
                           .option("header", "true")\
                           .load("C:/Users/ED/Desktop/covid/COVID-19/Date/usa_states_latitude.csv")
                                 
# 以 FIPS 合併檔案
data_counties = data_counties.select("FIPS", "State")


data_grocery_visits = data_counties.join(data_grocery_visits, data_counties.FIPS == data_grocery_visits.FIPS_G)

data_grocery_visits = data_grocery_visits.drop("FIPS_G")

data_hospital_visits = data_counties.join(data_hospital_visits, data_counties.FIPS == data_hospital_visits.FIPS_H)

data_hospital_visits = data_hospital_visits.drop("FIPS_H")

data_hospital_visits = data_hospital_visits.withColumnRenamed("State", "State_H")

#data_grocery_visits.show()

#data_hospital_visits.show()


def join_every_day_GH(year, month, dayStart, dayEnd, dataframe_for_union):

    for day in range(dayStart, dayEnd+1):
        grocery_column_name = "sum(" + str(year) + "/" + str(month) + "/" + str(day)+")"

        data_grocery_temp = data_grocery_visits.groupBy("State")\
                                               .agg({str(year) + "/" + str(month) + "/" + str(day): "sum"})\
                                               .withColumnRenamed(grocery_column_name, "grocery_visit")
        hospital_column_name = "sum(" + str(year) + "/" + str(month) + "/" + str(day)+")"

        data_hospital_temp = data_hospital_visits.groupBy("State_H")\
                                                 .agg({str(year) + "/" + str(month) + "/" + str(day): "sum"})\
                                                 .withColumnRenamed(hospital_column_name, "hospital_visit")
        data_join_temp = data_grocery_temp.join(data_hospital_temp, data_grocery_temp.State == data_hospital_temp.State_H)

        data_join_add_date = data_join_temp.withColumn("Date", lit(str(year) + "/" + str(month) + "/" + str(day)))

        dataframe_for_union = dataframe_for_union.union(data_join_add_date)

    return dataframe_for_union



def join_first_day_GH(f_year, f_month, f_day):

    # grocery_visits
    grocery_first_day = data_grocery_visits.groupBy("State")\
                                           .agg({str(f_year) + "/" + str(f_month) + "/" + str(f_day):"sum"})\
                                           .withColumnRenamed("sum(" + str(f_year) + "/" + str(f_month) + "/" + str(f_day)+")", "grocery_visit")
    #confirmed_first_day.show()


    # hospital_visit

    hospital_visit_first_day = data_hospital_visits.groupBy("State_H")\
                                                   .agg({str(f_year) + "/" + str(f_month) + "/" + str(f_day):"sum"})\
                                                   .withColumnRenamed("sum(" + str(f_year) + "/" + str(f_month) + "/" + str(f_day)+")", "hospital_visit")
    #death_first_day.show()


    # 合併 first day 的 grocery_visits、hospital_visit
    data_join_first_day = grocery_first_day.join(hospital_visit_first_day, grocery_first_day.State == hospital_visit_first_day.State_H)

    data_first_one = data_join_first_day.withColumn("Date_GH", lit(str(f_year) + "/" + str(f_month) + "/" + str(f_day)))

    return data_first_one

# 只有 2020/3/1 ~ 2020/3/21
March_GH = join_every_day_GH(2020,3,2,21, join_first_day_GH(2020,3,1))
March_GH = March_GH.drop("State_H")


# 把州的簡稱、全名合併
usa_states_latitude = usa_states_latitude.select("usa_state_code", "usa_state")
March_GH = usa_states_latitude.join(March_GH, usa_states_latitude.usa_state_code == March_GH.State)
March_GH = March_GH.drop("State", "usa_state_code") #,"usa_state_code"
#March_GH = March_GH.withColumnRenamed("usa_state_code", "State_short")


# all
Mix = month_all.join(March_GH, [month_all.Province_State == March_GH.usa_state, month_all.Date == March_GH.Date_GH], how='inner')
Mix = month_all.drop("Date_GH")







# Path
trends_by_day = spark.read\
                     .format("csv")\
                     .option("header", "true")\
                     .load("C:/Users/Moon/Desktop/trends_by_day.csv")
                           


Mix_all = Mix.join(trends_by_day, [Mix.Province_State == trends_by_day.geoName, Mix.Date == trends_by_day.date_trends], how='inner')

Mix_all = Mix_all.drop("geoName")

Mix_all = Mix_all.drop("date_trends")



Mix_all = Mix_all.join(usa_states_latitude, Mix_all.Province_State == usa_states_latitude.usa_state)

Mix_all = Mix_all.drop("usa_state")

Mix_all.show(300)



# Path
airavg = spark.read\
                     .format("csv")\
                     .option("header", "true")\
                     .load("C:/Users/Moon/Desktop/mix_three_avg.csv")

Mix_all = Mix_all.join(airavg, [Mix_all.usa_state_code == airavg.Air_state, Mix_all.Date == airavg.Air_date], how='inner')

Mix_all = Mix_all.drop("Air_state")

Mix_all = Mix_all.drop("Air_date")

Mix_all.show(300)




Mix_all.coalesce(1)\
        .write\
        .option("header", "true")\
        .csv("main_df.csv")

















