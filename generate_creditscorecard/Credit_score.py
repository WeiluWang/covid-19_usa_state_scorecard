import os
import sys

# Open Spark

spark_home = os.environ.get('SPARK_HOME', None)
if not spark_home:
    raise ValueError('SPARK_HOME environment variable is not set')
sys.path.insert(0, os.path.join(spark_home, 'python'))
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.10.8.1-src.zip'))
comm = os.path.join(spark_home, 'python/lib/py4j-0.10.8.1-src.zip')
print('start spark....', comm)
exec(open(os.path.join(spark_home, 'python/pyspark/shell.py')).read())

from pyspark.sql import Row
from pyspark.sql import functions as F
sqlContext = SQLContext(sc)

WOE_ICU = spark.read\
               .format("csv")\
               .option("header", "true")\
               .load("C:/Users/ED/Documents/covid/data_classfication-19/Date/WOE_ICU.csv")

WOE_trends_mask = spark.read\
                       .format("csv")\
                       .option("header", "true")\
                       .load("C:/Users/ED/Documents/covid/data_classfication-19/Date/WOE_trends_mask.csv")
                       
WOE_trends_face_mask = spark.read\
                            .format("csv")\
                            .option("header", "true")\
                            .load("C:/Users/ED/Documents/covid/data_classfication-19/Date/WOE_trends_face_mask.csv")                       

WOE_trends_covid_19 = spark.read\
                           .format("csv")\
                           .option("header", "true")\
                           .load("C:/Users/ED/Documents/covid/data_classfication-19/Date/WOE_trends_covid_19.csv")
                           
WOE_trends_coronavirus = spark.read\
                           .format("csv")\
                           .option("header", "true")\
                           .load("C:/Users/ED/Documents/covid/data_classfication-19/Date/WOE_trends_coronavirus.csv")
                           
WOE_trends_sanitizer = spark.read\
                            .format("csv")\
                            .option("header", "true")\
                            .load("C:/Users/ED/Documents/covid/data_classfication-19/Date/WOE_trends_sanitizer.csv")
                           
WOE_air_avg = spark.read\
                   .format("csv")\
                   .option("header", "true")\
                   .load("C:/Users/ED/Documents/covid/data_classfication-19/Date/WOE_air_avg.csv")
                   

#Beta = Beta_0 = Beta_1 = Beta_2 = Beta_3 = Beta_4 = Beta_5 = Beta_6 = Beta_7 = 1

def count_credit_score(Data, Beta_0, Beta_1):
    credit_score = 0
    credit_score = ((float(Data)*Beta_1)+Beta_0) * 115.41 + 148.5
    
    return credit_score


credit_score_ICU = []
credit_score_trends_mask = []
credit_score_trends_face_mask = []
credit_score_trends_covid_19 = []
credit_score_trends_coronavirus = []
credit_score_trends_sanitizer = []
credit_score_air_avg = []



# Wait this
credit_score_ICU_Beta = []
credit_score_trends_mask_Beta = []
credit_score_trends_face_mask_Beta = []
credit_score_trends_covid_19_Beta = []
credit_score_trends_coronavirus_Beta = []
credit_score_trends_sanitizer_Beta = []
credit_score_air_avg_Beta = []

'''
for i in range(0, 3):
    credit_score_ICU.append(count_credit_score(WOE_ICU.collect()[i][8], credit_score_ICU_Beta[i], credit_score_ICU_Beta[i+3]))
    credit_score_trends_mask.append(count_credit_score(WOE_trends_mask.collect()[i][8], credit_score_trends_mask_Beta[i], credit_score_trends_mask_Beta[i+3]))
    credit_score_trends_face_mask.append(count_credit_score(WOE_trends_face_mask.collect()[i][8], credit_score_trends_face_mask_Beta[i], credit_score_trends_face_mask_Beta[i+3]))
    credit_score_trends_covid_19.append(count_credit_score(WOE_trends_covid_19.collect()[i][8], credit_score_trends_covid_19_Beta[i], credit_score_trends_covid_19_Beta[i+3]))
    credit_score_trends_coronavirus.append(count_credit_score(WOE_trends_coronavirus.collect()[i][8], credit_score_trends_coronavirus_Beta[i], credit_score_trends_coronavirus_Beta[i+3]))
    credit_score_trends_sanitizer.append(count_credit_score(WOE_trends_sanitizer.collect()[i][8], credit_score_trends_sanitizer_Beta[i], credit_score_trends_sanitizer_Beta[i+3]))
    credit_score_air_avg.append(count_credit_score(WOE_air_avg.collect()[i][8], credit_score_air_avg_Beta[i], credit_score_air_avg_Beta[i+3]))
'''

for i in range(0, 3):
    credit_score_ICU.append(count_credit_score(WOE_ICU.collect()[i][8], -0.01161306, -0.0019276020458007983))
    credit_score_trends_mask.append(count_credit_score(WOE_trends_mask.collect()[i][8], -0.01161306,  0.12154219147248996))
    credit_score_trends_face_mask.append(count_credit_score(WOE_trends_face_mask.collect()[i][8], -0.01161306, -0.7607582033915038))
    credit_score_trends_covid_19.append(count_credit_score(WOE_trends_covid_19.collect()[i][8],-0.01161306, -0.18961628153937474))
    credit_score_trends_coronavirus.append(count_credit_score(WOE_trends_coronavirus.collect()[i][8], -0.01161306, -0.06276454502412158))
    credit_score_trends_sanitizer.append(count_credit_score(WOE_trends_sanitizer.collect()[i][8], -0.01161306, 0.036090109535216006))
    credit_score_air_avg.append(count_credit_score(WOE_air_avg.collect()[i][8], -0.01161306,  -0.03141952587420972))





credit_score_table = sc.parallelize([Row(
                                         Characteristic_Name = "ICU_Level_1",
                                         Point = credit_score_ICU[0],
                                        ),
                                     Row(
                                         Characteristic_Name = "ICU_Level_2",
                                         Point = credit_score_ICU[1],
                                        ),
                                     Row(
                                         Characteristic_Name = "ICU_Level_3",
                                         Point = credit_score_ICU[2],
                                        ),
                                     Row(
                                         Characteristic_Name = "trends_mask_Level_1",
                                         Point = credit_score_trends_mask[0],
                                        ),
                                     Row(
                                         Characteristic_Name = "trends_mask_Level_2",
                                         Point = credit_score_trends_mask[1],
                                        ),
                                     Row(
                                         Characteristic_Name = "trends_mask_Level_3",
                                         Point = credit_score_trends_mask[2],
                                        ),
                                     Row(
                                         Characteristic_Name = "trends_face_mask_Level_1",
                                         Point = credit_score_trends_face_mask[0],
                                        ),
                                     Row(
                                         Characteristic_Name = "trends_face_mask_Level_2",
                                         Point = credit_score_trends_face_mask[1],
                                        ),
                                     Row(
                                         Characteristic_Name = "trends_face_mask_Level_3",
                                         Point = credit_score_trends_face_mask[2],
                                        ),
                                     Row(
                                         Characteristic_Name = "trends_covid_19_Level_1",
                                         Point = credit_score_trends_covid_19[0],
                                        ),
                                     Row(
                                         Characteristic_Name = "trends_covid_19_Level_2",
                                         Point = credit_score_trends_covid_19[1],
                                        ),
                                     Row(
                                         Characteristic_Name = "trends_covid_19_Level_3",
                                         Point = credit_score_trends_covid_19[2],
                                        ),
                                     Row(
                                         Characteristic_Name = "trends_coronavirus_Level_1",
                                         Point = credit_score_trends_coronavirus[0],
                                        ),
                                     Row(
                                         Characteristic_Name = "trends_coronavirus_Level_2",
                                         Point = credit_score_trends_coronavirus[1],
                                        ),
                                     Row(
                                         Characteristic_Name = "trends_coronavirus_Level_3",
                                         Point = credit_score_trends_coronavirus[2],
                                        ),
                                     Row(
                                         Characteristic_Name = "trends_sanitizer_Level_1",
                                         Point = credit_score_trends_sanitizer[0],
                                        ),
                                     Row(
                                         Characteristic_Name = "trends_sanitizer_Level_2",
                                         Point = credit_score_trends_sanitizer[1],
                                        ),
                                     Row(
                                         Characteristic_Name = "trends_sanitizer_Level_3",
                                         Point = credit_score_trends_sanitizer[2],
                                        ),
                                     Row(
                                         Characteristic_Name = "trends_ir_avg_Level_1",
                                         Point = credit_score_air_avg[0],
                                        ),
                                     Row(
                                         Characteristic_Name = "trends_ir_avg_Level_2",
                                         Point = credit_score_air_avg[1],
                                        ),
                                     Row(
                                         Characteristic_Name = "trends_ir_avg_Level_3",
                                         Point = credit_score_air_avg[2],
                                        )
                                     ])
        
credit_score_table = credit_score_table.toDF()

credit_score_table = credit_score_table.select("Characteristic_Name", F.bround("Point", scale=2).alias("Point"))

credit_score_table.show(30, truncate = False)

'''
credit_score_table.coalesce(1)\
                  .write\
                  .option("header", "true")\
                  .csv("C:/Users/ED/Documents/covid/generate_creditscorecard/credit_score_table.csv")'''