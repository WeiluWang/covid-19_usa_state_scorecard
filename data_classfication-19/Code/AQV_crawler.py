
#https://www.epa.gov/outdoor-air-quality-data/air-quality-index-daily-values-report?fbclid=IwAR1NGlXy_gY0GlWYBWEgGHWeEW2LrlruZvrZyARtPSjEbKiyg9eRgJlRUHg
#https://www3.epa.gov/cgi-bin/broker?_service=data&_debug=0&_program=dataprog.ad_rep_aqi_daily_drupal_airnow.sas&querytext=&areaname=&areacontacts=&areasearchurl=&typeofsearch=epa&result_template=2col.ftl&poll=all&year=2020&cbsa=11460&county=-1

import urllib.request
import requests
from bs4 import BeautifulSoup
import pickle
from pyspark.sql import Row
import os
import sys
import csv


spark_home = os.environ.get('SPARK_HOME', None)
if not spark_home:
    raise ValueError('SPARK_HOME environment variable is not set')
sys.path.insert(0, os.path.join(spark_home, 'python'))
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.10.8.1-src.zip'))
comm = os.path.join(spark_home, 'python/lib/py4j-0.10.8.1-src.zip')
print('start spark....', comm)
exec(open(os.path.join(spark_home, 'python/pyspark/shell.py')).read())

sqlContext = SQLContext(sc)

print("123")

l1 = []
list_date=[]
la=[]
lb=[]
r0 = requests.get("https://www3.epa.gov/cgi-bin/broker?_service=data&_program=dataprog.ad_load_cbsas.sas&querytext=&areaname=&areacontacts=&areasearchurl=&typeofsearch=epa&result_template=2col.ftl&poll=all&year=2020")

soup0 = BeautifulSoup(str(r0.text), 'html.parser')
#print(soup0)
results = soup0.find_all('option')
for res in results:
    if(res['value'] != "-1"):
        l1.append(res['value'])
#print(l1)
'''
r1 = requests.get("https://www3.epa.gov/cgi-bin/broker?_service=data&_debug=0&_program=dataprog.ad_rep_aqi_daily_drupal_airnow.sas&querytext=&areaname=&areacontacts=&areasearchurl=&typeofsearch=epa&result_template=2col.ftl&poll=all&year=2020&cbsa="+"10100"+"&county=-1")
soup1 = BeautifulSoup(str(r1.text), 'html.parser')
#print(soup1)
td_tag = soup1.find_all('td')
index = 0
while(index < len(td_tag)):
    #print(index)
    list_date.append(td_tag[index].text)
    la.append(td_tag[index+1].text[len(td_tag[index+1].text)-2]+td_tag[index+1].text[len(td_tag[index+1].text)-1])
    index = index+9
'''

list_state = []

date=['01/01/2020', '01/02/2020', '01/03/2020', '01/04/2020', '01/05/2020', '01/06/2020', '01/07/2020', '01/08/2020', '01/09/2020', '01/10/2020', '01/11/2020', '01/12/2020', '01/13/2020', '01/14/2020', '01/15/2020', '01/16/2020', '01/17/2020', '01/18/2020', '01/19/2020', '01/20/2020', '01/21/2020', '01/22/2020', '01/23/2020', '01/24/2020', '01/25/2020', '01/26/2020', '01/27/2020', '01/28/2020', '01/29/2020', '01/30/2020', '01/31/2020','02/01/2020', '02/02/2020', '02/03/2020', '02/04/2020', '02/05/2020', '02/06/2020', '02/07/2020', '02/08/2020', '02/09/2020', '02/10/2020', '02/11/2020', '02/12/2020', '02/13/2020', '02/14/2020', '02/15/2020', '02/16/2020', '02/17/2020', '02/18/2020', '02/19/2020', '02/20/2020', '02/21/2020', '02/22/2020', '02/23/2020', '02/24/2020', '02/25/2020', '02/26/2020', '02/27/2020', '02/28/2020', '02/29/2020', '03/01/2020', '03/02/2020', '03/03/2020', '03/04/2020', '03/05/2020', '03/06/2020', '03/07/2020', '03/08/2020', '03/09/2020', '03/10/2020', '03/11/2020', '03/12/2020', '03/13/2020', '03/14/2020', '03/15/2020', '03/16/2020', '03/17/2020', '03/18/2020', '03/19/2020', '03/20/2020', '03/21/2020', '03/22/2020', '03/23/2020', '03/24/2020', '03/25/2020', '03/26/2020', '03/27/2020', '03/28/2020', '03/29/2020', '03/30/2020', '03/31/2020', '04/01/2020', '04/02/2020', '04/03/2020', '04/04/2020', '04/05/2020', '04/06/2020', '04/07/2020', '04/08/2020', '04/09/2020', '04/10/2020', '04/11/2020', '04/12/2020', '04/13/2020', '04/14/2020', '04/15/2020', '04/16/2020', '04/17/2020', '04/18/2020', '04/19/2020', '04/20/2020', '04/21/2020', '04/22/2020', '04/23/2020', '04/24/2020', '04/25/2020', '04/26/2020', '04/27/2020']

#  
#  
      
print("len(date) = ", len(date))

#l1 = ["10100", "10140"]



for l in l1:
    r1 = requests.get("https://www3.epa.gov/cgi-bin/broker?_service=data&_debug=0&_program=dataprog.ad_rep_aqi_daily_drupal_airnow.sas&querytext=&areaname=&areacontacts=&areasearchurl=&typeofsearch=epa&result_template=2col.ftl&poll=all&year=2020&cbsa="+l+"&county=-1")
    soup1 = BeautifulSoup(str(r1.text), 'html.parser')
    check = soup1.find('h1')
    if str(check) == "<h1>This request completed with errors.</h1>":
        continue
    region= soup1.find('strong').next_sibling
    region_state = region[len(region)-2]+region[len(region)-1]
    #print(region_state)
    td_tag = soup1.find_all('td')
    index = 0
    list_state.append(region_state)
    date_index = 0
    temp = "0"
    print("l = ", l)
    

    temp_date=[]
    temp_la=[]
    for d in date:
        temp_date.append(d)
        temp_la.append("0")
    
    while(index < len(td_tag)):
        
        if td_tag[index].text in temp_date:
            target = temp_date.index(td_tag[index].text)  
            temp_la[target]=td_tag[index+1].text[len(td_tag[index+1].text)-2]+td_tag[index+1].text[len(td_tag[index+1].text)-1]
            #temp = td_tag[index+1].text[len(td_tag[index+1].text)-2]+td_tag[index+1].text[len(td_tag[index+1].text)-1]
        index = index+9
    
    for d in temp_date:
        list_date.append(d)
    for value in temp_la:
        la.append(value)




#print(list_date)
print("----------region_state----------")
print(region_state)


#print("region_state[0] = ", region_state[0])
#print("type_region_state[0] = ", type(region_state[0]))

print("  ")
print("  ")
print("  ")
print("----------list_date----------")
print(list_date)
print("len(list_date) = ", len(list_date))
#print("type_list_date[0] = ", type(list_date[0]))

print("  ")
print("  ")
print("  ")
print("----------la----------")
print(la)
print("len(la) = ", len(la))
#print("list_date[0] = ", list_date[0])
#print("type_la[0] = ", type(la[0]))
#print("la[0] = ", la[0])

print("  ")
print("  ")
print("  ")
print("----------list_state----------")
print(list_state)
print("len(list_state) = ", len(list_state))




with open('C:/Users/ED/Documents/covid/data_classfication-19/Code/list_state.csv', 'w', newline='') as csvfile:
    writer  = csv.writer(csvfile)
    writer.writerow(list_state)

with open('C:/Users/ED/Documents/covid/data_classfication-19/Code/list_date.csv', 'w', newline='') as csvfile:
    writer  = csv.writer(csvfile)
    writer.writerow(list_date)

with open('C:/Users/ED/Documents/covid/data_classfication-19/Code/la.csv', 'w', newline='') as csvfile:
    writer  = csv.writer(csvfile)
    writer.writerow(la)





