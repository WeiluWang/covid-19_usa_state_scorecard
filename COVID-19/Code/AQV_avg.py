# -*- coding: utf-8 -*-


import sys
import os
import pickle
import csv



list_data = []

with open('C:/Users/ED/Desktop/covid/COVID-19/Date/mix_three.csv', newline='') as csvfile:
    rows = csv.reader(csvfile)
    for list_state in rows:
        print(list_state)
        list_data.append(list_state)



list_A = []
count = 0
temp_total = 0



for i in range(len(list_data)):

    if list_data[i][0] != temp_state or i == len(list_data)-1:
        if count == 0:
            list_A.append([list_data[i-1][0], list_data[i-1][1], "0"])
        else:
            list_A.append([list_data[i-1][0], list_data[i-1][1], str(round(temp_total / count, 1))])
        count = 0
        temp_total = 0

        temp_state = list_data[i][0]
    if list_data[i][2].isdigit() and list_data[i][2] != "0":   
        count = count + 1
        temp_total = temp_total + int(list_data[i][2])
    


with open('C:/Users/ED/Desktop/covid/COVID-19/Date/airavg.csv', 'w', newline='') as csvfile:
    writer  = csv.writer(csvfile)
    for row in list_A:
        writer.writerow(row)
        
        
        
        
        
        
        

