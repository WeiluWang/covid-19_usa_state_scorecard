# -*- coding: utf-8 -*-
"""
Created on Sat May  2 01:07:45 2020

@author: Moon
"""

import csv


# read data
with open('C:/Users/ED/Desktop/covid/COVID-19/Date/list_state.csv', newline='') as csvfile:
    rows = csv.reader(csvfile)
    for list_state in rows:
        print(list_state)


with open('C:/Users/ED/Desktop/covid/COVID-19/Date/list_date.csv', newline='') as csvfile:
    rows = csv.reader(csvfile)
    for list_date in rows:
        print(list_date)
        
with open('C:/Users/ED/Desktop/covid/COVID-19/Date/la.csv', newline='') as csvfile:
    rows = csv.reader(csvfile)
    for la in rows:
        print(la)






mix_three = []



for i in range(len(list_state)):
    for j in range(118*i, 118*i+118):
        mix_three.append([list_state[i], list_date[j], la[j]])


# write data
with open('C:/Users/ED/Desktop/covid/COVID-19/Date/mix_three.csv', 'w', newline='') as csvfile:
    writer  = csv.writer(csvfile)
    for row in mix_three:
        writer.writerow(row)