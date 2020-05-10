#!/usr/bin/env python
# coding: utf-8

# In[7]:


import numpy as np
import matplotlib
matplotlib.use('Agg')

import sys

if len(sys.argv) > 2:
    ratio = float(sys.argv[2])
else:
    ratio = 0.8
if len(sys.argv) > 1:
    cut_ratio = float(sys.argv[1])
else:
    cut_ratio = 0.3

if not (0 <= cut_ratio and cut_ratio < ratio and ratio < 1.0):
    cut_ratio, ratio = 0.3, 0.8

print({"Abandoned ratio: ":cut_ratio, "Training set ratio":(ratio-cut_ratio), "Testing set ratio":1.0-ratio})

f = open('C:\\Users\\ED\\Documents\\covid\\predict\\data2.csv')
title = f.readline().rstrip().split(',')
f.close()

data = np.loadtxt('C:\\Users\\ED\\Documents\\covid\\predict\\data2.csv', delimiter = ",", skiprows = 1)
# print(title)


# In[8]:


X, Y = data[:,2:], data[:,1] # binned features  (column9: latitude not included)

features = title[2:]
# print(features)

state = np.array([[i%49] for i in range(90*49)])
date = np.array([[i//49] for i in range(90*49)])

X = np.column_stack((X, state, date))


# In[40]:


import matplotlib.pyplot as plt  
from matplotlib import font_manager

accurate_list1 = [np.sum(Y[i*49:i*49+49]==1) for i in range(90)] # accurate counts of class1
accurate_list2 = [np.sum(Y[i*49:i*49+49]==2) for i in range(90)] # accurate counts of class2


# In[62]:


def build_dates(startdate, days=90):
    dates = []
    year, month, day = startdate.split('/')
    bigmonths = [1,3,5,7,8,10,12]
    littlemonths = [4,6,9,11]
    for i in range(days):
        if int(month) in bigmonths and int(day)+1 > 31             or int(month) in littlemonths and int(day)+1 > 30             or int(month) == 2 and int(day)+1 > 29:
            day = '01'
            if month == '12':
                month = '01'
                year = str(int(year)+1)
            else:
                month = '%02d' % (int(month)+1)
        else:
            day = '%02d' % (int(day)+1)
        dates += year+'/'+month+'/'+day,
    return dates


# In[72]:
def info(clf):

    coef_list = clf.coef_[0]

    print(features)
    print(coef_list)

    coefs = {}
    for i,f in enumerate(features):
        coefs[f] = coef_list[i]

    print(coefs)


    print("\nInterception: "+str(clf.intercept_))

    print("\nAccuracy: "+str(clf.score(x_test, y_label_test)))

    prediction = clf.predict(np.array(x_test))

    print("\nactual\tpredicted")
    print(np.sum(y_label_test==1), "\t", np.sum(prediction==1))
    print(np.sum(y_label_test==2), "\t", np.sum(prediction==2))

    return prediction

def plot(prediction, days):
    predict_list1 = [np.sum(prediction[i*49:i*49+49]==1) for i in range(days)]
    predict_list2 = [np.sum(prediction[i*49:i*49+49]==2) for i in range(days)]

    bar_width = 0.15

    dates = build_dates('2020/01/22',90)[-days:]
    x_a_1 = list(range(days))
    x_p_1 = [i+bar_width for i in x_a_1]
    x_middle = [i+bar_width for i in x_p_1]
    x_a_2 = [i+bar_width for i in x_middle]
    x_p_2 = [i+bar_width for i in x_a_2]

    plt.figure(figsize=(40, 8), dpi=80)

    plt.bar(x_a_1, accurate_list1[-days:], width=bar_width, label='<0.01')
    plt.bar(x_p_1, predict_list1, width=bar_width, label='predicted <0.01')
    plt.bar(x_a_2, accurate_list2[-days:], width=bar_width, label='>=0.01')
    plt.bar(x_p_2, predict_list2, width=bar_width, label='predicted >=0.01')

    plt.title('Title')
    plt.xlabel('Date')
    plt.ylabel('Count of Class')
    plt.xticks(x_middle, dates)
    plt.legend()

    plt.show(block=True)


# In[88]:


def split_train_test(data,test_ratio):
    np.random.seed(42)
    shuffled_indices = np.random.permutation(len(data))
    test_set_size = int(len(data) * test_ratio)
    test_indices = shuffled_indices[:test_set_size]
    train_indices = shuffled_indices[test_set_size:]
    return train_indices, test_indices


# ratio = 0.8 # training set ratio

'''split randomly
'''

# train_indices, test_indices = split_train_test(X, 1-ratio)

# x_train, x_test = X[train_indices], X[test_indices]
# y_label_train, y_label_test = Y[train_indices], Y[test_indices]


'''split by time point
'''

split = int(90*ratio)

# x_train, x_test = X[:split*49], X[split*49:]
# y_label_train, y_label_test = Y[:split*49], Y[split*49:]

cut = int(90*cut_ratio)
x_train, x_test = X[cut*49:split*49], X[split*49:]
y_label_train, y_label_test = Y[cut*49:split*49], Y[split*49:]


# In[89]:


from sklearn.svm import LinearSVC

clf = LinearSVC(C=1.0, class_weight='balanced', random_state=1, max_iter=10000)         .fit(x_train, y_label_train)


print('Linear SVM\n')


prediction = info(clf)
    
plot(prediction, 90-split)


# In[90]:


from sklearn.datasets import load_iris
from sklearn.linear_model import LogisticRegression

clf = LogisticRegression(C=1.0,class_weight='balanced',random_state=7, max_iter=10000).fit(x_train, y_label_train)

print('Logistic Regression\n')

prediction = info(clf)

    
plot(prediction, 90-split)

# In[ ]:



