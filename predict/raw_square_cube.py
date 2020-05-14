#!/usr/bin/env python
# coding: utf-8

# In[33]:


# coding=utf8


import numpy as np
import matplotlib
matplotlib.use('Agg')

import sys

f = open('data.csv')
title = f.readline().strip('\n').split(',')
f.close()
data = np.loadtxt('data.csv', delimiter = ",", skiprows = 1)


# In[34]:


X, Y = data[:,1:], data[:,0]

features = title[1:]

state = np.array([[i%49] for i in range(90*49)])
date = np.array([[i//49] for i in range(90*49)])


Y_label = np.array(Y)
for i,labeli in enumerate(Y_label):
    if labeli < 0.01:
        Y_label[i] = 1
    else:
        Y_label[i] = 2
        
# usa_state_latitude,Residents_Aged_60_plus_Per_Each_ICU_Bed,mask,face mask,covid-19,coronavirus,sanitizer,Air_value_avg_perday
X_square = np.power(X[:,2:7], [2, 2, 2, 2, 2])
X_cube = np.power(X[:,2:7], [3, 3, 3, 3, 3])

X_data = np.column_stack((X, X_square, X_cube, state, date))
features += ['square_'+key for key in features[2:7]]
features += ['cube_'+key for key in features[2:7]]
features += ['state', 'date']
print(features)
# X_data = X


# In[35]:


def split_train_test(data,test_ratio):
    np.random.seed(42)
    shuffled_indices = np.random.permutation(len(data))
    test_set_size = int(len(data) * test_ratio)
    test_indices = shuffled_indices[:test_set_size]
    train_indices = shuffled_indices[test_set_size:]
    return train_indices, test_indices


ratio = 0.8 # training set ratio

'''split randomly
'''

# train_indices, test_indices = split_train_test(X_data, 1-ratio)

# x_train, x_test = X_data[train_indices], X_data[test_indices]
# y_train, y_test = Y[train_indices], Y[test_indices]
# y_label_train, y_label_test = Y_label[train_indices], Y_label[test_indices]


'''split by time point
'''

split = int((len(X_data)//49)*ratio)

x_train, x_test = X_data[:split*49], X_data[split*49:]
y_train, y_test = Y[:split*49], Y[split*49:]
y_label_train, y_label_test = Y_label[:split*49], Y_label[split*49:]


# In[36]:


accurate_list1 = [np.sum(Y_label[i*49:i*49+49]==1) for i in range(90)] # accurate counts of class1
accurate_list2 = [np.sum(Y_label[i*49:i*49+49]==2) for i in range(90)] # accurate counts of class2

def info(clf):

    coef_list = clf.coef_[0]

#     print(features)
#     print(coef_list)

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


# In[47]:


# get_ipython().run_line_magic('matplotlib', 'inline')
import matplotlib.pyplot as plt  
from matplotlib import font_manager

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

def plot(prediction, days):
    predict_list1 = [np.sum(prediction[i*49:i*49+49]==1) for i in range(days)]
    predict_list2 = [np.sum(prediction[i*49:i*49+49]==2) for i in range(days)]

    bar_width = 0.2

    dates = build_dates('2020/01/22',90)[-days:]
    x_a_1 = list(range(days))
    x_p_1 = [i+bar_width for i in x_a_1]
    x_middle = [i+bar_width/3.0 for i in x_p_1]
    x_a_2 = [i+bar_width for i in x_middle]
    x_p_2 = [i+bar_width for i in x_a_2]

    #resolution setting
    plt.figure(figsize=(40, 8), dpi=80)

    #load data
    plt.bar(x_a_1, accurate_list1[-days:], width=bar_width, label='<0.01', color='lime')
    plt.bar(x_a_2, accurate_list2[-days:], width=bar_width, label='≥0.01', color='coral')
    plt.bar(x_p_1, predict_list1, width=bar_width, label='predicted <0.01', color='turquoise')
    plt.bar(x_p_2, predict_list2, width=bar_width, label='predicted ≥0.01', color='violet')

    plt.title('Title')

    plt.xlabel('Date')
    plt.ylabel('Count of Class')

    plt.xticks(x_middle, dates)
    plt.legend()

    plt.show()


# In[48]:


from sklearn.svm import LinearSVC

clf = LinearSVC(C=10, class_weight='balanced', random_state=9)         .fit(x_train, y_label_train)

print('Linear SVM\n')

prediction = info(clf)

plot(prediction, 90-split)


# In[50]:


from sklearn.linear_model import LinearRegression
reg = LinearRegression().fit(x_train, y_train)


print('Linear Regression\n')
print(features)
print(reg.coef_)

print("\nAccuracy: ",reg.score(x_test, y_test))

prediction = reg.predict(np.array(x_test))

print("\nactual\tpredicted")
print(np.sum(y_test<0.01), "\t", np.sum(prediction<0.01))
print(np.sum(y_test>=0.01), "\t", np.sum(prediction>=0.01))


plot(prediction, 90-split)


# In[52]:


from sklearn import tree
clf = tree.DecisionTreeClassifier()
clf = clf.fit(x_train, y_label_train)

print('Decision Tree\n')
print("\nAccuracy: ", clf.score(x_test, y_label_test))

prediction = clf.predict(x_test)

print("\nactual\tpredicted")
print(np.sum(y_label_test==1), "\t", np.sum(prediction==1))
print(np.sum(y_label_test==2), "\t", np.sum(prediction==2))

plot(prediction, 90-split)


# In[53]:


from sklearn.ensemble import RandomForestClassifier
from sklearn.datasets import make_classification

clf = RandomForestClassifier(max_depth=None
                             , random_state=5)

clf.fit(x_train, y_label_train)

print('Random Forest\n')
print("\nAccuracy: ",clf.score(x_test, y_label_test))


prediction = clf.predict(x_test)

print("\nactual\tpredicted")
print(np.sum(y_label_test==1), "\t", np.sum(prediction==1))
print(np.sum(y_label_test==2), "\t", np.sum(prediction==2))

plot(prediction, 90-split)


# In[54]:


from sklearn.datasets import load_iris
from sklearn.linear_model import LogisticRegression

clf = LogisticRegression(C=1.0,class_weight='balanced',random_state=1).fit(x_train, y_label_train)

print('Logistic Regression\n')

prediction = info(clf)
plot(prediction, 90-split)


# In[ ]:




