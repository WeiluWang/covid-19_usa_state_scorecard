Project : covid-19 scorecard of each state in USA

Group member:

name: Weilu Wang netid:wlw273

name: Jingxian Du netid:jd4472

Ref textbook:

CREDIT RISK ANALYTICS 
by 
Bart Baesens
Daniel Rösch
Harald Scheule

Ref websites:

https://github.com/CSSEGISandData/COVID-19

https://www.arcgis.com/apps/opsdashboard/index.html#/bda7594740fd40299423467b48e9ecf6

https://medium.com/henry-jia/how-to-score-your-credit-1c08dd73e2ed

https://medium.com/@yanhuiliu104/credit-scoring-scorecard-development-process-8554c3492b2b

https://towardsdatascience.com/intro-to-credit-scorecard-9afeaaa3725f

https://github.com/GeneralMills/pytrends

Ref papers:

Estimating credit and profit scoring of a Brazilian credit union with logistic regression and machine-learning techniques 
by
Daniel Abreu Vasconcellos de Paula, Rinaldo Artes,
Fabio Ayres and Andrea Maria Accioly Fonseca Minardi
Insper, São Paulo, SP, Brazi

Predicting credit default probabilities using machine learning techniques in the face of unequal class distributions
by
ANNA STELZER from Vienna University of Economics and Business





data classification:


Aqv_crawler.py -> output raw data of air quality data in each state

trends_by_day.py ->output raw data of trends score of each keywords of  each state everyday since 1/20 to 4/21

AQV_combine_three_air_data.py ->join region in to state with timestamp and air quality value

AQV_avg.py ->output average value of each region in each state

Data_classification.py -> join all raw data into a dataframe
 
classify_data_level.py -> output level of each data in each factors and WOE of each level of each factor

Factor_IV.py ->output IV value of each factors

predict.py -> output coeffiecient and interception of SVM and logistic regression 
