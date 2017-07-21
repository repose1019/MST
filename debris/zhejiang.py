# !/usr/bin/env python
# -*- coding:utf-8 -*-
# auther: repose
import numpy as np
import pandas as pd
import statsmodels.api as sm
import matplotlib.pyplot as plt
from sklearn import linear_model
from sklearn.preprocessing import  PolynomialFeatures

def SM(x,y,piciNum):
    X = sm.add_constant(x)
    X_score = sm.add_constant([i for  i in range(min(x),max(x)+1)])
    if piciNum == 1:
        X = np.column_stack((X, X ** 2))
        X_score = np.column_stack((X_score, X_score ** 2))
    elif   piciNum == 3:
        X = np.column_stack((X, X ** 2,X**3))
        X_score = np.column_stack((X_score, X_score ** 2,X_score ** 3))
    else:  X = X
    results = sm.OLS(y, X).fit()
    y_fitted = results.predict(X_score)
    min_index = np.argmin(y_fitted)
    for i in np.arange(min_index,len(y_fitted)):
        y_fitted[i] = y_fitted[min_index]
    return [i for  i in range(min(x),max(x)+1)], y_fitted, results

def SK(X,y,piciNum):
    regr = linear_model.LinearRegression()
    regr.fit(X.reshape(-1, 1),y)
    a, b = regr.coef_, regr.intercept_
    X_score = np.asanyarray([i for  i in range(min(X),max(X)+1)]).reshape(-1, 1)
    y_fitted = regr.predict(X_score)
    return X_score,y_fitted,regr

def PLT(score_data,piciNum,year,wenli,score=False):

    if piciNum == 1:
        pici = '一批'
    elif piciNum == 2:
        pici = '二批'
    else: pici = '三批'

    first_score_li = min(score_data[(score_data['批次'] == '一批') & (score_data['年份'] == year)&(score_data['科类'] == '理科')]['分数'])
    second_score_li = min(score_data[(score_data['批次'] == '二批') & (score_data['年份'] == year)&(score_data['科类'] == '理科')]['分数'])
    first_score_wen = min(score_data[(score_data['批次'] == '一批') & (score_data['年份'] == year)&(score_data['科类'] == '文科')]['分数'])
    second_score_wen = min(score_data[(score_data['批次'] == '二批') & (score_data['年份'] == year)&(score_data['科类'] == '文科')]['分数'])

    data = score_data[(score_data['批次'] == pici) & (score_data['年份'] == year)]

    if wenli == '理科': 
        first_score = first_score_li
        second_score = second_score_li
    else: 
        first_score = first_score_wen
        second_score = second_score_wen
    
    '理科,文科'
    data = data[data['科类'] == wenli]
    total = max(data['累计人数'])
    if piciNum == 1 :
        data = data.iloc[1:]
    elif   piciNum ==2:
        data = data[data['分数']< first_score]
    else :
        data = data[data['分数'] < second_score]
    y = data['累计人数']/total
    X = data['分数']
    X_score, y_fitted,regr = SM(X,y,piciNum)


    # '文科'
    # data_wen = data[data['科类'] == '文科']
    # total_wen = max(data_wen['累计人数'])
    # if piciNum == 1 :
    #     data_wen = data_wen.iloc[1:]
    # elif   piciNum ==2:
    #     data_wen = data_wen[data_wen['分数']< first_score_wen]
    # else :
    #     data_wen = data_wen[data_wen['分数'] < second_score_wen]
    # y_wen = data_wen['累计人数']/total_wen
    # X_wen = data_wen['分数']
    # X_wen_score, y__wen_fitted,regr_wen = SM(X_wen,y_wen,piciNum)
    if score:
        print("排名人次: ", data[(data['分数']== score)&(data['科类']==wenli)]['累计人数'].iloc[0])
        print("排名比: ",y_fitted[np.where(np.asanyarray(X_score)==int(score))])
    else:
        plt.figure(1)
        # plt.subplot(211)
        plt.scatter(X, y, color='blue')
        plt.plot(X_score, y_fitted, color='red', linewidth=4)
        plt.title(wenli)
        plt.show()



if __name__ == '__main__':
    score_data = pd.read_excel("D:/Users/MST/Desktop/浙江数据/浙江14-16年分数段表.xlsx")

    # print(score_data.head())

    PLT(score_data,1,2015,'理科',score=False)
