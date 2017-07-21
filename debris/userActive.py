# -*-coding:utf-8-*- 
# __author__ = 'Repose' 
# 2017/6/16


import pandas as pd
import numpy as np
from pandas import DataFrame
from sklearn.datasets import make_hastie_10_2
from sklearn.ensemble import GradientBoostingClassifier



def readFile(file):
    a = open(str(file),'r')
    tmp = []
    for line in a.readlines():
        col = line.replace('\\N','0').strip().split('\001')
        tmp.append(col)
    a.close()
    user_active = pd.DataFrame(tmp,columns=['uid','create_date','status','is_card','last_time','create_days','login_days','pv','lesson_num','duration_num','topic_posting_num','topic_up_num','topic_reply_num','talk_posting_num','talk_up_num','talk_reply_num','circlepv'])
    user_active = user_active[['status','create_days','login_days','pv','lesson_num','duration_num','topic_posting_num','topic_up_num','topic_reply_num','talk_posting_num','talk_up_num','talk_reply_num','circlepv','is_card']]
    for coli in user_active.columns:
        user_active[coli] = user_active[coli].astype(int)
    return user_active.as_matrix()

def autoNorm(dataSet):
    '''
    Args: 归一化处理
    :param dataSet: 需要归一化处理的数组
    :return: normDataSet,归一化后的数组. ranges:最大差值.  minVals,最小值数组
    '''
    if len(dataSet) >1:
        minVals = dataSet.min(0)
        maxVals = dataSet.max(0)
        ranges = maxVals - minVals
        normDataSet = np.zeros(np.shape(dataSet))
        normDataSet = dataSet - minVals
        normDataSet = normDataSet / ranges  # element wise divide
        # normDataSet = array([normDataSet]).reshape(-1, 1)
    else : normDataSet = dataSet
    return normDataSet

def GBDT(data):
    X = autoNorm(data[:,:-1])
    y = data[:,-1]
    rowNum = len(data)
    trainNum = int(rowNum*0.8)
    trainRowN = np.random.choice(rowNum, trainNum, replace=False)
    textRowN = list(set([i for i in range(rowNum)]) - set(trainRowN))
    # print(trainRowN,textRowN)
    X_train = X[trainRowN,:]
    X_test = X[textRowN,:]
    y_train = y[trainRowN]
    y_test = y[textRowN]

    gbdt = GradientBoostingClassifier(n_estimators=100, learning_rate=1.0,
        max_depth=1, random_state=0)
    clf = gbdt.fit(X_train, y_train)
    print(clf.score(X_test, y_test))
    imp_sort = gbdt.feature_importances_
    data_imp_sort = {}
    colname = ['status','create_days','login_days','pv','lesson_num','duration_num','topic_posting_num','topic_up_num','topic_reply_num','talk_posting_num','talk_up_num','talk_reply_num','circlepv']
    for i,namei in enumerate(colname):
        data_imp_sort[namei] = imp_sort[i]
    return data_imp_sort



if __name__ == '__main__':
    file = 'D:/Users/MST/Desktop/000000_0'
    print(readFile(file)[:5,:])
    print(GBDT(readFile(file)))

