# -*-coding:utf-8-*- 
# @author: 'Repose' 
# @date: 2017/7/5

import numpy as np
from datetime import datetime,timedelta,date
# timestart = datetime.strptime('2017-07-13', '%Y-%m-%d')
# timeend = datetime.strptime('2017-06-15', '%Y-%m-%d')
# dayNum = (timestart-timeend).days
# print(dayNum)
# a = date.today().replace(day=1)
# print(type(a))
# print(str(a))
# print(date.today()-timedelta(days=1))


# for i in np.arange(0,189,step=7):
#     # print(i)
#     timenow = timestart-timedelta(days=int(i))
#     time7 = timenow-timedelta(days=7)
#     time_now = "'" + str(datetime.strftime(timenow, '%Y-%m-%d')) + "'"
#     time_7 = "'" + str(datetime.strftime(time7, '%Y-%m-%d')) + "'"
#     print(time_7,time_now,timenow.weekday()  )
#

# for i in np.arange(0,dayNum,step=7):
#     timenow = timestart - timedelta(days= int(i))
#     time7ago = timenow - timedelta(days=6)
#     time_now = "'" + str(datetime.strftime(timenow, '%Y-%m-%d')) + "'"
#     time_7ago = "'" + str(datetime.strftime(time7ago, '%Y-%m-%d')) + "'"
#     print(time_7ago,time_now)

# from scipy.stats.stats import pearsonr
# import pandas as pd
# data2 = pd.DataFrame([[1,2,3],[4,5,6]],columns=["a","b","c"])
# print(data2)
# # p1 = data2[:,1]
# # p2 = data2[:,0]
# p1 = data2.iloc[:,0]
# p2 = data2.iloc[:,1]
# print(p1,p2)
# print(pearsonr(p1,p2))

# from datetime import datetime,date,timedelta
# time_now = "'" + str(datetime.strftime(date.today()- timedelta(days=1), '%Y-%m-%d')) + "'"
# print(time_now)

#
# a = '5_3'
# print(a.split('_')[0])
#
# df = [5]
# for i in range(9):
#     df.append(i)
# print(df)
# print(df.append(i ) for i in range(9) )

from datetime import date,timedelta,datetime
timestart = date.today().replace(day=1)
for i in range(6):
    timenow = (timestart - timedelta(20)).replace(day=1)
    time_now = "'" + str(timenow) + "'"
    print(time_now)
    timestart = timenow


# print( datetime.strptime(str(date.today()-timedelta(days=1)), '%Y-%m-%d'))
# print(date.today().month)
