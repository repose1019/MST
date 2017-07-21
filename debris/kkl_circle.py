# -*-coding:utf-8-*- 
# @author: 'Repose' 
# @date: 2017/6/16


import pandas as pd
import mpl_toolkits.mplot3d
import matplotlib.pyplot as plt

data = pd.read_excel('E:/kkl_files/circle_test_20170616.xlsx')
data = data[(data.diffdays>0) & (data.day_durationnum >0) & (data.diffdays< 9999)]
# data = data[(data.day_durationnum >0) & (data.diffdays== 9999)]

x = data.diffdays
y = data.day_lessonnum
z = data.day_durationnum

ax=plt.subplot(111,projection='3d')
ax.scatter(x,y,z,c='y')
ax.set_xlabel("X")
ax.set_ylabel("Y")
ax.set_zlabel("Z")
ax.set_zlabel('day_durationnum') #坐标轴
ax.set_ylabel('day_lessonnum')
ax.set_xlabel('diffdays')
plt.show()

