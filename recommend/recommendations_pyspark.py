# !/usr/bin/env
# -*-coding:utf-8-*-
# @author: 'Repose'
# @date: 2017/6/22

critics = {'Lisa Rose': {'Lady in the Water': 2.5, 'Snakes on a Plane': 3.5,
                         'Just My Luck': 3.0, 'Superman Returns': 3.5, 'You, Me and Dupree': 2.5,
                         'The Night Listener': 3.0},
           'Gene Seymour': {'Lady in the Water': 3.0, 'Snakes on a Plane': 3.5,
                            'Just My Luck': 1.5, 'Superman Returns': 5.0, 'The Night Listener': 3.0,
                            'You, Me and Dupree': 3.5},
           'Michael Phillips': {'Lady in the Water': 2.5, 'Snakes on a Plane': 3.0,
                                'Superman Returns': 3.5, 'The Night Listener': 4.0},
           'Claudia Puig': {'Snakes on a Plane': 3.5, 'Just My Luck': 3.0,
                            'The Night Listener': 4.5, 'Superman Returns': 4.0,
                            'You, Me and Dupree': 2.5},
           'Mick LaSalle': {'Lady in the Water': 3.0, 'Snakes on a Plane': 4.0,
                            'Just My Luck': 2.0, 'Superman Returns': 3.0, 'The Night Listener': 3.0,
                            'You, Me and Dupree': 2.0},
           'Jack Matthews': {'Lady in the Water': 3.0, 'Snakes on a Plane': 4.0,
                             'The Night Listener': 3.0, 'Superman Returns': 5.0, 'You, Me and Dupree': 3.5},
           'Toby': {'Snakes on a Plane': 4.5, 'You, Me and Dupree': 1.0, 'Superman Returns': 4.0}}

'''
+-----+---------+--------------------+
|  uid|productid|          completion|
+-----+---------+--------------------+
|10092|      613|5.307855626326964E-4|
|10092|      954|0.002920962199312715|
|10098|     1327|  0.2885057471264368|
|10098|     1334|0.011471861471861472|
|10098|     1338|                 0.0|
|10098|     1393|0.005263157894736842|
+-----+---------+--------------------+
'''
from math import *
from pyspark.sql import SparkSession
from pandas import DataFrame
from scipy.stats.stats import pearsonr
from multiprocessing.pool import ThreadPool

class KKL_Recommend(object):


def sim_pearson(df, col1, col2, col3, p1, p2):
    """
    :param df: 数组
    :param col1, col2, col3: col1 用户变量, col2目标变量, col3 得分变量
    :param p1, p2: 传入的两个参数
    :return: 关联度
    """
    rating_p1 = df.filter(df[col1] == p1)[col1, col2, col3]
    rating_p2 = df.filter(df[col1] == p2)[col1, col2, col3]
    prefs_p1 = rating_p1.toPandas()
    prefs_p2 = rating_p2.toPandas()

    si = []
    for item in prefs_p1[col2].as_matrix():
        if item in prefs_p2[col2].as_matrix():
            si.append(item)
    si = DataFrame(si, columns=[col2])

    if len(si) == 0:
        return 0

    ps1 = prefs_p1.merge(si, on=col2)[col3].as_matrix()
    ps2 = prefs_p2.merge(si, on=col2)[col3].as_matrix()
    if len(ps1)>1 and len(ps2)>1:
        r = pearsonr(ps1,ps2)
        return r[0]
    else:return -1

def topMatches(df, itemPrefs, col1, col2, col3, p1, similarity=sim_pearson):
    scores = []
    for other in itemPrefs:
        if other[0] != p1 and df.filter(df[col1]== other[0]).collect()[0][0] == df.filter(df[col1]== p1).collect()[0][0]:
            scores.append(similarity(df, col1, col2, col3, p1, other[0]))
    return scores

def calculateSimilarItems(df,col1, col2, col3, n=10):
    result = {}
    itemPrefs = df.select(col1).distinct().collect()
    c = 0
    for item in itemPrefs:
        # Status updates for large datasets
        c += 1
        if c % 100 == 0: print("%d / %d" % (c, len(itemPrefs)))
        # Find the most similar items to this one
        scores = topMatches(df,itemPrefs,col1, col2, col3, item[0],  similarity=sim_pearson)
        scores.sort()
        scores.reverse()
        result[item[0]] = scores[0:n]
    return result

if __name__ == '__main__':
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    df = spark.sql("select * from   kkl_dw.dw_kkl_recommend_base_f_1d where day ='2017-07-05' limit 100")
    col1, col2, col3 = 'productid', 'uid', 'completion'
    print(calculateSimilarItems(df,col1, col2, col3))