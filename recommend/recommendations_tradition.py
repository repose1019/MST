# !/usr/bin/env
# -*-coding:utf-8-*-
# @author: 'Repose'
# @date: 2017/6/22


"""
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
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.mllib.recommendation import *
from pandas import DataFrame
from scipy.stats.stats import pearsonr
from multiprocessing.pool import ThreadPool

class KKL_Recommend(object):
    def __init__(self,sc,df,col1, col2, col3,simNum,proNum):
        self.df = df
        self.col1 = col1
        self.col2 = col2
        self.col3 = col3
        self.simNum = int(simNum)
        self.proNum = int(proNum)
        self.sc = sc

    @staticmethod
    def sim_pearson(df, col1, col2, col3, p1, p2):
        """
        :param df: 数组
        :param col1, col2, col3: col1 用户变量, col2目标变量, col3 得分变量
        :param p1, p2: 传入的两个参数
        :return: 关联度
        """
        rating_p1 = df.filter(df[col1] == p1)
        rating_p2 = df.filter(df[col1] == p2)
        prefs_p1 = rating_p1.toPandas()
        prefs_p2 = rating_p2.toPandas()

        si = []
        for item in prefs_p1[col2].as_matrix():
            if item in prefs_p2[col2].as_matrix():
                si.append(item)
        si = DataFrame(si, columns=[col2])

        if len(si) == 0:
            return -1

        ps1 = prefs_p1.merge(si, on=col2)[col3].as_matrix()
        ps2 = prefs_p2.merge(si, on=col2)[col3].as_matrix()

        if len(ps1)>1 and len(ps2)>1:
            r = pearsonr(ps1.toPandas().as_matrix(),ps2.toPandas().as_matrix())
            return r[0]
        else:return -1

    def topMatches(self, itemPrefs, p1, similarity):
        scores = []
        pool = ThreadPool(processes=self.proNum)
        for other in itemPrefs:
            p1_col1 = self.df.filter(self.df[self.col1]== other[0]).select('grade','subjectId').collect()[0]
            p2_col1 = self.df.filter(self.df[self.col1]== p1).select('grade','subjectId').collect()[0]
            if other[0] != p1 and p1_col1 == p2_col1:
                # print("p1_col1,p2_col1:",p1,other[0],p1_col1,p2_col1)
                scores.append(pool.apply_async(similarity,args=(self.df, self.col1, self.col2, self.col3, p1, other[0])))
                # scores.append(similarity(self.df, self.col1, self.col2, self.col3, p1, other[0]))
        return scores

    def calculateSimilarItems(self):
        result = {}
        itemPrefs = self.df.select(self.col1).distinct().collect()
        c = 0
        for item in itemPrefs:
            # Status updates for large datasets
            c += 1
            if c % 100 == 0: print("%d / %d" % (c, len(itemPrefs)))
            # Find the most similar items to this one
            scores = self.topMatches(itemPrefs, item[0],  similarity=self.sim_pearson)
            scores.sort()
            scores.reverse()
            result[item[0]] = scores[0:self.simNum]
        return result

    def getRecommendedItems(self,user):
        userRatings = self.df.filter(self.df[self.col2]==user).select(self.col1,self.col3).toPandas().as_matrix()
        scores = {}
        totalSim = {}
        itemMatch = self.calculateSimilarItems()
        # Loop over items rated by this user
        for rowi in userRatings.items():
            item, rating = rowi[0],rowi[1]
            # Loop over items similar to this one
            for (similarity, item2) in itemMatch[item]:

                # Ignore if this user has already rated this item
                if item2 in userRatings[:,0]: continue
                # Weighted sum of rating times similarity
                scores.setdefault(item2, 0)
                scores[item2] += similarity * rating
                # Sum of all the similarities
                totalSim.setdefault(item2, 0)
                totalSim[item2] += similarity

        # Divide each total score by total weighting to get an average
        rankings = [(score / totalSim[item], item) for item, score in scores.items()]

        # Return the rankings from highest to lowest
        rankings.sort()
        rankings.reverse()
        return rankings

    def Model_ALS(self):
        result= {}
        gradeData = self.df.select("grade").distinct().collect()
        subjectData = self.df.select("subjectId").distinct().collect()
        for grade_i in gradeData:
            for subject_j in subjectData:
                df_ij = self.df.filter((self.df["grade"]== grade_i[0])&(self.df["subjectId"]==subject_j[0])).select(self.col1,self.col2,self.col3).collect()
                rdd_ij = self.sc.parallelize(df_ij)
                model_ij = ALS.train(df_ij,rank=self.simNum,lambda_=0.1)
                result_ij = model_ij.predictAll(rdd_ij).collect()
                result["grade"] = grade_i
                result["subjectId"] = subject_j
                result["model"] = result_ij
        return result

if __name__ == '__main__':
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    df = spark.sql("select * from   kkl_dw.dw_kkl_recommend_base_f_1d where day ='2017-07-09' ")
    col1, col2, col3 = 'productid', 'uid', 'completion'

    print(KKL_Recommend(sc,df,col1, col2, col3,simNum=10,proNum=10).calculateSimilarItems())