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
from pyspark.sql.functions import col,corr
from pyspark.context import SparkContext
from pyspark.mllib.recommendation import *
from datetime import  datetime,date,timedelta
from multiprocessing.pool import ThreadPool
# from concurrent.futures import ThreadPoolExecutor


spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext

class KKL_Similar(object):
    def __init__(self,df,col1, col2, col3,simNum,proNum,saveModel = False):
        """
        :param df: 推荐系统底层数据
        :param col1:  对象1
        :param col2:  对象2
        :param col3:  评分依据(观看完成度,点赞,星级)
        :param simNum:  相似个数
        :param proNum:   进程并发数
        :param saveModel:   是否保存输出到 hive
        """
        self.col1 = col1
        self.col2 = col2
        self.col3 = col3
        self.simNum = int(simNum)
        self.proNum = int(proNum)
        self.df = df
        self.saveModel = bool(saveModel)

    @staticmethod
    def Model_pearson(df, col1, col2, col3, p1, p2):

        rating_p1 = df.filter(df[col1] == p1).select(col(col2).alias(str("p1_"+col2)),col(col3).alias(str("p1_"+col3)))
        rating_p2 = df.filter(df[col1] == p2).select(col(col2).alias(str("p2_"+col2)),col(col3).alias(str("p2_"+col3)))
        sameDF = rating_p1.join(rating_p2,rating_p1[str("p1_"+col2)] == rating_p2[str("p2_"+col2)]).select(str("p1_"+col3),str("p2_"+col3))
        # print(sameDF.count())
        if sameDF.count() > 1 :
            pearDF = sameDF.agg(corr(str("p1_"+col3),str("p2_"+col3)).alias("sim"))
            r = pearDF.collect()[0][0]
            print("pearson:", p1, p2,r)
            return r
        elif sameDF.count() == 1:
            return abs(sameDF.first()[0]-sameDF.first()[1])
        else:return -1

    def topMatches(self, p1):
        result={}
        p1_identical = self.df.filter(self.df[self.col1] == p1).select("identical").first()[0]
        other_df = self.df.filter((self.df[self.col1] != p1)&(self.df.identical == p1_identical))
        other_names=other_df.select(other_df[self.col1]).distinct().collect()
        # print("p1,simDF:",p1,other_names)
        '''并发'''
        poolResult = []
        pool = ThreadPool(processes=self.proNum)
        for other in other_names:
            # scores.append(similarity(self.df, self.col1, self.col2, self.col3, p1, other[0]))
            poolResult.append(pool.apply_async(
                self.Model_pearson,args=(self.df, self.col1, self.col2, self.col3, p1, other[0])))
        pool.close()
        pool.join()
        scores = [t.get() for t in poolResult]
        scores.sort()
        scores.reverse()
        result[p1] = scores[0:self.simNum]
        return result

    def getSimilarItems_CORR(self):
        """"
        :return:  物品相似度模型,离线
        """
        result = {}
        poolResult_cal = []
        pool_cal = ThreadPool(processes=self.proNum)
        itemPrefs = self.df.select(self.col1).distinct().collect()
        for item in itemPrefs:
            # Find the most similar items to this one
            # scores = self.topMatches(item[0],  similarity=KKL_Recommend.sim_pearson)
            poolResult_cal.append(pool_cal.apply_async(self.topMatches,args=(self,item[0])))
        pool_cal.close()
        pool_cal.join()
        for pooi in poolResult_cal:
            result = dict(result, ** pooi.get())

        if self.saveModel:
            try:
                with open("model_CORR", 'w') as df_file:
                    print(result, file=df_file)
                    df_file.close()
                    print("write in !")
            except IOError:
                print("file error")

        return result

    def getRecommended_CORR(self,user,itemMatch):
        """
        :param user:
        :param itemMatch:  self.getSimilarItems_CORR (已离线运行完)
        :return:
        """
        userRatings = self.df.filter(self.df[self.col2]==user).select(self.col1,self.col3).distinct()
        scores = {}
        totalSim = {}
        # Loop over items rated by this user
        for rowi in userRatings.collect():
            item, rating = rowi[0],rowi[1]
            # Loop over items similar to this one
            for (similarity, item2) in itemMatch[item]:

                # if item2 in userRatings[:,0]: continue
                if userRatings[userRatings[self.col1].isin([item2])]:continue
                # Weighted sum of rating times similarity
                scores.setdefault(item2, 0)
                scores[item2] += similarity * rating
                # Sum of all the similarities
                totalSim.setdefault(item2, 0)
                totalSim[item2] += similarity

        # Divide each total score by total weighting to get an average
        rankings = [(score / totalSim[item], item) for item, score in scores.items()]

        rankings.sort()
        rankings.reverse()
        return rankings

    @staticmethod
    def Model_ALS(df,col1,col2,col3,simNum,ident_i,saveModel):
        """
        :param ident_i: 'grade' +'subjectId'
        :return:  'grade', 'subjectId'
        """
        predict_ij = predictDF.filter(predictDF.identical== ident_i)
        if predict_ij.count() >3:
            # print("start")
            df_ij = df.filter(df.identical== ident_i).select(col1,col2,col3)
            model_ij = ALS.train(df_ij,rank=simNum,lambda_=0.1)
            if saveModel:
                try:
                    model_ij.save(sc,path="/var/lib/hadoop-hdfs/workpath/excute/model/"+bizdate+"/"+str(col3)+"_"+str(ident_i))
                except:pass
            #  训练的Rdd 由 tmp_kkl_model_recommend_Train 决定
            result_ij = model_ij.predictAll(predict_ij.select(col1,col2).rdd)
            try:
                # print(result_ij.toDF().first())
                result_ij = result_ij.zipWithIndex().map(lambda x: (x[0][0],x[0][1],str(col3),x[0][2],ident_i.split('_')[0],ident_i.split('_')[1]))
                result_ij = result_ij.toDF([col1,col2,'ratingName','rating','grade','subjectId'])
                return result_ij
            except:
                return spark.createDataFrame([[-1, -1, col3, -1, -1, -1]],
                                             schema=[col1, col2, 'ratingName', 'rating', 'grade', 'subjectId'])
        else:
            return spark.createDataFrame([[-1,-1,col3,-1,-1,-1]],schema=[col1,col2,'ratingName','rating','grade','subjectId'])

    def getSimilarItems_ALS(self):
        result= spark.createDataFrame([[-1,-1,str(self.col3),-1,-1,-1]],schema=[self.col1,self.col2,'ratingName','rating','grade','subjectId'])
        identical = self.df.select("identical").distinct().collect()

        poolALS = ThreadPool(processes=self.proNum)
        poolResult_ALS = [poolALS.apply_async(self.Model_ALS,args=(self.df,self.col1,self.col2,self.col3,self.simNum,ident_i[0],self.saveModel))
                          for ident_i in identical if ident_i[0] != None  ]

        # poolResult_ALS = []
        # for ident_i in identical:
            # result = result.union(self.Model_ALS(ident_i[0]))
            # poolResult_ALS.append(poolALS.apply_async(
            #     self.Model_ALS,args=(self.df,self.col1,self.col2,self.col3,self.simNum,ident_i[0])))

        poolALS.close()
        poolALS.join()

        for pool_i in poolResult_ALS:
            result = result.union(pool_i.get())

        return result



class KKL_Recommend(object):
    def __init__(self,col1, col2, col3_list,simNum=10,proNum=20,saveModel = False):
        self.col1 = col1
        self.col2 = col2
        self.col3_list = col3_list
        self.simNum = int(simNum)
        self.proNum = int(proNum)
        self.saveModel = bool(saveModel)

    def runFuncALS(self):
        for col3_ in self.col3_list:
            df = spark.sql(""" select distinct productid,uid,""" + col3_ + """
                    ,concat(grade,"_",subjectId) identical  
                    from   kkl_dw.dw_kkl_recommend_base_f_1d 
                    where """ + col3_ + """ is not null and day ='"""
                    + bizdate + "'")
            result_col3 = KKL_Similar(df, self.col1, self.col2, col3_, self.simNum, self.proNum, self.saveModel).getSimilarItems_ALS()
            result_col3.registerTempTable("modelDF")
            if  result_col3.count()>3:
                spark.sql("insert  " + ("overwrite table" if col3_==self.col3_list[0] else "into") +
                """ dm.dm_kkl_model_ALS
                select uid,productid,ratingName,rating,grade,subjectId
                from modelDF
                where uid> 0 
                """)
                print(col3_+" Model data write in.")

    def getRecommendedALS(self):
        try:
            df = spark.sql("""
            insert overwrite table  dm.dm_kkl_model_recommend_first
            select 
            ma.uid,ma.productid,ma.grade,ma.subjectId
            ,nvl(sum(ma.completeRating),0) as completeRating
            ,nvl(sum(ma.commentRating),0) as commentRating
            ,nvl(sum(ma.starsRating),0) as starsRating
            from(select distinct
                ma.uid,ma.productid,ma.grade,ma.subjectId
                ,case when ma.ratingName = 'completion' then ma.rating end completeRating
                ,case when ma.ratingName = 'iscomment' then ma.rating end commentRating
                ,case when ma.ratingName = 'stars' then ma.rating end starsRating
                from dm.dm_kkl_model_ALS  ma
                where not exists (select rb.uid,rb.productid 
                    from kkl_dw.dw_kkl_recommend_base_f_1d  rb 
                    where rb.uid = ma.uid   and ma.productid = rb.productid   
                    and rb.day ='""" + bizdate + """')
            ) ma 
            group by ma.uid,ma.productid,ma.grade,ma.subjectId
            """
            )
            if df.count() > 2:
                print("tmp_kkl_model_recommend_first write in !")
        except :
            print("tmp_kkl_model_recommend_first write false !")



if __name__ == '__main__':
    bizdate = str(date.today() - timedelta(days=1))
    col1 = 'uid'
    col2 = 'productid'
    col3_list = ['completion', 'stars']
    predictDF = spark.sql("""select distinct t.uid,t.productid,concat(grade,"_",subjectId) identical  from dm.dm_kkl_model_recommend_predict t """)
    Rec_KKL_ = KKL_Recommend( col1, col2, col3_list, simNum=10, proNum=20, saveModel=True)
    Rec_KKL_.runFuncALS()
    # Rec_KKL_.getRecommendedALS()


    # spark.sql(""" select sum() from kkl_dw.dw_kkl_recommend_base_f_1d """).sort(col("grade").desc())





