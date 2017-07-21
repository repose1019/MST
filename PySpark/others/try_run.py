# !/usr/bin/env
# -*-coding:utf-8-*-
# @author: 'Repose' 
# @date: 2017/6/22

# from pyspark.sql import SparkSession
# # spark = SparkSession.builder.appName("PythonSQL")\
# #     .config("spark.some.config.option", "some-value")\
# #     .getOrCreate()
# spark = SparkSession.builder.enableHiveSupport().getOrCreate()
# teenagers = spark.sql("SELECT * FROM tmp.tmp_kkl_model_recommended_assess limit 10")
# print(teenagers.show())
# # for each in teenagers.collect():
# #     print(each[0])
# # spark.stop()

from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

df = spark.sql("select * from   kkl_dw.dw_kkl_recommend_base_f_1d where day ='2017-07-05' ")
users = df.select('uid').distinct().collect()
critics = {}
for uid in users:
    critics['uid'] = uid[0]
    critics['uid'][0]



rating = df.select('teacherid', 'uid', 'completion').collect()[1][1]


# rating = df.filter(df.col1 in (p1,p2))[col1, col2, 'completion']
# prefs = rating.toPandas().to_dict(orient='list')
# for i,j in enumerate
# # si = {}
# # for item in rating_p1.select('productid').collect():
# #     if item in rating_p2.select('productid').collect():
# #         si[item[0]] = 1