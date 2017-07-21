# -*-coding:utf-8-*- 
# @author: 'Repose' 
# @date: 2017/7/5


from datetime import datetime, timedelta, date
import numpy as np
from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
timestart = datetime.strptime('2017-07-12', '%Y-%m-%d')
timeend = datetime.strptime('2017-06-15', '%Y-%m-%d')
dayNum = int((timestart-timeend).days)

for i in np.arange(dayNum+1):
    timenow = timestart - timedelta(days= int(i))
    time_now = "'" + str(datetime.strftime(timenow, '%Y-%m-%d')) + "'"
    print(time_now)

    spark.sql('''
        insert overwrite table kkl_rpt.rpt_kkl_model_rec_i_d partition(day = ''' +time_now+ ''')
        select 
        rec.day bizdate
        ,avg(rec_pv_rate)			rec_pv_rate
        ,avg(sce_pv_rate)			sce_pv_rate
        ,avg(rec_course_rate)		rec_course_rate
        ,avg(sce_course_rate)		sce_course_rate
        ,avg(rec_duration_rate)		rec_duration_rate
        ,avg(sce_duration_rate)		sce_duration_rate
        ,avg(Recall_Rate) 			rec_Recall_Rate
        ,avg((1+1)*rec_course_rate*Recall_Rate/(1*rec_course_rate+Recall_Rate))	 rec_Measure 	
        from kkl_dw.dw_kkl_evaluation_recommend_i_1d  rec 
        join  kkl_dw.dw_kkl_scene_evaluation_course_i_1d sce 
        on rec.day = sce.day
        where rec.day = ''' +time_now+ '''
        and sce.day = ''' +time_now+ '''
        group by rec.day 
                     ''')

# 删分区
# for i in range(dayNum):
#     timenow = timestart + timedelta(days=- i)
#     time_now = str(datetime.strftime(timenow, '%Y-%m-%d'))
#     # print(time_now)
#     subprocess.call(
#         ["hadoop", "fs", "-rm", "-r", "/user/hive/warehouse/kkl_rpt.db/rpt_kkl_recorded_f_1d/day=" + time_now])

