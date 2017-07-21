# -*-coding:utf-8-*- 
# @author: 'Repose' 
# @date: 2017/7/5


from datetime import datetime, timedelta, date
import numpy as np
from pyspark.sql import SparkSession
import subprocess

"spark.sql 中 to_date() 的输出是 datetime.date(7,1,2016)的格式 ,得换成cast(to_date() as string)"

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
timestart = datetime.strptime(str(date.today()-timedelta(days=1)), '%Y-%m-%d')
timeend = datetime.strptime('2017-01-01', '%Y-%m-%d')
dayNum = int((timestart-timeend).days)
data = spark.sql('''
    SELECT distinct 
    case when c.term_id=1 then '七年级上'
      when c.term_id=2 then '七年级下'
      when c.term_id=3 then '八年级上'
      when c.term_id=4 then '八年级下'
      when c.term_id=5 then '九年级上'
      when c.term_id=6 then '九年级下'
      when c.term_id=7 then '中考复习'
      when c.term_id=8 then '初升高衔接'
      end class_Name
    ,c.kemu_name, c.course_id, cast(to_date(c.course_updatetime) as string) course_updatetime ,c.day
    from kkl_dw.dw_kkl_course_info_f_1d  c
    where c.play_type =1 
    and c.day = '''+"'" +str(date.today()-timedelta(days=1))+"'" +'''
    and c.term_id >=1 and c.term_id <= 8
    and c.commodity_status = 1
    ''')
data.registerTempTable('table_c')
total_courseNum = spark.sql('''
    select
    count(distinct c.course_id)   total_courseNum
    from kkl_dw.dw_kkl_course_info_f_1d c 
    where c.play_type =1 
    and c.term_id >=1 and c.term_id <= 8
	and c.commodity_status = 1
    and c.day = '''+"'" +str(date.today()-timedelta(days=1))+"'" +'''
    ''').collect()[0][0]
for i in np.arange(dayNum+1):
    timenow = timestart + timedelta(days=- int(i))
    time_now = "'" + str(datetime.strftime(timenow, '%Y-%m-%d')) + "'"
    print(time_now)

    spark.sql('''
    insert overwrite table  kkl_rpt.rpt_kkl_recorded_f_1d partition(day = ''' +time_now+ ''')

    select
    ''' +time_now+ '''	as mathtime
    ,t1.class_Name
    ,t1.kemu_name
    ,t1.courseNum
    ,t1.new_courseNum
    ,t1.view_courseNum
    ,round(100*t1.courseNum/''' +str(total_courseNum)+ ''',2)  courseNum_rate
    ,t1.userNum
    ,round(t1.total_duration/60)  total_duration
    ,round(t1.avg_duration/60,2)  avg_duration
    ,t1.uid_courseNum
    ,t1.uid_lessonNum
    
    from(
        select 
        c.day
        ,c.class_Name
        ,c.kemu_name	
        ,count(distinct c.course_id)   courseNum
        ,count(distinct case when c.course_updatetime = ''' +time_now+ ''' then c.course_id  end)  new_courseNum
        ,count(distinct case when ml.course_id is not null then ml.course_id  end ) view_courseNum
        ,count(distinct ml.uid )    userNum
        ,nvl(sum(ml.duration ),0)         total_duration
        ,nvl(sum(ml.duration ) /count(distinct ml.uid ),0)  avg_duration
        ,count(distinct ml.course_id,ml.uid)      uid_courseNum
        ,count(distinct ml.lesson_id ,ml.uid)		uid_lessonNum
        from table_c as c 
    
        left join (select ml.uid,ml.course_id, ml.duration,ml.lesson_id
            from kkl_dw.dw_kkl_course_i_1d ml 
            where ml.day =  ''' +time_now+ ''' 
            ) ml
        on c.course_id = ml.course_id 
    
        group by c.class_Name,c.kemu_name ,c.day	
    
        union all
    
        select 
        c.day
        ,c.class_Name
        ,'合计'  kemu_name	
        ,count(distinct c.course_id)   courseNum
        ,count(distinct case when to_date(c.course_updatetime) = ''' +time_now+ ''' then c.course_id  end)  new_courseNum
        ,count(distinct case when ml.course_id is not null then ml.course_id  end ) view_courseNum
        ,count(distinct ml.uid )    userNum
        ,nvl(sum(ml.duration ),0)         total_duration
        ,nvl(sum(ml.duration ) /count(distinct ml.uid ),0)  avg_duration
        ,count(distinct ml.course_id,ml.uid)      uid_courseNum
        ,count(distinct ml.lesson_id ,ml.uid)		uid_lessonNum
    
        from table_c as c 
    
        left join (select ml.uid,ml.course_id, ml.duration,ml.lesson_id 
            from kkl_dw.dw_kkl_course_i_1d ml 
            where ml.day =  ''' +time_now+ ''' 
            ) ml
        on c.course_id = ml.course_id 
    
        group by c.day,c.class_Name
    
        union all
    
        select 
        c.day
        ,'合计' as class_Name
        ,c.kemu_name	
        ,count(distinct c.course_id)   courseNum
        ,count(distinct case when to_date(c.course_updatetime) = ''' +time_now+ ''' then c.course_id  end)  new_courseNum
        ,count(distinct case when ml.course_id is not null then ml.course_id  end ) view_courseNum
        ,count(distinct ml.uid )    userNum
        ,nvl(sum(ml.duration ),0)         total_duration
        ,nvl(sum(ml.duration ) /count(distinct ml.uid ),0)  avg_duration
        ,count(distinct ml.course_id,ml.uid)      uid_courseNum
        ,count(distinct ml.lesson_id ,ml.uid)		uid_lessonNum
    
        from table_c as c 
    
        left join (select ml.uid,ml.course_id, ml.duration,ml.lesson_id 
            from kkl_dw.dw_kkl_course_i_1d ml 
            where ml.day =  ''' +time_now+ ''' 
            ) ml
        on c.course_id = ml.course_id     
        group by c.kemu_name,c.day
    
        ) t1
                     ''')

# 删分区
# for i in range(dayNum):
#     timenow = timestart + timedelta(days=- i)
#     time_now = str(datetime.strftime(timenow, '%Y-%m-%d'))
#     # print(time_now)
#     subprocess.call(
#         ["hadoop", "fs", "-rm", "-r", "/user/hive/warehouse/kkl_rpt.db/rpt_kkl_recorded_f_1d/day=" + time_now])

