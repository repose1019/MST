###first table
from pyspark.sql import SQLContext,Row
ccdata=sc.textFile("/home/srtest/spark/spark-1.3.1/examples/src/main/resources/cc.txt")
ccpart = ccdata.map(lambda le:le.split(","))  ##我的表是以逗号做出分隔
cc1=ccpart.map(lambda p:Row(sid=p[0],age=int(p[1]),yz=p[2],yf=p[3],yc=p[4],hf=p[5],hk=p[6])) ####这就是将数据变成ROW的格式，另外确定数据类型
schemacc1=sqlContext.createDataFrame(cc1)#######源码中createDataframe(ROW,schema),所以如果上步没有转化成ROW是无法完成转化成dataframe
schemacc1.registerTempTable("cc1")#############注册临时表
xx=sqlContext.sql(" SELECT * FROM cc1 WHERE age=20 ") ########直接用写sql就能实现表的运算



from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("PythonSQL")\
#     .config("spark.some.config.option", "some-value")\
#     .getOrCreate()
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

teenagers = spark.sql("SELECT * FROM kkl_ods.dw_kkl_app_log_decoded where day = '2017-06-25' limit 10")
print(teenagers.show())






# from datetime import datetime,timedelta,date
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.enableHiveSupport().getOrCreate()
# for i in range(13):
#     timenow = datetime.strptime('2017-06-15', '%Y-%m-%d')  + timedelta(days = i)
#     time_now = "'"+str(datetime.strftime(timenow, '%Y-%m-%d'))+"'"
# 	timenow = str(date.today()+ timedelta(days = -1))
# 	time_now = "'"+timenow+"'"
# 	print(time_now)
# 	data = spark.sql('''
#
#
#
#
#
#
# 		''')
#
# 	data.registerTempTable("kkl_APP_Browse_course_i_1d")
# 	spark.sql('''
# 		insert overwrite table kkl_dw.kkl_APP_Browse_course_i_1d
# 		SELECT * FROM kkl_APP_Browse_course_i_1d
#
# 		'''  )