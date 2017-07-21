# !/usr/bin/env 
# -*-coding:utf-8-*- 
# @author: 'Repose' 
# @date: 2017/7/19


from KKL_Complement import *
from KKL_Recommend_ALS import *

class KKL_result(KKL_Recommend,KKL_complement):
    def __init__(self,col1, col2, col3_list,simNum,proNum,saveModel):
        KKL_Recommend.__init__(self,col1, col2, col3_list,simNum,proNum,saveModel)
        KKL_complement.__init__(self,simNum)



if __name__ == '__main__':
    col1 = 'uid'
    col2 = 'productid'
    col3_list = ['completion', 'stars']
    predictDF = spark.sql("""select distinct t.uid,t.productid,concat(grade,"_",subjectId) identical  from tmp.tmp_kkl_model_recommend_predict t """)
    Rec_KKL_ = KKL_Recommend( col1, col2, col3_list, simNum=10, proNum=20, saveModel=True)
    Rec_KKL_.runFuncALS()
    # Rec_KKL_.getRecommendedALS()
