# !/usr/bin/env 
# -*-coding:utf-8-*- 
# @author: 'Repose' 
# @date: 2017/7/19

from KKL_Recommend_ALS import *

class KKL_complement(object):
    def __init__(self,simNum):
        self.simNum = int(simNum)

    def tmp_kkl_model_recommend_third(self):
        """
        :return: 根据用户近期行为,做出相应的科目推荐调整
        """
        sql_text =            """
            insert overwrite table dm.dm_kkl_model_recommend_third
            select rf.uid,rf.productid,rf.grade,rf.subjectId,rf.completeRating,rf.commentRating,rf.starsRating,rf.rankNumber
            from dm.dm_kkl_model_recommend_first rf
            join(
                select
                r.uid
                ,r.grade
                ,r.subjectId
                ,ceil(r.productrate / total.productrate * """ + str(self.simNum) + """)  proportion
                from dm.dm_kkl_model_recommend_second  r
                join (
                    select 
                    t.uid
                    ,sum(productrate)  productrate
                    from dm.dm_kkl_model_recommend_second t
                    group by t.uid
                    ) total
                on r.uid = total.uid
                ) mt 
            on rf.uid = mt.uid
            and rf.grade = mt.grade
            and rf.subjectId = mt.subjectId
            where rf.ranknumber <= mt.proportion
        """
        # print(sql_text)
        spark.sql(sql_text)



    def complement_subjectid(self):
        """
        :return: 1.补足模型跑数后 uid 推荐的课程不满足 self.simNum 的情况,按subjectid 的关联课程补足
                    2. 不在third表里的用户,按默认推荐
        """
        # 按年级分了再排序的课程,去除科学
        gradeProductRank = spark.sql("""
            select grade,subjectId,productId,useNum
            ,rank()over(partition by grade,subjectId order by useNum desc) as rankNum
            from (
                select grade,subjectId,productId,count(distinct uid)  useNum 
                from kkl_dw.dw_kkl_recommend_base_f_1d 
                where day = '""" + bizdate + """'
                and status = 1
                and subjectID != 10
                group by grade,subjectId,productId 
                ) g
        """)
        gradeProductRank.registerTempTable("gradeProductRank")
        # 无近期行为满足条件的会员
        successUID_public = spark.sql("""
        select uid,count(distinct productid) productNum
        from dm.dm_kkl_model_recommend_first
        group by uid
        having count(distinct productid) >= """+str(self.simNum)+"""
        """)
        successUID_public.registerTempTable("successUID_public")
        # 有近期行为满足条件的会员
        successUID_recent = spark.sql("""
        select uid,count(distinct productid) productNum
        from dm.dm_kkl_model_recommend_third
        group by uid
        having count(distinct productid) >= """+str(self.simNum)+"""
        """)
        successUID_recent.registerTempTable("successUID_recent")
        # 1.有近期行为且推荐课程 >= simNum 的,直接输出
        spark.sql("""
        insert overwrite table kkl_dw.dw_kkl_recommend_rating_f_1d partition(day= '""" + bizdate + """')
        select distinct
        rf.uid,rf.productid,rf.grade,rf.subjectId,rf.completeRating,rf.commentRating,rf.starsRating,rf.rankNumber
        from dm.dm_kkl_model_recommend_third rf
        join successUID_recent s
        on rf.uid = s.uid
        """)
        print("1 is ok")
        # 2.无近期行为且推荐课程 >= simNum 的,直接输出
        spark.sql("""
            insert into kkl_dw.dw_kkl_recommend_rating_f_1d partition(day= '""" + bizdate + """')
            select distinct
            rf.uid ,rf.productId,rf.grade,rf.subjectId,rf.completeRating,rf.commentRating,rf.starsRating,rf.ranknumber
            from dm.dm_kkl_model_recommend_first rf
            join successUID_public s
            on s.uid = rf.uid
            where not exists( select th.uid from dm.dm_kkl_model_recommend_third th
                where th.uid = rf.uid)
        """)
        print("2 is ok")

        # 3.推荐课程 < simNum 的,把剩余的课程补上
        # recentDF_text = """
        #     insert into kkl_dw.dw_kkl_recommend_rating_f_1d partition(day= '""" + bizdate + """')
        #     select
        #     o.uid
        #     ,coalesce(o.productId,g.productId ) productId
        #     ,coalesce(o.grade,g.grade) grade
        #     ,coalesce(o.subjectId,g.subjectId)  subjectId
        #     ,nvl(o.completeRating,0) completeRating
        #     ,nvl(o.commentRating,0) commentRating
        #     ,nvl(o.starsRating,0) starsRating
        #     ,coalesce(o.rankNumber,"""+str(self.simNum)+"""+g.rankNum) as rankNumber
        #     from (
        #         select th.uid ,th.productId,th.grade,th.subjectId,th.completeRating,th.commentRating,th.starsRating,th.rankNumber
        #         from dm.dm_kkl_model_recommend_third th
        #         where not exists(select s.uid from successUID_recent s where s.uid = th.uid)
        #
        #         union all
        #
        #         select rf.uid ,rf.productId,rf.grade,rf.subjectId,rf.completeRating,rf.commentRating,rf.starsRating,rf.rankNumber
        #         from dm.dm_kkl_model_recommend_first rf
        #         where not exists( select th.uid from dm.dm_kkl_model_recommend_third th
        #             where th.uid = rf.uid)
        #         and not exists( select s.uid  from successUID_public s where s.uid = rf.uid)
        #     ) as o
        #     join  gradeProductRank g
        #     on g.grade = o.grade
        #     join dm.dm_kkl_model_recommend_predict p
        #     on p.uid = o.uid
        #     where g.rankNum <= """+str(self.simNum)+"""
        #     and g.productId != o.productId
        #     and p.productId = o.productId
        #     """

        recentDF_text = """
        insert overwrite table dm.dm_kkl_model_recommend_fourth
        select distinct
        th.uid ,th.productId,th.grade,th.subjectId,th.completeRating,th.commentRating,th.starsRating,th.rankNumber
        from dm.dm_kkl_model_recommend_third th
        where not exists(select s.uid from successUID_recent s where s.uid = th.uid)

        union all 
        
        select distinct
        rf.uid ,rf.productId,rf.grade,rf.subjectId,rf.completeRating,rf.commentRating,rf.starsRating,rf.rankNumber
        from dm.dm_kkl_model_recommend_first rf
        where not exists( select th.uid from dm.dm_kkl_model_recommend_third th 
            where th.uid = rf.uid)  
        and not exists( select s.uid  from successUID_public s where s.uid = rf.uid)    
        
        """
        # print(sql_text)
        spark.sql(recentDF_text)
        print("3 is ok")




if __name__ == '__main__':
    COM = KKL_complement(simNum=10)
    COM.tmp_kkl_model_recommend_third()
    COM.complement_subjectid()

