# -*-coding:utf-8-*- 
# @author: 'Repose' 
# @date: 2017/6/19

class Connect(object):
    # num_count = 0
    def __init__(self,database,host,port,user, pwd,db):
        self.database = str(database).lower()
        self.host =  host
        self.port = port
        self.user = user
        self.pwd = pwd
        self.db = db

        # Connect.num_count +=1
        # print (Connect.num_count)
    def __del__(self):
        self.conn.close()
        # Connect.num_count -=1
        # print (Connect.num_count)

    def __GetConnect(self):
        if self.database == 'sqlserver'  :
            import pyodbc
            conn_info = 'DRIVER={SQL Server};DATABASE=%s;SERVER=%s;UID=%s;PWD=%s' % (self.db, str(self.host)+','+str(self.port), self.user, self.pwd)
            conn = pyodbc.connect(conn_info)
            # except:
            #     import pymssql
            #     conn = pymssql.connect(host=str(self.host)+':'+str(self.port), user=self.user, password=self.pwd, database=self.db)
        elif self.database == 'hive'  :
            import pyhs2
            conn = pyhs2.connect(host=self.host, port=self.port, user=self.user, authMechanism="PLAIN",
                                 password=self.pwd)
        else :
            import pyodbc
            conn_info = (
            'Driver={MySQL ODBC 5.1 Driver};Server=%s;Port=%s;Database=%s;User=%s; Password=%s;Option=3;' % (
            self.host, self.port, self.db, self.user, self.pwd))
            conn = pyodbc.connect(conn_info)
        self.conn = conn
        cur = self.conn.cursor()   #  使用conn连接创建(并返回)一个游标的对象
        if not cur:
            raise (NameError, "数据库连接失败")
        else:
            return cur

    def ExecQuery(self,sql):
        cur=self.__GetConnect()
        try:
            cur.execute(sql)   # 执行数据库select命里
            resList = cur.fetchall()  # 获取查询结果的所有行
        finally:
            cur.close()
        return resList

    def OperatTable(self,sql):
        cur = self.__GetConnect()
        try:
            cur.execute(sql)   # 执行数据库select命里
            self.conn.commit()
        finally:
            cur.close()


if __name__ == '__main__':
    CN = Connect('sqlserver',host='10.0.0.213',port='1433',user='test',pwd='test',db='test')
    # print(CN.ExecQuery('''select * from test.dbo.Table_1'''))
    CN.OperatTable('''
     -- insert into test.dbo.Table_wxt select t.* from (select 1 as t1,'python' as t2)as t 
      CREATE TABLE test.dbo.Table_python(te int ,fasdf nvarchar(50) ) 
    ''')
    # import pyodbc
    # pyodbc.connect('DRIVER={SQL Server};DATABASE=test;SERVER=10.0.0.213,1433;UID=test;PWD=test')
