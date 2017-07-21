# -*-coding:utf-8-*- 
# @author: 'Repose' 
# @date: 2017/6/16




from pandas import DataFrame


def read_py_gj_min_price(database):
    SQL = '''
        CREATE TABLE [dbo].[Table_2](
      [te] [int] NULL,
      [fasdf] [nvarchar](50) NULL
    ) ON [PRIMARY]
    '''
    return DataFrame(Connect(database=database).ExecQuery(SQL))



read_py_gj_min_price('sqlserver')