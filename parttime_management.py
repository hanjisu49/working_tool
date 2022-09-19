# part-time management.py
# ---------------

"""
이 파일은 잔여 데이터가 일정 개수 이하인 아르바이트 생들에게
자동으로 데이터 추가 할당을 부여하기 위한 코드입니다.
"""

# import lib for default use by Python for MSSQL DB connections
import schedule
import time
import pandas as pd
import pymssql

# connect with mssql with python
connector = pymssql.connect(server = '[insert server code]',
                            user = '[insert id]',
                            password = '[insert pw]',
                            database = '[insert database name]',
                            charset = 'utf8')
cursor = connector.cursor()

# pull USERID which is 30 or less remaining data
under_30 = "SELECT USERID, COUNT(ID) AS 'CNT' FROM [insert database route] WHERE [put condition 1] GROUP BY [put condition 2] HAVING COUNT(ID) <= 30 ORDER BY 1"
df_idcnt = pd.read_sql(sql=under_30, con=connector)
list_id = list(df_idcnt.loc[:,'USERID'])

# exclude employee and kanclass p.t. id
list_id = [id for id in list_id if id not in [1, 2, 6, 16, 17, 18, 25]+[4, 5, 8, 11]]
print(list_id)

# assign 200 data to output id
def push_data(list_var):

    lake = chk_200()    
    for userid in list_var:
        push_query = '''
        UPDATE [insert database route]
        SET USERID = %d, [put condition 1]
        FROM [insert database route] A
            INNER JOIN (SELECT top(200)*
                        FROM [insert database route]
                        WHERE [put condition 2]
                        AND USERID = '%d'
                        AND [put condition 3])	B ON [put condition of join(key)]
        where ([put condition 4] OR [put condition 5]);
        '''
    
        # run query
        cursor.execute(push_query, (userid, lake))
        connector.commit()

        print('%d번에게 데이터 200개를 추가 할당했습니다.' % userid)        

# ensure that there are more than 200 data to pull from
def chk_200():
    temp4 = "SELECT top(200)* FROM [insert database route] WHERE [put condition 1] AND USERID = '4' AND [put condition 2]"
    temp5 = "SELECT top(200)* FROM [insert database route] WHERE [put condition 1] AND USERID = '5' AND [put condition 2]"
    temp8 = "SELECT top(200)* FROM [insert database route] WHERE [put condition 1] AND USERID = '8' AND [put condition 2]"
    if pd.read_sql(sql=temp4, con=connector)["Id"].count() >= 200:
        return 4
    elif pd.read_sql(sql=temp5, con=connector)["Id"].count() >= 200:
        return 5
    elif pd.read_sql(sql=temp8, con=connector)["Id"].count() >= 200:
        return 8
    
# def work(self, ):
print(list_id)
print(chk_200())

if list_id:
    push_data(list_id)

# close
connector.close()