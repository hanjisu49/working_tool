# part-time management.py
# ---------------

"""
이 파일은 잔여 데이터가 일정 개수 이하인 아르바이트 생들에게
자동으로 데이터 추가 할당을 부여하기 위한 코드입니다.
"""

# import lib for default use by Python for MSSQL DB connections
from winreg import LoadKey
import pandas as pd
import pymssql

from threading import Thread
import time
import warnings
warnings.filterwarnings("ignore")

# connect with mssql with python
connector = pymssql.connect(server = '[insert server code]',
                            user = '[insert id]',
                            password = '[insert pw]',
                            database = '[insert database name]',
                            charset = 'utf8')
cursor = connector.cursor()

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
            where ([put condition 4] OR [put condition 5];
        '''
    
        # run query
        cursor.execute(push_query, (userid, lake))
        conn.commit()

        print('%d번에게 데이터 3개를 추가 할당했습니다.' % userid)        

# ensure that there are more than 200 data to pull from
def chk_200():
    temp4 = "SELECT top(200)* FROM [insert database route] WHERE [put condition 1] AND USERID = '4' AND [put condition 2]"
    temp5 = "SELECT top(200)* FROM [insert database route] WHERE [put condition 1] AND USERID = '5' AND [put condition 2]"
    temp8 = "SELECT top(200)* FROM [insert database route] WHERE [put condition 1] AND USERID = '8' AND [put condition 2]"
    if pd.read_sql(sql=temp4, con=conn)["Id"].count() >= 200:
        return 4
    elif pd.read_sql(sql=temp5, con=conn)["Id"].count() >= 200:
        return 5
    elif pd.read_sql(sql=temp8, con=conn)["Id"].count() >= 200:
        return 8

def take_input():
    print("take_input() invoked!")
    while True:
        user_input = input('Type Q to terminate the program: \n')
        # doing something with the input
        if user_input == "q":
            print("Terminating... Please wait for max 1 minute.\n")
            conn.close()
            break
        else:
            print("Invalid Input.\n")

def main_frame():
    global thread_running

    while thread_running:
        print("\nMain thread activated.\n")  
        # pull USERID which is 30 or less remaining data
        under_30 = "SELECT USERID, COUNT(ID) AS 'CNT' FROM [insert database route] WHERE [put condition 1] GROUP BY [put condition 2] HAVING COUNT(ID) <= 30 ORDER BY 1"
        df_idcnt = pd.read_sql(sql=under_30, con=conn)
        list_id = list(df_idcnt.loc[:,'USERID'])

        # exclude employee and kanclass p.t. id
        list_id = [id for id in list_id if id not in [1, 2, 6, 7, 16, 17, 18, 25]+[4, 5, 8 ,11]]

        if list_id:
            print("Finalizing list of workers for additional workload: ", list_id)
            push_data(list_id)
        else:
            print("할당할 사람이 없습니다.")

        while(thread_running):
            for minute in range(TIME_INTERVAL):
                time.sleep(30)
    
    print("FALSE on thread_running detected, terminating main thread now...\n")

if __name__ == '__main__':
    t1 = Thread(target=main_frame)
    t2 = Thread(target=take_input)
    
    t1.start()
    t2.start()

    t2.join()
    thread_running = False
    
    print("Process successfully terminated.")