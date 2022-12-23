import pandas as pd
import numpy as np
import string
import sys
import os
import shutil
import time
from pathlib import Path
####################################################################################################################################################
###Function for Creation of the Views
####################################################################################################################################################
def create_views(input_view):
    l=input_view['TABNAME'].nunique() # <class 'pandas.core.frame.DataFrame'> 클래스에서 nunique 메서드는 고유한 값들의 갯수를 출력
    listofuniquetargets = pd.unique(input_view['TABNAME']).tolist() # 해당 컬럼의 유니크한 값을 리스트로 만듬 ['F_SUBSCS_D', 'F_BLLO']
    for i in range(l):
        scan = input_view.loc[input_view['TABNAME'] == listofuniquetargets[i]] # loc 메서드는 행과 열 조회 가능. true 되는 것만 조회 가능. https://m.blog.naver.com/wideeyed/221964700554
        Target_Table_Name=scan['TABNAME'].iloc[0] # iloc는 행번호로 선택 가능
        # Source_Dataset_Name=scan['Source_Dataset_Name'].iloc[0]
        #str(Source_Dataset_Name).strip(" ")
        Target_Columns=scan['FIELDNAME'].tolist()
        Target_View_Name=scan['TABNAME'].iloc[0]
        # Target_Project_Name=scan['Target_Project_Name'].iloc[0]
        Target_Project_Name='emart-datafabric'
        # Target_Dataset_Name=scan['Target_Dataset_Name'].iloc[0]
        Target_Dataset_Name='bods_test'
        pop=[]
        view = open(r'Output_DDL/View_Report001.sql', 'a')
        view.write('CREATE OR REPLACE TABLE `'+str.lower(Target_Project_Name)+'.'+str.lower(Target_Dataset_Name)+'.'+Target_View_Name+'`'+' ')
        view.write('AS SELECT'+'\n')
        for n in range(0, len(Target_Columns)):
            data= '   '+Target_Columns[n]+','
            view.write(data)
            view.write('\n')
        #Source_Dataset_Name.translate({ord(c): None for c in string.whitespace})
        # input_view['Source_Dataset_Name'] = input_view['Source_Dataset_Name'].replace(' ','')
        # view.write('FROM `'+Source_Dataset_Name+'.'+Target_Table_Name+'`;'+'\n')
        view.close() # Close writing the SQL View File
####################################################################################################################################################
###Function for Removing the Last Comma
####################################################################################################################################################
def remove_last_comma():
    # with open(r'C:\Users\Sourav Roy\Desktop\Workspace\VIEW_CREATION\Output_DDL\View_Report001.sql', 'r') as file :
    with open(r'Output_DDL/1._ADSO_V3.4.sql', 'r') as file :
        filedata = file.read()
    filedata = filedata.replace(",\nFROM","\nFROM")
    with open(r'Output_DDL/1._ADSO_V3.4.sql', 'w') as file:
         file.write(filedata)
####################################################################################################################################################
###Main Function
####################################################################################################################################################
if(__name__=='__main__'):
    input_view = pd.read_csv(r"Input/1._ADSO_V3.4.csv",encoding='utf8', sep=',', header=None, skiprows=0, na_values=' ', error_bad_lines=False)
    input_view.columns = input_view.iloc[0] # Index(['Source_Dataset_Name', 'TARGET_Table_Name', 'TARGET_Columns', 'Target_View_Name', 'Target_Project_Name', 'Target_Dataset_Name'], dtype='object', name= 0)
    input_view = input_view[1:] # input_view.column을 지정하면 input_view는 최 상단에 0 1 2 3 4 가 컬럼으로 지정됨. 따라서 실제 데이터는 1행의 컬럼이 적혀 있는 것이 사라져야 함
    input_view.replace('', np.nan, inplace=True)
    # input_view['Source_Dataset_Name'] = input_view['Source_Dataset_Name'].replace(" ","")
    create_views(input_view)
    remove_last_comma()
