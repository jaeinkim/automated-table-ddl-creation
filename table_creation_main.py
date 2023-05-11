import pandas as pd
import numpy as np
import string
import sys
import os
import shutil
import time
from pathlib import Path

#type_hm ={'DATS': 'DATE', 'CHAR': 'STRING', 'NUMC': 'NUMERIC', 'DEC': 'FLOAT', 'INT4': 'INTEGER', 'QUAN': 'FLOAT', 'CUKY': 'STRING', 'UNIT': 'STRING', 'CURR': 'FLOAT', 'INT8': 'INTEGER', 'RAW': 'BYTES', 'TIMS': 'TIMESTAMP', 'FLTP': 'FLOAT', 'LANG': 'STRING', 'INT2': 'INTEGER'}
####################################################################################################################################################
###Function for Creation of the Tables
####################################################################################################################################################
def create_tables(input_table):
    l=input_table['TABNAME'].nunique() # <class 'pandas.core.frame.DataFrame'> 클래스에서 nunique 메서드는 고유한 값들의 갯수를 출력
    listofuniquetargets = pd.unique(input_table['TABNAME']).tolist() # 해당 컬럼의 유니크한 값을 리스트로 만듬 ['F_SUBSCS_D', 'F_BLLO']
    for i in range(l):
        scan = input_table.loc[input_table['TABNAME'] == listofuniquetargets[i]] # loc 메서드는 행과 열 조회 가능. true 되는 것만 조회 가능. https://m.blog.naver.com/wideeyed/221964700554
        Target_Table_Name=scan['TABNAME'].iloc[0] # iloc는 행번호로 선택 가능
        # Source_Dataset_Name=scan['Source_Dataset_Name'].iloc[0]
        #str(Source_Dataset_Name).strip(" ")
        Target_Columns=[s.strip() for s in scan['FIELDNAME'].tolist()]
        Target_Columns_Desc=[str(s).strip() for s in scan['Description'.upper()].tolist()]
        # Target_Columns_Cluster_Key=[k for k, v in dict(zip(Target_Columns, scan['KEYFLAG'].tolist())).items() if not pd.isna(v)][0:4]
        Target_Columns_Cluster_Key=[k for k, v in dict(zip(Target_Columns, scan['KEYFLAG'].tolist())).items() if not (pd.isna(v) or v.strip() == '')][0:4]
        Target_Columns_Type=[s.strip() for s in scan['BQ_TYPE'].tolist()]
        Source_Columns_Type=[s.strip() for s in scan['DATATYPE'].tolist()]
        if scan.get('PARTITION') is None:
            Target_Partition_Key = []
        else:
            Target_Partition_Key=[k for k, v in dict(zip(Target_Columns, scan['PARTITION'].tolist())).items() if not pd.isna(v) and v != '-']
        Target_Table_Name=scan['TABNAME'].iloc[0].strip()
        Target_Table_Desc=scan['TABNAME_Description'.upper()].iloc[0].strip()

        # Target_Project_Name=scan['Target_Project_Name'].iloc[0]
        Target_Project_Name='emart-datafabric'
        # Target_Dataset_Name=scan['Target_Dataset_Name'].iloc[0]
        Target_Dataset_Name='bw'
        pop=[]
        table = open(fr'Output_DDL/{input_filename}.sql', 'a')
        # print(f'Target_Table_Name = {Target_Table_Name}')
        table.write('DROP TABLE `'+str.lower(Target_Project_Name)+'.'+str.lower(Target_Dataset_Name)+'.'+Target_Table_Name+'`;'+'\n')
        table.write('CREATE OR REPLACE TABLE `'+str.lower(Target_Project_Name)+'.'+str.lower(Target_Dataset_Name)+'.'+Target_Table_Name+'`'+'\n(')
        # table.write('AS SELECT'+'\n')
        table.write('\n')
        for n in range(0, len(Target_Columns)):
            if Target_Columns_Type[n] == 'NUMC':
                Transformed_Columns_Type = 'NUMERIC'
            elif Target_Columns_Type[n] == 'FLOAT':
                Transformed_Columns_Type = 'FLOAT64'
            elif Source_Columns_Type[n] == 'INT2':
                Transformed_Columns_Type = 'INT64'
            elif Source_Columns_Type[n] == 'LANG':
                Transformed_Columns_Type = 'STRING'
            elif Source_Columns_Type[n] == 'TIMS':
                Transformed_Columns_Type = 'TIME'
            elif Target_Table_Name == 'ZCO_AD055' and Target_Columns[n].endswith('_TM'):
                Transformed_Columns_Type = 'FLOAT64'
            else:
                Transformed_Columns_Type = Target_Columns_Type[n]
            # print(Target_Columns[n])
            # print(f'Target_Table_Name = {Target_Table_Name}')
            # print(Target_Columns_Desc[n])
            data= '   '+ Target_Columns[n] + ' ' + Transformed_Columns_Type  + (' NOT NULL' if Target_Columns[n] in Target_Columns_Cluster_Key else '') + ' OPTIONS(description=\"' + Target_Columns_Desc[n] + '\"),'



            table.write(data)
            table.write('\n')
        if not (input_filename.startswith('master') or input_filename.startswith('text')):
            table.write('   ' + 'PTT_DT DATE ' +  f'OPTIONS(description=\"파티션 키\"), \n')
        table.write('   ' + 'ETL_LOAD_DTTM STRING ' +  'OPTIONS(description=\"ETL적재일시\")')
        table.write('\n')
        #Source_Dataset_Name.translate({ord(c): None for c in string.whitespace})
        # input_table['Source_Dataset_Name'] = input_table['Source_Dataset_Name'].replace(' ','')
        # table.write('FROM `'+Source_Dataset_Name+'.'+Target_Table_Name+'`;'+'\n')
        table.write(')\n')
        if len(Target_Partition_Key) != 0:
            table.write('PARTITION BY PTT_DT \n')
        table.write('CLUSTER BY ' + ', '.join([s for s in Target_Columns_Cluster_Key if not pd.isna(s)] ) + '\n')
        table.write('OPTIONS(description=\"' + Target_Table_Desc + (f'. Partition 키: {Target_Partition_Key[0]}' if len(Target_Partition_Key) !=0 else '') +  '\")')
        table.write('\n;\n\n')
        table.close() # Close writing the SQL Table File
####################################################################################################################################################
###Function for Removing the Last Comma
####################################################################################################################################################
def remove_last_comma():
    # with open(r'C:\Users\Sourav Roy\Desktop\Workspace\VIEW_CREATION\Output_DDL\View_Report001.sql', 'r') as file :
    with open(fr'Output_DDL/{input_filename}.sql', 'r') as file :
        filedata = file.read()
    filedata = filedata.replace(",\nFROM","\nFROM")
    with open(fr'Output_DDL/{input_filename}.sql', 'w') as file:
         file.write(filedata)

####################################################################################################################################################
###Function for Removing the legacy content
####################################################################################################################################################
def remove_legacy():
    # with open(r'C:\Users\Sourav Roy\Desktop\Workspace\VIEW_CREATION\Output_DDL\View_Report001.sql', 'r') as file :
    with open(fr'Output_DDL/{input_filename}.sql', 'w') as file:
        file.write('')
####################################################################################################################################################
###Main Function
####################################################################################################################################################
if(__name__=='__main__'):
    # input_filename = '1._ADSO_V3.5.csv'.strip().replace('.csv', '').replace('CSV', '')
    # input_filename = 'master_v3.5.csv'.strip().replace('.csv', '').replace('CSV', '')
    # input_filename = 'text_v3.5.csv'.strip().replace('.csv', '').replace('CSV', '')
    # input_filename = 'add_mig.csv'.strip().replace('.csv', '').replace('CSV', '')
    # input_filename = '1.ADSO_리스트_V.4.2_변경분반영.csv'.strip().replace('.csv', '').replace('CSV', '')
    # input_filename = 'ZPR_AD005-006.csv'.strip().replace('.csv', '').replace('CSV', '')
    input_filename = 'ZMM_AD025, ZMM_AD026.csv'.strip().replace('.csv', '').replace('CSV', '')

    # input_filename = 'test.csv'.strip().replace('.csv', '').replace('CSV', '')


    input_table = pd.read_csv(fr"Input/{input_filename}.csv",encoding='utf8', sep=',', header=None, skiprows=0, na_values=' ', error_bad_lines=False)
    # input_table.columns = input_table.iloc[0] # Index(['Source_Dataset_Name', 'TARGET_Table_Name', 'TARGET_Columns', 'Target_Table_Name', 'Target_Project_Name', 'Target_Dataset_Name'], dtype='object', name= 0)
    input_table.columns = input_table.iloc[0].str.strip() # Index(['Source_Dataset_Name', 'TARGET_Table_Name', 'TARGET_Columns', 'Target_Table_Name', 'Target_Project_Name', 'Target_Dataset_Name'], dtype='object', name= 0)
    input_table = input_table[1:] # input_table.column을 지정하면 input_table는 최 상단에 0 1 2 3 4 가 컬럼으로 지정됨. 따라서 실제 데이터는 1행의 컬럼이 적혀 있는 것이 사라져야 함
    input_table.replace('', np.nan, inplace=True)
    # input_table['Source_Dataset_Name'] = input_table['Source_Dataset_Name'].replace(" ","")
    remove_legacy()
    create_tables(input_table)
    # remove_last_comma()
