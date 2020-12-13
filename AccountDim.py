"""
ACCOUNT DIM READER
Created on Sun Dec. 13th 2020
Goal: Automate reading of account_dim files and updating SQL db  
The file should only change very very rarely.

@author: David A. Nash
"""
import numpy as np ##linear algebra tools
import pandas as pd ##data processing, CSV I/O (e.g. pd.read_csv)
import os ##allows for path searching for filenames
import sqlite3
from sqlalchemy import create_engine ##for converting DF to DB

def readAccountDim(): ##generate pandas DataFrame and update SQL db
    ##define column headers account_dim Canvas flat file
    accountdim_cols = ['id', 'canvas_id','name','depth','workflow_state',
                   'parent_account','parent_account_id','grandparent_account',
                   'grandparent_account_id','root_account','root_account_id']
    unneededsubs = []
    for i in range(1,16):
        accountdim_cols.append('subaccount'+str(i))
        accountdim_cols.append('subaccount'+str(i)+'_id')
        if i>3: ##collect columns to delete later
            unneededsubs.append('subaccount'+str(i))
            unneededsubs.append('subaccount'+str(i)+'_id')
    accountdim_cols.append('sis_source_id')  
    
    path = 'C:/Users/ProfN/Documents/GitHub/Canvas_LMC/Canvas_LMC/Dec 10 Data/'
    file_name = []
    for file in os.listdir(path):
        if file.endswith('.gz'):
            file_name.append(file)
            if 'account_dim' in file:
                '''UNNEEDED SUBS columns are all blank
                ROOT_ACCOUNT is always Le Moyne
                ROOT_ACCOUNT_ID is always the same
                CANVAS_ID + KEYDIFF = ID, so drop ID
                moreover, we subtract KEYDIFF from subaccount IDs
                '''
                print('Reading data from:', file)
                temp = pd.read_csv(path+file, compression='gzip', 
                                   sep='\t', names=accountdim_cols).drop(
                                       columns=unneededsubs+['root_account',
                                                'root_account_id', 'id'])
                workflow_dict1 = {'active':1, 'deleted':0}
                temp.workflow_state = temp.workflow_state.apply(lambda x:
                                                                workflow_dict1[x])
                for col in ['parent_account_id', 'grandparent_account_id',
                            'subaccount1_id','subaccount2_id','subaccount3_id']:
                    temp[col] = temp[col].apply(lambda x: np.nan if x==r'\N'
                                                else int(x)-KEYDIFF)
                temp.drop(columns=['parent_account','grandparent_account',
                                   'subaccount1','subaccount2','subaccount3'],
                          inplace = True)
                account_dim_df = temp
            else:
                pass
        else:
            pass

    ##UPDATE SQL DB##
    #print('Building/Appending to Canvas DB')
    #engine = create_engine('sqlite:///Canvas.db', echo=False)
    #account_dim_df.to_sql('Account_Dim',con=engine,if_exists='replace')
    
    return file_name, account_dim_df 



file_name, account_dim_df = readAccountDim()

