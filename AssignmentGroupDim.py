"""
ASSIGNMENT GROUP DIM READER
Created on Sun Dec. 13th 2020
Goal: Automate reading of assignment_group_dim files and updating SQL db  
In principle these should change mostly before a semester begins?

@author: David A. Nash
"""
import numpy as np ##linear algebra tools
import pandas as pd ##data processing, CSV I/O (e.g. pd.read_csv)
import os ##allows for path searching for filenames
import datetime as dt
import sqlite3
from sqlalchemy import create_engine ##for converting DF to DB

'''Nearly all user ids in canvas have been converted to big int by adding
15270000000000000.  We undo that expansion throughout.
We also will use -1 as a replacement for \ N in those keys so they can be stored
as integers instead of strings.'''

KEYDIFF = 15270000000000000
##List of known faculty user_ids
faculty = [454254935450860033,352329503483327465,]

def readAssignmentGroupDim(): ##generate pandas DataFrame and update SQL db
    ##define column headers account_dim Canvas flat file
    cols = ['id','canvas_id','course_id','name','default_assignment_name',
            'workflow_state','position','created_at','updated_at']
 
    
    path = 'C:/Users/ProfN/Documents/GitHub/Canvas_LMC/Canvas_LMC/Dec 10 Data/'
    file_name = []
    for file in os.listdir(path):
        if file.endswith('.gz') and 'assignment_group_dim' in file:
            file_name.append(file)
            '''
            ID = CANVAS_ID + KEYDIFF, so drop CANVAS_ID and adjust ID
            similarly, adjust COURSE_ID by subtracting KEYDIFF
            POSITION is unneeded
            DEFAULT_ASSIGNMENT_NAME is unneeded also
            '''
            print('Reading data from:', file)
            temp = pd.read_csv(path+file, compression='gzip', 
                               sep='\t', names=cols)
            temp.drop(columns=['canvas_id','position',
                               'default_assignment_name']
                      , inplace=True)
            temp.id = temp.id.apply(lambda x: int(x)-KEYDIFF)
            temp.course_id = temp.course_id.apply(lambda x: int(x)-KEYDIFF)
            '''
            for now, drop all assignments that were created before Jun 2020
            then drop CREATED_AT and UPDATED_AT
            '''
            temp.created_at = temp.created_at.apply(
                lambda x: dt.datetime.strptime(x.split()[0],'%Y-%m-%d') 
                if x!=r'\N' else np.nan)
            temp = temp[temp.created_at>dt.datetime(2020,6,1)]
            temp.drop(columns=['created_at','updated_at'], inplace=True)
            '''
            remove 'deleted' groups and drop WORKFLOW_STATE
            '''
            temp=temp[temp.workflow_state=='available']
            temp.drop(columns=['workflow_state'], inplace=True)
            assignment_group_dim_df = temp
            '''
            parse the NAME column to reduce the number of possibilities to a
            few options.
            '''
            
        else:
            pass

    ##UPDATE SQL DB##
    #print('Building/Appending to Canvas DB')
    #engine = create_engine('sqlite:///Canvas.db', echo=False)
    #account_dim_df.to_sql('Account_Dim',con=engine,if_exists='replace')
    
    return file_name, assignment_group_dim_df 



file_name, assignment_group_dim_df = readAssignmentGroupDim()

