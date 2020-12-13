"""
ASSIGNMENT DIM READER
Created on Sun Dec. 13th 2020
Goal: Automate reading of assignment_dim files and updating SQL db  
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

def readAssignmentDim(): ##generate pandas DataFrame and update SQL db
    ##define column headers account_dim Canvas flat file
    accountdim_cols = ['id','canvas_id','course_id','title','description',
                       'due_at','unlock_at','lock_at','points_possible',
                       'grading_type','submission_types','workflow_state',
                       'created_at','updated_at','peer_review_count',
                       'peer_reviews_due_at','peer_reviews_assigned',
                       'peer_reviews','automatic_peer_reviews','all_day',
                       'all_day_date','could_be_locked',
                       'grade_group_students_individually',
                       'anonymous_peer_reviews','muted','assignment_group_id',
                       'position','visibility','external_tool_id']
 
    
    path = 'C:/Users/ProfN/Documents/GitHub/Canvas_LMC/Canvas_LMC/Dec 10 Data/'
    file_name = []
    for file in os.listdir(path):
        if file.endswith('.gz'):
            file_name.append(file)
            if 'assignment_dim' in file:
                '''
                ID = CANVAS_ID + KEYDIFF, so drop ID
                POSITION keeps track of the order in a list -- unneeded
                MUTED is deprecated and therefore always null
                COULD_BE_LOCKED is unneeded right now
                UNLOCK_AT and LOCK_AT are unnecessary for now
                CREATED_AT and UPDATED_AT and PEER_REVIEWS_DUE_AT are no needed
                ANONYMOUS_PEER_REVIEWS is essentially always false
                ALL_DAY and ALL_DAY_DATE are not being used since we have DUE_DATE
                GRADE_GROUP_STUDENTS_INDIVIDUALLY is essentially always false
                same for AUTOMATIC_PEER_REVIEWS
                VISIBILITY appears to always be the same in this dataset
                drop DESCRIPTION for now
                drop PEER_REVIEW_COUNT as unneeded info
                '''
                print('Reading data from:', file)
                temp = pd.read_csv(path+file, compression='gzip', 
                                   sep='\t', names=accountdim_cols)
                temp.drop(columns=['id','position','muted','could_be_locked',
                                   'unlock_at','lock_at','created_at',
                                   'updated_at','peer_reviews_due_at','all_day',
                                   'anonymous_peer_reviews','all_day_date',
                                   'grade_group_students_individually',
                                   'automatic_peer_reviews','visibility',
                                   'description','peer_review_count']
                          , inplace=True)
                '''
                Drop all assignments that are not visible to students
                i.e. not 'published' in WORKFLOW_STATE
                Then drop WORKFLOW_STATE
                '''
                temp = temp[temp.workflow_state=='published']
                temp.drop(columns=['workflow_state'], inplace=True)
                '''
                convert DUE_AT to 'YYYY-MM-DD' and drop everything due before
                Fall semester began (for now)
                then drop DUE_AT
                '''
                temp.due_at = temp.due_at.apply(lambda x: dt.datetime.strptime(
                    x.split()[0],'%Y-%m-%d') if x!=r'\N' else np.nan)
                temp = temp[temp.due_at>dt.datetime(2020,8,20)]
                temp.drop(columns=['due_at'], inplace=True)
                '''
                convert GRADING_TYPE to dictionary of integer values
                '''
                grade_type = {'not_graded':0, 'pass_fail':1, 'letter_grade':2,
                              'percent':3, 'points':4}
                temp.grading_type = temp.grading_type.apply(
                    lambda x: grade_type[x]).astype(int)
                '''
                subtract KEY DIFF from EXTERNAL_TOOL_ID and replace \ N
                subtract KEY DIFF from COURSE_ID
                subtract KEY DIFF from ASSIGNMENT_GROUP_ID
                '''
                temp.external_tool_id = temp.external_tool_id.apply(
                    lambda x: np.nan if x==r'\N' else int(x)-KEYDIFF)
                temp.course_id = temp.course_id.apply(lambda x: int(x)-KEYDIFF)
                temp.assignment_group_id = temp.assignment_group_id.apply(
                    lambda x: int(x)-KEYDIFF)
                '''
                rename CANVAS_ID as ID
                '''
                temp.rename(columns={'canvas_id':'id'}, inplace=True)
                assignment_dim_df = temp
            else:
                pass
        else:
            pass

    ##UPDATE SQL DB##
    #print('Building/Appending to Canvas DB')
    #engine = create_engine('sqlite:///Canvas.db', echo=False)
    #account_dim_df.to_sql('Account_Dim',con=engine,if_exists='replace')
    
    return file_name, assignment_dim_df 



file_name, assignment_dim_df = readAssignmentDim()

