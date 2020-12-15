"""
REQUESTS READER
Created on Mon Dec. 14th 2020
Goal: Automate reading of requests, aggregation of data,
updating of SQL database

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
##List of known faculty user_ids -- this includes anyone who created new
##assignments, etc.
faculty = [454254935450860033,352329503483327465,461510280206911953,
           260399413616254107,446354931750470913,459297289574575361,
           13494308845404191,-111579948995598634,-439233688573536041,
           -331983840839712843,279149467034293695,168521247232947848,
           -276503210160044986,22038591728744718,-262710809639375872,
           -20556520449892440,207578437277249390,449545178198982231,
           269525800159524778,433449880292367858,169297019236742547,
           383696511811241783,-444968307030906019,-129125899578755545,
           -129924333674845945,-513414722028092034,528368423185167393,
           134794813544496452,349032263021651377,-160041951289911943,
           -331983840839712843,-390256345561288500,213550622228073392,
           -53469270470739886,-189986813610458687,-189007000015170712,
           218988151122579889,363382961312611750,221590282248879275,
           555427588082431840,-533044698678973262,-520779095311242509,
           -66791551983613735]

def readRequests(): ##generate pandas DataFrame and update SQL db
    ##define column headers account_dim Canvas flat file
    req_cols = ['id','timestamp','timestamp_year','timestamp_month','timestamp_day',
                'user_id', 'course_id', 'root_account_id', 'course_account_id',
                'quiz_id','discussion_id','conversation_id','assignment_id',
                'url','user_agent','http_method','remote_ip','interaction_micros',
                'web_application_controller','web_application_action',
                'web_application_context_type','web_application_context_id',
                'real_user_id','session_id','user_agent_id','http_status',
                'http_version','developer_key_id']
 
    
    path = 'C:/Users/ProfN/Documents/GitHub/Canvas_LMC/Canvas_LMC/Dec 10 Data/'
    file_name = []
    requests_agg = []
    requests_full = []
    for file in os.listdir(path):
        if file.endswith('.gz') and 'requests' in file:
            print('Reading data from file',file)
            file_name.append(file)
            '''Lots of work to do here... this is always going to be
            the biggest file and with the most updating to do
            ROOT_ACCOUNT_ID is always the same
            HTTP_VERSION is always the same (and unhelpful)
            REMOTE_IP is potentially identifying
            USER_AGENT_ID is not implemented
            TIMESTAMP_YEAR and TIMESTAMP_MONTH contained in TIMESTAMP_DAY
            USER_AGENT explains how they connected -- drop for now --
            HTTP_STATUS ??? -- drop for now --
            INTERACTION_MICROS = time required to process request -- drop
            WEB_APPLICATION_CONTEXT_ID appears to match COURSE_ID nearly always
            '''
            temp = pd.read_csv(path+file, compression='gzip', 
                                        sep='\t', names=req_cols)
            temp.drop(columns=['root_account_id','http_version','remote_ip',
                               'timestamp_year', 'timestamp_month',
                               'user_agent','http_status','user_agent_id',
                               'interaction_micros',
                               'web_application_context_id'], inplace=True)
            
            
            '''
            if COURSE_ID is null, then the activity is not associated to
            any course, so we should drop those records.
            We also do not care *which* quiz/assignment/etc is being
            interacted with, so convert each of these columns to 1-hots
            '''
            temp = temp[temp.course_id!=r'\N']
            temp.quiz_id = temp.quiz_id.apply(lambda x: x!=r'\N')
            temp.discussion_id = temp.discussion_id.apply(
                lambda x: x!=r'\N')
            temp.conversation_id = temp.conversation_id.apply(
                lambda x: x!=r'\N')
            temp.assignment_id = temp.assignment_id.apply(
                lambda x: x!=r'\N')
            
            
            '''
            We want to study student interactions, so we will delete all
            rows with DEVELOPER_KEY_ID not null, and then delete that col.
            only instructors and IT can masquerade as other users, so
            we remove all rows with REAL_USER_ID not null, and drop that col.
            Moreover, if USER_ID is missing, we drop that request
            '''
            temp = temp[temp.developer_key_id==r'\N']
            temp = temp[temp.real_user_id==r'\N']
            temp.drop(columns=['developer_key_id', 'real_user_id'],
                      inplace=True)
            temp = temp[temp.user_id!=r'\N'] ##drop requests with no user_id
            ##convert data type
            temp.user_id = temp.user_id.astype('int64')


            
            '''
            if URL contains 'undefined', then the request went to a missing
            page.  For now, we drop all such rows as they should not count
            Moreover, if URL contains 'gradebook' then it must be faculty.
            So use those values to generate a list of faculty ids
            Drop those faculty rows and those actions
            '''
            temp['und'] = temp.url.apply(lambda x: 'undefined' in x
                                         or 'users' in x)
            temp = temp[temp.und!=True]

            temp['test'] = temp.url.apply(lambda x: 'gradebook' in x)
            fac_list = list(temp[temp.test].user_id.drop_duplicates())
            fac_list = [int(x) for x in fac_list]
            print('\nFaculty detected:', len(fac_list))
            ##add in known faculty
            fac_list = set(fac_list+faculty)
            ##drop rows with faculty ids
            temp['fac'] = temp.user_id.apply(lambda x: x in fac_list)
            #print(temp.fac.sum())
            temp = temp[temp.fac!=True]
            temp.drop(columns=['fac', 'test','und'], inplace=True)
            #temp = temp[temp.test==False]
            #temp.drop(columns=['test'],inplace=True)
            
            
            '''
            Create a list of WEB_APPLICATION_ACTION values that
            cannot possibly correspond to student actions
            supposedly ping corresponds to idle activity
            activity_stream_summary seems to also correspond to idleness
            Also drop 'quiz_assignment_overrides' from WEB_APPLICATION_CONTROLLER
            '''
            non_student = ['save_assignment_order','publish','unpublish',
                           'speed_grader_settings','student_view','backup',
                           'turnitin_report','reorder_items','speed_grader',
                           'moderate','ping','change_gradebook_version',
                           'change_gradebook_column_size','new','unenroll_user',
                           'save_gradebook_column_order','reorder',
                           'reorder_assignments', 'move_questions',
                           'extensions','activity_stream_summary']
            temp['non'] = temp.web_application_action.apply(lambda x:
                                                            False if x in
                                                            non_student
                                                            else x)
            temp = temp[temp.non!=False]
            temp['over'] = temp.web_application_controller.apply(
                lambda x: 'overrides' in x)
            temp = temp[temp.over!=True]
            temp.drop(columns=['non','over'], inplace=True)
            
            
            '''
            Convert HTTP_METHOD to a numerical coding
            Replace IDs with ID - KEYDIFF and replace \ N with np.nan
            '''
            http_dict = {'GET':0, 'POST':1, 'PUT':2, 'DELETE':3, 'HEAD':4}
            temp.http_method = temp.http_method.apply(lambda x: 
                                                      http_dict[x] if x in 
                                                      http_dict else -1)
            for col in ['course_id', 'course_account_id']:
                temp[col] = temp[col].apply(lambda x: np.nan if x==r'\N'
                                            else int(x)-KEYDIFF)
            
            
            ##return requests without aggregation for further exploration
            if len(requests_full)==0:
                requests_full = temp.copy()
            else:
                requests_full = requests_full.append(temp.copy())
                requests_full.drop_duplicates(inplace=True)
            
            
            
            '''
            Next, we will extract the hour of the day from the timestamp
            and convert the data to view counts and action counts
            grouped by course, course_account, day, and hour
            First, drop all unnecessary columns
            '''
            temp['hour'] = temp.timestamp.apply(lambda x: int(x.split()[1][:2]))
            temp.drop(columns=['url', 'timestamp', 'user_id',
                               'quiz_id','discussion_id','conversation_id',
                               'assignment_id','web_application_controller',
                               'web_application_context_type','session_id',
                               'web_application_action', 'id'], inplace=True)
            ##First for page_views -- http_method==GET
            temp_views = temp[temp.http_method==0]
            temp_views = temp_views.groupby(by=['course_account_id','course_id',
                                                'timestamp_day','hour'
                                                ]).count()
            temp_views.rename(columns={'http_method':'hourly_views'},
                              inplace=True)
            ##recreate columns for grouped vars
            temp_views = temp_views.reset_index()
            
            ##Next for "actions" -- http_method==POST
            temp_actions = temp[temp.http_method==1]
            temp_actions = temp_actions.groupby(by=['course_account_id','course_id',
                                                    'timestamp_day','hour'
                                                    ]).count()
            temp_actions.rename(columns={'http_method':'hourly_actions'},
                                inplace=True)
            ##recreate columns for groups vars
            temp_actions = temp_actions.reset_index()
            ##recombine the two tables to get both counts together
            temp = pd.merge(temp_views,temp_actions,'outer',
                            on=['course_account_id','course_id',
                                'timestamp_day','hour'])
            ##fill NAs with zeros
            temp.fillna(0, inplace=True)
            ##change datatypes to ints
            temp.hourly_views = temp.hourly_views.astype(int)
            temp.hourly_actions = temp.hourly_actions.astype(int)
            requests_agg.append(temp)
        else:
            pass

    ##UPDATE SQL DB##
    #print('Building/Appending to Canvas DB')
    #engine = create_engine('sqlite:///Canvas.db', echo=False)
    #requests_agg.to_sql('Requests_Counts',con=engine,if_exists='replace')
    
    return file_name, requests_full, requests_agg



file_name, requests_full, requests_agg = readRequests()

