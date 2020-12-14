"""
DATA CLEANER
Created on Mon Nov. 30 2020
Goal: Explore and automate cleaning/organizing of data from .gz files 
    From Canvas and create SQL database   

@author: David A. Nash
"""
import numpy as np ##linear algebra tools
import pandas as pd ##data processing, CSV I/O (e.g. pd.read_csv)
import os ##allows for path searching for filenames
import datetime
import sqlite3
from sqlalchemy import create_engine ##for converting DF to DB
#import string # python library
#import re # regex library
## Preprocesssing functions from gensim
## preprocess_string, strip_tags, strip_punctuation, strip_numeric,
## strip_multiple_whitespaces, remove_stopwords, strip_short
#import gensim.parsing.preprocessing as pp
#from gensim.models import Word2Vec # Word2vec
'''With the complete set of datafiles available from Canvas, some of them
will never be useful for our purposes, so delete/ignore those.  They include:
'assignment_group_rule_dim'
all 'override' files
'assignment_rule_dim'
'''

'''Nearly all user ids in canvas have been converted to big int by adding
15270000000000000.  We undo that expansion throughout.
We also will use -1 as a replacement for \ N in those keys so they can be stored
as integers instead of strings.'''

KEYDIFF = 15270000000000000
##List of known faculty user_ids
faculty = [454254935450860033,352329503483327465,]

def readGZs(): ##generate dictionary of pandas DFs with pertinent columns
    ##define column headers for different types of Canvas flat files
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
    
    ccdim_cols = ['id', 'canvas_id', 'user_id', 'address','type',
                  'position','workflow_state','created_at','updated_at']
    #ccfact_cols = ['communication_channel_id', 'user_id','bounce_count']
    course_dim_cols = ['id', 'canvas_id', 'root_account_id','account_id','enrollment_term_id',
                       'name','code','type','created_at','start_at','conclude_at',
                       'publicly_visible','sis_source_id','workflow_state',
                       'wiki_id','syllabus_body']
    cUIcanNAvdim_cols = ['id','canvas_id','name','default','original_position']
    cUInavITEMdim_cols = ['id','root_account_id','visible','position']
    cUInavITEMfact_cols = ['root_account_id','course_ui_navigation_item_id',
                           'course_ui_canvas_navigation_id',
                           'external_tool_activation_id','course_id',
                           'course_account_id','enrollment_term_id']
    Psdim_cols = ['id','canvas_id','user_id','account_id','workflow_state',
                  'last_request_at','last_login_at','current_login_at',
                  'last_login_ip','current_login_ip','position','created_at',
                  'updated_at','password_auto_generated','deleted_at','sis_user_id',
                  'unique_name','integration_id','authentication_provider_id']
    Psfact_cols = ['pseudonym_id','user_id','account_id','login_count',
                   'failed_login_count']
    req_cols = ['id','timestamp','timestamp_year','timestamp_month','timestamp_day',
                'user_id', 'course_id', 'root_account_id', 'course_account_id',
                'quiz_id','discussion_id','conversation_id','assignment_id',
                'url','user_agent','http_method','remote_ip','interaction_micros',
                'web_application_controller','web_application_action',
                'web_application_context_type','web_application_context_id',
                'real_user_id','session_id','user_agent_id','http_status',
                'http_version','developer_key_id']
    userDim_cols = ['id','canvas_id','root_account_id','name','time_zone',
                    'created_at','visibility','school_name','school_position',
                    'gender','locale','public','birthdate','country_code',
                    'workflow_state','sortable_name','global_canvas_id']
    
    
    path = 'C:/Users/ProfN/Documents/GitHub/Canvas_LMC/Canvas_LMC/'
    files = []
    DF_dict = {}
    for file in os.listdir(path):
        if file.endswith('.gz'):
            files.append(file)
            print('Reading data from:', file)
            if 'account_dim' in file:
                '''UNNEEDED SUBS columns are all blank
                ROOT_ACCOUNT is always Le Moyne
                ROOT_ACCOUNT_ID is always the same
                CANVAS_ID + KEYDIFF = ID, so drop ID
                moreover, we subtract KEYDIFF from subaccount IDs
                '''
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
                DF_dict[file] = temp
            elif "communication_channel_dim" in file:
                '''ADDRESS is identifying info
                TYPE contains whether address is email, FB, push, twitter, or sms
                WORKFLOW_STATE keeps track of whether contact setup is 'active',
                'unconfirmed', or 'retired'
                POSITION keeps track of order of preference for communications
                which we don't need since we've dropped the different types.
                We also drop duplicate entries based on user_id and keep only the
                most recent.
                ID = CANVAS_ID + KEYDIFF, so drop ID
                '''
                DF_dict[file] = pd.read_csv(path+file, compression='gzip', 
                                            sep='\t', names=ccdim_cols).drop(
                                                columns=['address', 'type',
                                                         'workflow_state','id',
                                                         'position']).drop_duplicates(
                                                             subset='user_id',
                                                             keep='last')
            elif 'communication_channel_fact' in file:
                #DF_dict[file] = pd.read_csv(path+file, compression='gzip', 
                                            #sep='\t', names=ccfact_cols)
                '''We skip this file because it only keeps track of how many
                communication attempts have bounced back from the user'''
                pass
            elif 'course_dim' in file:
                '''TYPE is deprecated and always NULL
                START_AT is empty for almost all courses, so we'll drop it for now
                CONCLUDE_AT ditto above
                ROOT_ACCOUNT_ID is always the same
                ID == CANVAS_ID + KEYDIFF
                convert WORKFLOW_STATE to coded integer values
                SIS_SOURCE_ID seems to be unnecessary
                Convert all other IDs to ID-KEYDIFF and replace \ N with nan
                Eliminate 'Demo' courses from tracking
                Replace \ N with np.nan in SYLLABUS_BODY
                '''
                temp = pd.read_csv(path+file, compression='gzip', 
                                            sep='\t', names=course_dim_cols)
                temp.drop(columns=['type','start_at','conclude_at', 'id',
                                   'root_account_id','sis_source_id'],
                          inplace=True)
                workflow_dict ={'deleted':0, 'created':1, 'completed':2,
                                'available':3, 'claimed':4}
                temp.workflow_state = temp.workflow_state.apply(
                    lambda x: workflow_dict[x])
                
                temp['lower'] = temp.name.apply(lambda x: x.lower())
                temp.lower = temp.lower.apply(lambda x: True if 'demo ' in x
                                              else False)
                temp = temp[temp.lower==False]
                temp.drop(columns=['lower'], inplace=True)
                
                for col in ['account_id','enrollment_term_id','wiki_id']:
                    temp[col] = temp[col].apply(lambda x: np.nan if x==r'\N'
                                                else int(x)-KEYDIFF)
                
                temp.syllabus_body = temp.syllabus_body.apply(lambda x: np.nan
                                                              if x==r'\N'
                                                              else x)
                
                DF_dict[file] = temp
            elif 'course_ui_canvas' in file:
                '''SKIP THIS BECAUSE IT IS NOT USEFUL TO US'''
                ###Recast default as a boolean 1='Default' 0='NotDefault'
                #temp = pd.read_csv(path+file, compression='gzip', 
                #                            sep='\t', names=cUIcanNAvdim_cols)
                #default_dict = {'Default':True, 'NotDefault':False}
                #temp.default = temp.default.apply(lambda x: default_dict[x])
                ###replace '\N' with -1 in order to use int dtype
                #temp.original_position = temp.original_position.apply(lambda x: -1 if x==r'\N' else x)
                #temp.original_position = temp.original_position.astype(int)
                #DF_dict[file] = temp
                pass
            elif 'item_dim' in file:
                ##ROOT_ACCOUNT_ID is always the same, so we'll drop it
                ##Replace VISIBLE values with boolean
                ##Replace \N in POSITION with -1
                #temp = pd.read_csv(path+file, compression='gzip', 
                #                   sep='\t', names=cUInavITEMdim_cols)
                #temp.drop(columns=['root_account_id'], inplace=True)
                #temp.visible = temp.visible.apply(lambda x: x=='visible')
                #temp.position = temp.position.apply(lambda x: -1 if x==r'\N'
                #                                    else x)
                #temp.position = temp.position.astype(int)
                #DF_dict[file] = temp
                ##SKIP THIS ONE FOR NOW IT SEEMS UNNECESSARY
                pass
            elif 'item_fact' in file:
                '''
                ROOT_ACCOUNT_ID is always the same, so we'll drop it
                COURSE_UI_NAVIGATION_ITEM_ID appears to contain COURSE_ID
                return to this later for more efficiency.
                '''
                temp = pd.read_csv(path+file, compression='gzip', 
                                            sep='\t', names=cUInavITEMfact_cols)
                temp.drop(columns=['root_account_id'], inplace=True)
                DF_dict[file] = temp
                ##SKIP THIS ONE FOR NOW IT SEEMS UNNECESSARY
            elif 'pseudonym_dim' in file:
                '''AUTHENTICATION_PROVIDER_ID is not useful information
                ACCOUNT_ID is always the same (root id)
                INTEGRATION_ID appears to always be the same (and not useful)
                UNIQUE_NAME has identifiable information
                POSITION is not useful information
                PASSWORD_AUTO_GENERATED is not useful information
                LOGIN_IPs are potentially identifiable info
                Convert WORKFLOW_STATE to bool 1='active', 0='deleted'
                ID = CANVAS_ID + KEYDIFF, so drop ID
                SIS_USER_ID is not being used.
                DELETED_AT is not going to be useful
                Drop all users who have never logged in
                '''
                temp = pd.read_csv(path+file, compression='gzip', 
                                            sep='\t', names=Psdim_cols)
                temp.drop(columns=['authentication_provider_id','account_id',
                                   'integration_id','unique_name','position',
                                   'password_auto_generated','last_login_ip',
                                   'current_login_ip','id', 'sis_user_id',
                                   'deleted_at']
                          , inplace=True)
                temp.workflow_state = temp.workflow_state.apply(lambda x: True
                                                                if x=='active'
                                                                else False)
                temp = temp[temp.last_login_at!=r'\N']
                ##REPLACE \N WITH np.nan
                for col in ['last_request_at','last_login_at','current_login_at']:
                    temp[col] = temp[col].apply(lambda x: np.nan if x==r'\N'
                                                else x)
                DF_dict[file]=temp
            elif 'pseudonym_fact' in file:
                '''
                FAILED_LOGIN_COUNT is not useful information
                ACCOUNT_ID is always the same
                Drop all users who have never logged in
                '''
                temp = pd.read_csv(path+file, compression='gzip', 
                                            sep='\t', names=Psfact_cols)
                temp.drop(columns=['failed_login_count','account_id'], 
                          inplace=True)
                temp = temp[temp.login_count!=0]
                DF_dict[file] = temp
            elif 'requests' in file:
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
                if COURSE_ID is null, then the activity is not associated to
                any course, so we should drop those records. (revisit this)
                We also do not care *which* quiz/assignment/etc is being
                interacted with, so convert each of these columns to 1-hots
                We want to study student interactions, so we will delete all
                rows with DEVELOPER_KEY_ID not null, and then delete that col.
                only instructors and IT can masquerade as other users, so
                we remove all rows with REAL_USER_ID not null, and drop that col.
                if URL contains 'undefined', then the request went to a missing
                page.  For now, we drop all such rows as they should not count
                Moreover, if URL contains 'preview' then it must be faculty.
                Convert HTTP_METHOD to a numerical coding
                If USER_ID is missing, then drop the request.
                '''
                temp = pd.read_csv(path+file, compression='gzip', 
                                            sep='\t', names=req_cols)
                temp.drop(columns=['root_account_id','http_version','remote_ip',
                                   'timestamp_year', 'timestamp_month',
                                   'user_agent','http_status','user_agent_id',
                                   'interaction_micros',
                                   'web_application_context_id'], inplace=True)
                temp = temp[temp.course_id!=r'\N']
                temp.quiz_id = temp.quiz_id.apply(lambda x: x!=r'\N')
                temp.discussion_id = temp.discussion_id.apply(
                    lambda x: x!=r'\N')
                temp.conversation_id = temp.conversation_id.apply(
                    lambda x: x!=r'\N')
                temp.assignment_id = temp.assignment_id.apply(
                    lambda x: x!=r'\N')
                temp = temp[temp.developer_key_id==r'\N']
                temp = temp[temp.real_user_id==r'\N']
                temp.drop(columns=['developer_key_id', 'real_user_id'],
                          inplace=True)
                temp['test'] = temp.url.apply(lambda x: 'undefined' in x)
                temp = temp[temp.test==False]
                temp['test2'] = temp.url.apply(lambda x: 'preview' in x)
                temp = temp[temp.test2==False]
                temp.drop(columns=['test', 'test2'],inplace=True)
                '''Create a list of WEB_APPLICATION_ACTION values that
                cannot possibly correspond to student actions
                supposedly ping corresponds to idle activity'''
                non_student = ['save_assignment_order','publish','unpublish',
                               'speed_grader_settings','student_view','backup',
                               'turnitin_report','reorder_items','speed_grader',
                               'moderate','ping','change_gradebook_version',
                               'change_gradebook_column_size',
                               'save_gradebook_column_order','reorder',
                               'reorder_assignments', 'move_questions',
                               'extensions']
                temp['non'] = temp.web_application_action.apply(lambda x:
                                                                False if x in
                                                                non_student
                                                                else x)
                temp = temp[temp.non!=False]
                temp.drop(columns=['non'], inplace=True)
                http_dict = {'GET':0, 'POST':1, 'PUT':2, 'DELETE':3, 'HEAD':4}
                temp.http_method = temp.http_method.apply(lambda x: 
                                                          http_dict[x] if x in 
                                                          http_dict else -1)
                temp = temp[temp.user_id!=r'\N'] ##drop requests with no user_id
                ##Now replace IDs with ID - KEYDIFF and replace \N with np.nan
                for col in ['course_id', 'course_account_id']:
                    temp[col] = temp[col].apply(lambda x: np.nan if x==r'\N'
                                                else int(x)-KEYDIFF)
                DF_dict[file] = temp.copy()
                '''
                Next, we will extract the hour of the day from the timestamp
                and convert the data to view counts and action counts
                grouped by student, course, day, and hour
                First, drop all unnecessary columns
                '''
                temp['hour'] = temp.timestamp.apply(lambda x: int(x.split()[1][:2]))
                temp.drop(columns=['url','course_account_id', 'timestamp',
                                   'quiz_id','discussion_id','conversation_id',
                                   'assignment_id','web_application_controller',
                                   'web_application_context_type','session_id',
                                   'web_application_action', 'id'], inplace=True)
                temp_views = temp[temp.http_method==0]
                temp_views = temp_views.groupby(by=['timestamp_day','hour',
                                                    'course_id',
                                                    'user_id']).count()
                temp_views.rename(columns={'http_method':'daily_views'},
                                  inplace=True)
                ##recreate columns for grouped vars
                temp_views = temp_views.reset_index()
                temp_actions = temp[temp.http_method==1]
                temp_actions = temp_actions.groupby(by=['timestamp_day','hour',
                                                        'course_id',
                                                        'user_id']).count()
                temp_actions.rename(columns={'http_method':'daily_actions'},
                                    inplace=True)
                ##recreate columns for groups vars
                temp_actions = temp_actions.reset_index()
                ##recombine the two tables to get both counts together
                temp = pd.merge(temp_views,temp_actions,'outer',
                                on=['timestamp_day','hour','course_id','user_id'])
                ##fill NAs with zeros
                temp.fillna(0, inplace=True)
                ##change datatypes to ints
                temp.daily_views = temp.daily_views.astype(int)
                temp.daily_actions = temp.daily_actions.astype(int)
                DF_dict['Requests_Counts'] = temp
                
            elif 'user_dim' in file:
                '''
                VISIBILITY is deprecated and will always be null
                SCHOOL_NAME was only used in trial versions
                SCHOOL_POSITION was only used in trial versions
                PUBLIC was used in trial version to track institution type
                BIRTHDATE is identifiable information
                COUNTRY_CODE may be identifiable (and is not useful)
                ROOT_ACCOUNT_ID is always the same
                TIME_ZONE is not useful information (almost always ET)
                LOCALE is almost always blank, and not useful
                GLOBAL_CANVAS_ID == CANVAS_ID + 15270000000000000
                Drop all 'Test Student' rows
                Then drop NAME and SORTABLE_NAME as identifiable
                Finally, convert WORKFLOW_STATE to numerical codes
                '''
                temp = pd.read_csv(path+file, compression='gzip', 
                                            sep='\t', names=userDim_cols)
                temp.drop(columns = ['visibility','school_name',
                                     'school_position',
                                     'gender','public','birthdate',
                                     'country_code','root_account_id',
                                     'time_zone', 'locale',
                                     'global_canvas_id'], inplace=True)
                temp = temp[temp.name!='Test Student']
                temp.drop(columns=['name','sortable_name'], inplace=True)
                WF_dict = {'deleted':0, 'registered':1, 'pre_registered':2,
                           'creation_pending':3}
                temp.workflow_state = temp.workflow_state.apply(lambda x:
                                                                WF_dict[x])
                DF_dict[file] = temp
            else:
                DF_dict[file] = pd.read_csv(path+file, compression='gzip', 
                                            sep='\t', header=0)
        else:
            pass
    return files, DF_dict 



files, DF_dict = readGZs()
############# CREATE SQL DB ##############################
print('Building/Appending to Canvas DB')
engine = create_engine('sqlite:///Canvas.db', echo=False)
Table_names = ['Account_Dim','Communication_Channel_Dim','Communication_Channel_Fact',
               'Course_Dim','Course_UI_Canvas_Navigation_Dim', 
               'Course_UI_Navigation_Item_Dim','Course_UI_Navigation_Item_Fact',
               'Pseudonym_Dim', 'Pseudonym_Fact', 'Requests', 'User_Dim',
               'Requests_Counts']

#for i in range(12):
#    if i!=2 and i!=4 and i!=5 and i!=6 and i!=9 and i!=11:
#        DF_dict[files[i]].to_sql(Table_names[i], con=engine, if_exists='replace')
#    elif i==11:
#        DF_dict['Requests_Counts'].to_sql(Table_names[i], con=engine,
#                                          if_exists='replace')

df0 = DF_dict[files[0]]
df1 = DF_dict[files[1]]
###SKIP DF2 BECAUSE IT'S NOT USEFUL###
#df2 = DF_dict[files[2]] 
#df2.to_sql('Communication_Channel_Fact', con=engine, if_exists='append')
df3 = DF_dict[files[3]]
df3.created_at = df3.created_at.astype('datetime64')
df3Recent = df3[df3.created_at>'2020-01-01']
##SKIP DF4, DF5, and DF6 AS THEY SEEM UNNECESSARY##
#df4 = DF_dict[files[4]]
#df5 = DF_dict[files[5]]
df6 = DF_dict[files[6]]
df7 = DF_dict[files[7]]
df8 = DF_dict[files[8]]
df9 = DF_dict[files[9]]
df10 = DF_dict[files[10]]
dfCounts = DF_dict['Requests_Counts']

#conn = sqlite3.connect('Canvas.db')
#cur = conn.cursor()


#fig, (ax1, ax2) = plt.subplots(ncols=2, figsize=(20,6))
#by_code = sns.barplot(x='code', y='daily_actions', data=testCourse.tail(15), ax=ax1)
#for item in by_code.get_xticklabels():
#    item.set_rotation(90)
#by_code2 = sns.barplot(x='code', y='daily_views', data=testCourse.tail(15), ax=ax2)
#for item in by_code2.get_xticklabels():
#    item.set_rotation(90)
#plt.show()

