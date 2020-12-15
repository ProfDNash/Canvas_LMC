# -*- coding: utf-8 -*-
"""
CANVAS SQL DATABASE QUERY VISUALIZER
Created on Wed Dec  9 14:30:55 2020

@author: ProfN
"""

import numpy as np
import pandas as pd
import sqlite3
import sqlalchemy as db
from sqlalchemy import create_engine
import seaborn as sns
import matplotlib.pyplot as plt

engine = create_engine('sqlite:///Canvas.db')
conn = engine.connect()
meta = db.MetaData()
req = db.Table('Requests_Counts', meta, autoload=True, 
                       autoload_with=engine)
courses = db.Table('Course_Dim', meta, autoload=True,
                   autoload_with=engine)
acc = db.Table('Account_Dim', meta, autoload=True,
               autoload_with=engine)
query = db.select([req])
result_proxy = conn.execute(query)
result_set = result_proxy.fetchall()
col_names = ['index','day','hour','course_id','course_account_id',
             'user_id','hourly_views','hourly_actions']
req_df = pd.DataFrame(result_set, columns=col_names)

def hourlyViz():
    ##query the DB for hourly views and actions all time and visualize the results
    hourly_query = db.select([db.func.sum(req.columns.hourly_views).label('hourly_views'),
                          db.func.sum(req.columns.hourly_actions).label('hourly_actions')]).group_by(req.columns.hour)
    hourly_result_proxy = conn.execute(hourly_query)
    hourly_result_set = hourly_result_proxy.fetchall()
    hourly_df = pd.DataFrame(hourly_result_set, 
                             columns=['hourly_views','hourly_actions'])
    hourly_df.reset_index(inplace=True)  ##gives an index column to use
    ##plot query information##
    sns.set_palette('viridis',24)
    fig, (ax1, ax2) = plt.subplots(ncols=2, figsize=(20,6))
    sns.barplot(x='index', y='hourly_views', data=hourly_df, ax=ax1)
    sns.barplot(x='index', y='hourly_actions', data=hourly_df, ax=ax2)
    ax1.set_xlabel('Hour of the Day')
    ax1.set_ylabel('Hourly Page Views')
    ax2.set_xlabel('Hour of the Day')
    ax2.set_ylabel('Hourly Actions')
    ax1.set_title('Student Page Views by the Hour')
    ax2.set_title('Student Actions by the Hour')
    plt.show()
    return None

def classViz(n_results=25, orderby = 'views'):
    ##query the DB for page views and actions grouped by course, visualize top
    ##n_results number of courses ordered by orderby
    byClass_query = db.select([courses.columns.code, db.func.sum(req.columns.hourly_views),
                               db.func.sum(req.columns.hourly_actions)]).where(
        courses.columns.canvas_id==req.columns.course_id).group_by(courses.columns.code)
    byClass_result_proxy = conn.execute(byClass_query)
    byClass_result_set = byClass_result_proxy.fetchall()
    byClass_df = pd.DataFrame(byClass_result_set, 
                              columns=['Code','daily_views','daily_actions'])
    byClass_df.sort_values(by='daily_'+orderby, ascending=False, inplace=True)
    ##plot query information##
    sns.set_palette('viridis',n_results)
    fig, (ax1, ax2) = plt.subplots(ncols=2, figsize=(20,6))
    bar1 = sns.barplot(x='Code', y='daily_views', data=byClass_df.head(n_results), ax=ax1)
    for item in bar1.get_xticklabels():
        item.set_rotation(90)
    bar2 = sns.barplot(x='Code', y='daily_actions', data=byClass_df.head(n_results), ax=ax2)
    for item in bar2.get_xticklabels():
        item.set_rotation(90)
    ax1.set_xlabel('')
    ax1.set_ylabel('Total Page Views')
    ax2.set_xlabel('')
    ax2.set_ylabel('Total Actions')
    ax1.set_title('Total Student Page Views by Course for Top {}'.format(n_results))
    ax2.set_title('Total Student Actions by Course for Top {}'.format(n_results))
    plt.show()
    return None

def departmentViz(n_results=10, orderby='views'):
    ##query the DB and aggregate AVG daily views and daily actions by department
    ##visualize the results of the top n_results departments
    byDept_query = db.select([req.columns.course_account_id,req.columns.course_id,
                              req.columns.timestamp_day,
                              db.func.sum(req.columns.hourly_views),
                              db.func.sum(req.columns.hourly_actions)]).group_by(
                                  req.columns.course_account_id,
                                  req.columns.course_id,
                                  req.columns.timestamp_day)
    byDept_result_proxy = conn.execute(byDept_query)
    byDept_result_set = byDept_result_proxy.fetchall()
    byDept_df = pd.DataFrame(byDept_result_set, 
                             columns=['account_id','course_id','day',
                                      'daily_views','daily_actions'])
    ##aggregate average daily views accross departments
    byDept_df = byDept_df.groupby(by=['account_id']).mean()
    byDept_df.drop(columns=['course_id'], inplace=True)
    byDept_df.reset_index(inplace=True)
    
    ##pull department names from Account_Dim table
    deptNames_query = db.select([acc.columns.canvas_id,acc.columns.name])
    deptNames_result_proxy = conn.execute(deptNames_query)
    deptNames_result_set = deptNames_result_proxy.fetchall()
    deptNames_df = pd.DataFrame(deptNames_result_set,
                                columns=['account_id','name'])
    
    ##merge the data to put department names with the average counts
    byDept_df = pd.merge(byDept_df,deptNames_df,how='left',on=['account_id'])
    
    ##sort the data by chosen value
    byDept_df.sort_values(by='daily_'+orderby, ascending=False, inplace=True)
    ##plot query information##
    sns.set_palette('viridis',n_results)
    fig, (ax1, ax2) = plt.subplots(ncols=2, figsize=(20,6))
    bar1 = sns.barplot(x='name', y='daily_views', data=byDept_df.head(n_results), ax=ax1)
    for item in bar1.get_xticklabels():
        item.set_rotation(90)
    bar2 = sns.barplot(x='name', y='daily_actions', data=byDept_df.head(n_results), ax=ax2)
    for item in bar2.get_xticklabels():
        item.set_rotation(90)
    ax1.set_xlabel('')
    ax1.set_ylabel('Average Daily Page Views per Course')
    ax2.set_xlabel('')
    ax2.set_ylabel('Average Daily Actions per Course')
    ax1.set_title('Average Daily Page Views by Department for Top {}'.format(n_results))
    ax2.set_title('Average Daily Actions by Department for Top {}'.format(n_results))
    plt.show()
    return None
    
    
    
    
    