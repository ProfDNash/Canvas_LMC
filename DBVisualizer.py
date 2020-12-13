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
query = db.select([req])
result_proxy = conn.execute(query)
result_set = result_proxy.fetchall()
col_names = ['index','day','hour','course_id','user_id','daily_views','daily_actions']
req_df = pd.DataFrame(result_set, columns=col_names)

def hourlyViz():
    hourly_query = db.select([db.func.sum(req.columns.daily_views).label('hourly_views'),
                          db.func.sum(req.columns.daily_actions).label('hourly_actions')]).group_by(req.columns.hour)
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
    byClass_query = db.select([courses.columns.code, db.func.sum(req.columns.daily_views),
                               db.func.sum(req.columns.daily_actions)]).where(
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
    