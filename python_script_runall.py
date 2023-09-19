import os
import sqlite3
import logging
import csv
import json
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, Float, DateTime, String

date_string = datetime.now().strftime("%Y-%m-%d")

FORMAT = "%(levelname)s:%(name)s:%(asctime)s:%(message)s"
logging.basicConfig(filename=f"logs/database/{date_string}_database.log", filemode="a", level=logging.DEBUG, format=FORMAT)
logger = logging.getLogger(__name__)


def contact_info_address(val):
    new_dict = json.loads(val)
    mailing_address = new_dict.get('mailing_address')
    return mailing_address


def contact_info_email(val):
    new_dict = json.loads(val)
    email = new_dict.get('email')
    return email


def convert_to_int(val):
    new_val = int(val.split('.')[0])
    return new_val


conn = sqlite3.connect("subscriber-pipeline-starter-kit/dev/cademycode_updated.db")

df_students = pd.read_sql_query("SELECT DISTINCT * FROM cademycode_students", conn)
df_courses = pd.read_sql_query("SELECT DISTINCT * FROM cademycode_courses", conn)
df_student_jobs = pd.read_sql_query("SELECT DISTINCT * FROM cademycode_student_jobs", conn)

df_students['dob'] = df_students.dob.astype('datetime64[ns]')

df_students['mailing_address'] = df_students.contact_info.apply(contact_info_address)
df_students['email'] = df_students.contact_info.apply(contact_info_email)
df_students = df_students.drop(columns=['contact_info'])

df_students['job_id'] = df_students.job_id.fillna('0.0')
df_students['job_id'] = df_students.job_id.apply(convert_to_int)

df_students['current_career_path_id'] = df_students.current_career_path_id.fillna('11.0')
df_students['current_career_path_id'] = df_students.current_career_path_id.apply(convert_to_int)
df_students = df_students.rename(columns={'current_career_path_id': 'career_path_id'})

df_students['num_course_taken'] = df_students.num_course_taken.fillna('0.0')
df_students['num_course_taken'] = df_students.num_course_taken.apply(convert_to_int)

df_students['time_spent_hrs'] = df_students.time_spent_hrs.fillna('0.0')
df_students['time_spent_hrs'] = df_students.time_spent_hrs.astype(np.float64)

df_courses.loc[len(df_courses)] = [11, 'Other', 20]

df_final = pd.merge(df_students, df_student_jobs, how='inner', on='job_id')
df_final = pd.merge(df_final, df_courses, how='inner', on='career_path_id')

df_final = df_final.reindex(columns=['uuid', 'name', 'dob', 'sex', 'mailing_address', 
                   'email', 'job_id', 'job_category', 'avg_salary', 
                   'num_course_taken', 'time_spent_hrs', 'career_path_id', 
                   'career_path_name', 'hours_to_complete'])

final_cols = {'uuid': 'user_id', 'name': 'full_name', 'dob': 'date_of_birth'}
df_final = df_final.rename(columns=final_cols)

df_final.to_csv("data_dev/subscriber_data_clean.csv", sep="|", index=False)

Base = declarative_base()


class Subscriber(Base):

    __tablename__ = "subscribers"

    user_id = Column(Integer, primary_key=True)
    full_name = Column(String)
    date_of_birth = Column(DateTime)
    sex = Column(String)
    mailing_address = Column(String)
    email = Column(String)
    job_id = Column(Integer)
    job_category = Column(String)
    avg_salary = Column(Integer)
    num_course_taken = Column(Integer)
    time_spent_hrs = Column(Float)
    career_path_id = Column(Integer)
    career_path_name = Column(String)
    hours_to_complete = Column(Integer)
    

try:
    os.remove("data_dev/cademycode_analytics.db")
except FileNotFoundError:
    pass

engine = create_engine("sqlite:///data_dev/cademycode_analytics.db")
Base.metadata.create_all(engine)

conn_new = sqlite3.connect("data_dev/cademycode_analytics.db")
df_final.to_sql(name='subscribers', con=conn_new, if_exists='append', index=False)

logger.info(f"The final subscribers table was updated with {len(df_final)} records to the database.")
