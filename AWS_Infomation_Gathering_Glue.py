#!/usr/bin/env python
# coding: utf-8

# In[3]:


get_ipython().system('pip install psycopg2')


# In[1]:


import psycopg2
import json
import boto3
import datetime
import traceback


class AWSClient:
    job_list = []
    job_run_list = []
    client = None
    def __init__(self):
        pass

    def get_boto_session_client(self, p_profile_name, p_service_name):
        try:
            session = boto3.Session(profile_name=p_profile_name)
            self.client = session.client(p_service_name)
        except Exception:
            traceback.print_exc()
        return self.client

    def get_boto_client(self, p_service_name):
        try:
            self.client = boto3.client(p_service_name)
        except Exception:
            traceback.print_exc()
        return self.client
    
    def get_job(self, p_job_name):
        self.job_response= ''
        try:
            self.job_response = self.client.get_job(JobName=p_job_name)
        except Exception:
            traceback.print_exc()
        return self.job_response

    def get_jobs(self):
        try:
            job_response = self.client.get_jobs()
            self.job_list.append(job_response)
            for i in range(100):
                if 'NextToken' in job_response.keys():
                    self.job_list.append(job_response)
                    job_response = self.client.get_jobs(NextToken=job_response['NextToken'])
                else:
                    self.job_list.append(job_response)
                    break
                print('getting job list of next page.')
        except Exception:
            traceback.print_exc()
        return self.job_list

    def get_job_runs(self, job_name):
        try:
            job_run_response = self.client.get_job_runs(JobName=job_name, MaxResults=16)
            print(job_run_response)
        except Exception:
            traceback.print_exc()
        return job_run_response

    def get_job_runs(self, job_name, max_results, loop_range):
        try:
            job_run_response = self.client.get_job_runs(JobName=job_name, MaxResults=max_results)
            self.job_run_list.append(job_run_response)
            for i in range(loop_range):
                if 'NextToken' in job_run_response.keys():
                    self.job_run_list.append(job_run_response)
                    job_run_response = self.client.get_job_runs(JobName=job_name, MaxResults=max_results, NextToken=job_run_response['NextToken'])
                else:
                    self.job_run_list.append(job_run_response)
                    break
                print(job_run_response)
        except Exception:
            traceback.print_exc()
        return self.job_run_list

class DatabaseUtility:
    def __init__(self, database_name, user_name, password, host_uri, port):
        self.database_name = database_name
        self.user_name = user_name
        self.password = password
        self.host_uri = host_uri
        self.port = port

    def json_datetime_default(self, o):
        if isinstance(o, (datetime.date, datetime.datetime)):
            return o.isoformat()

    def get_connection_cursor(self):
        try:
            conn = psycopg2.connect(database=self.database_name, user=self.user_name, password=self.password,
                                    host=self.host_uri, port=self.port)
            cur = conn.cursor()
        except Exception:
            traceback.print_exc()
        return conn, cur

    def setup_database(self, cursor):
        glue_info_table_create = """
        CREATE TABLE testetl.glue_info_json (
            "key" varchar(100) NULL,
            data_json json NULL,
            update_date timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
        try:
            cursor.execute(glue_info_table_create)
        except Exception:
            traceback.print_exc()

    def select_query(self, connection, cursor, query):
        try:
            cursor.execute(query)
            result_list = cursor.fetchall()
            connection.commit()
        except Exception:
            traceback.print_exc()
        return result_list

    def insert_job_list_query(self, connection, cursor, data_dictionary):
        for jobs in data_dictionary:
            for job_line in jobs['Jobs']:
                records = json.dumps(job_line, default=self.json_datetime_default)
                records = records.replace("'",'')
                sql = f"insert into testetl.glue_info_json(key,data_json) values ('glue_job_list','{records}')"
                if sql.find('ErrorMessage') < 0:
                    print(sql)
                    cursor.execute(sql)
        connection.commit()

    def insert_job_runs_query(self, connection, cursor, data_dictionary):
        for job_run in data_dictionary['JobRuns']:
            records = json.dumps(job_run, default=self.json_datetime_default)
            records = records.replace("'",'')
            sql = f"insert into testetl.glue_info_json(key,data_json) values ('glue_job_run','{records}')"
            if sql.find('ErrorMessage') < 0:
                print(sql)
                cursor.execute(sql)
        connection.commit()

is_gathering_job_list = True
if_gathering_job_runs = False

if is_gathering_job_list:
    awsclient = AWSClient()
    awsclient.get_boto_session_client('default','glue')
    jobs_list = awsclient.get_jobs()

    databaseUtility = DatabaseUtility(database_name="db",user_name="user",password="pass",host_uri="uri",port="port")
    conn, cursor = databaseUtility.get_connection_cursor()
    databaseUtility.insert_job_list_query(conn, cursor, jobs_list)

if if_gathering_job_runs:
    awsclient = AWSClient()
    awsclient.get_boto_client('glue')
    databaseUtility = DatabaseUtility(database_name="db", user_name="user", password="port",
                                      host_uri="uri", port="port")
    get_job_list_query = """
                        select key
                            ,data_json -> 'Command' ->> 'Name' glue_type
                            ,data_json ->> 'GlueVersion' glue_version
                            ,data_json ->> 'Name' job_name
                        from pssetl.glue_info_json 
                        where "key" ='glue_job_list' and data_json ->> 'Name' like '%test%'
                            and data_json -> 'Command' ->> 'Name' like '%testetl%'
                            and data_json ->> 'GlueVersion' = '3.0'
                            and update_date >= to_date('20211005','YYYYMMDD');
                            """
    conn, cursor = databaseUtility.get_connection_cursor()
    result_list = databaseUtility.select_query(conn, cursor, get_job_list_query)
    for i in range(len(result_list)):
        databaseUtility.insert_job_runs_query(conn, cursor, awsclient.get_job_runs(result_list[i][3]))
    cursor.close()
    conn.close()

"""
awsclient = AWSClient()
awsclient.get_boto_session_client('default','glue')
run_list = awsclient.get_job('!!!job_name')

def json_datetime_default(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()

records = json.dumps(run_list, default=json_datetime_default)
print(type(records))
records = records.replace("'",'"')
print(records)
"""


# In[ ]:




