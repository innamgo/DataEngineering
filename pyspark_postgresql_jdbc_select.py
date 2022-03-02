#!/usr/bin/env python
# coding: utf-8

import sys
import json
import boto3
from awsglue.gluetypes import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import Row
from pyspark.sql.types import *

try:
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
except Exception as ex:
        print('Exception : '+str(ex))
        glueContext = GlueContext(spark.sparkContext)


source_db_schema = 'public'
source_table_name = 'source_table'
target_db_schema = 'test_schema'
target_table_name = 'test_table'
logger = glueContext.get_logger()

def getSql():
    use_sql = """
    (
            SELECT
        		 *
            FROM	public.test
            ) tmp 
        """.strip('\n').replace('\n', ' ')
    logger.info('use_sql : ' + use_sql)
    return use_sql

sub_folder_name = '/raw/test/'
s3_output_full = "s3://test/"
db_url = 'jdbc:postgresql://1111111.ap-northeast-2.rds.amazonaws.com:1234/testdb'
db_username = 'id'
db_password = 'pass'
jdbc_driver_name = 'org.postgresql.Driver'


df = glueContext.read.format("jdbc").option("driver", jdbc_driver_name).option("url", db_url).option("dbtable", getSql()).option("user", db_username).option("password", db_password).load()




