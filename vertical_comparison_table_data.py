"""
DB에 생성할 테이블 DDL
CREATE TABLE IF NOT EXISTS mzc.inspect_table_data_condition
(
  source VARCHAR(10)   ENCODE lzo
  ,dbschema VARCHAR(100)   ENCODE lzo
  ,tablenm VARCHAR(100)   ENCODE lzo
  ,col VARCHAR(100)   ENCODE lzo
  ,grouping_col VARCHAR(500)   ENCODE lzo
  ,where_condition VARCHAR(4000)   ENCODE lzo
  ,inspect_yn VARCHAR(10)   ENCODE lzo
  ,insertdt TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;
CREATE TABLE IF NOT EXISTS mzc.inspect_table_data
(
  insertdt TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
  ,schema_name VARCHAR(100)   ENCODE lzo
  ,table_name VARCHAR(100)   ENCODE lzo
  ,col_name VARCHAR(100)   ENCODE lzo
  ,measure_base VARCHAR(100)   ENCODE lzo
  ,variable VARCHAR(100)   ENCODE lzo
  ,value VARCHAR(4000)   ENCODE lzo
)
DISTSTYLE AUTO
;
CREATE TABLE IF NOT EXISTS mzc.inspect_table_data_iias
(
  insertdt TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
  ,schema_name VARCHAR(100)   ENCODE lzo
  ,table_name VARCHAR(100)   ENCODE lzo
  ,col_name VARCHAR(100)   ENCODE lzo
  ,measure_base VARCHAR(100)   ENCODE lzo
  ,variable VARCHAR(100)   ENCODE lzo
  ,value VARCHAR(4000)   ENCODE lzo
)
DISTSTYLE AUTO
;
"""

import ibm_db
import ibm_db_dbi
import psycopg2
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import decimal as dc
import numpy as np
import os
import datetime
import time
import openpyxl
from openpyxl.styles import PatternFill, Border, Side, Font, Alignment

UTCday = datetime.datetime.today() + datetime.timedelta(hours=9)
CreateDate = datetime.datetime.today().strftime('%Y%m%d')
saveInsertDt = time.mktime(UTCday.timetuple())
InsertDt = pd.Timestamp.now()

# 최대 줄 수 설정
pd.set_option('display.max_rows', None)
# 최대 열 수 설정
pd.set_option('display.max_columns', None)
# 표시할 가로의 길이
pd.set_option('display.width', None)

# -------------------------------------------------------------------------------------------------------------
# Connection Set
# -------------------------------------------------------------------------------------------------------------
dictDb2Conn = {}
dictDb2Conn['db_username'] = ''
dictDb2Conn['db_password'] = ''
dictDb2Conn['db_url'] = ''
dictDb2Conn['port'] = ''
dictDb2Conn['database_name'] = ''

dictRedshiftConn = {}
dictRedshiftConn['db_username'] = ''
dictRedshiftConn['db_password'] = ''
dictRedshiftConn['db_url'] = ''
dictRedshiftConn['port'] = ''
dictRedshiftConn['database_name'] = ''

# -------------------------------------------------------------------------------------------------------------
# Verify User Set
# -------------------------------------------------------------------------------------------------------------

strSavePath = 'D:/verify'
strJobId = "VERTICAL_INSPECT"
strVerifySchema = "schema"






strVerifyTable = ""
strVerifyScope = ""
strCliProfile = '--profile xxxx'
SourceDb = 'Redshift'

if not os.path.exists('{}/{}/'.format(strSavePath, strJobId)):
    os.makedirs('{}/{}/'.format(strSavePath, strJobId))


def make_pandas_schema(col_nm, col_type, precision=0, scale=0, coerce_time='ns'):
    """
    pandas dataframe 스키마 적용
    :param col_nm: 컬럼명
    :param col_type: 컬럼타입
    :param precision: decimal precision
    :param scale: decimal scale
    :param coerce_time: datetime format
    :return: pandas 스키마 컬럼 타입
    """
    if col_type == 'str':
        return pa.field(col_nm, pa.string())
    elif col_type == 'int16':
        return pa.field(col_nm, pa.int16())
    elif col_type == 'int32':
        return pa.field(col_nm, pa.int32())
    elif col_type == 'int64':
        return pa.field(col_nm, pa.int64())
    elif col_type == 'decimal':
        return pa.field(col_nm, pa.decimal128(precision, scale))
    elif col_type == 'date':
        return pa.field(col_nm, pa.date32())
    elif col_type == 'timestamp':
        return pa.field(col_nm, pa.timestamp('ns'))
    elif col_type == 'float32':
        return pa.field(col_nm, pa.float32())
    elif col_type == 'float64':
        return pa.field(col_nm, pa.float64())
    else:
        return pa.field(col_nm, pa.string())

def get_inspect_list():
    get_list_sql = """
    SELECT 	ordinal_position ,table_schema,table_name, column_name
          , (CASE WHEN data_type IN ('character varying','text') 
          			THEN 'SELECT MAX(UPPER('''||dbschema||''')) SCHEMA_NAME,'||'MAX(UPPER('''||tablenm||''')) TABLE_NAME,'||'MAX(UPPER('''||column_name||''')) COL_NAME,'||'TO_CHAR('||grouping_col||',''YYYY-MM'') MEASURE_BASE'||', CAST(COUNT('||column_name||
          					') AS VARCHAR) MEASURE_COUNT, CAST(SUM(0) AS VARCHAR) MEASURE_SUM, CAST(FLOOR(AVG(0)) AS VARCHAR) MEASURE_AVG, CAST(MIN('||column_name||
          					') AS VARCHAR) MEASURE_MIN, CAST(MAX('||column_name||') AS VARCHAR) MEASURE_MAX, CAST(SUM(CASE WHEN '||column_name||' IS NULL THEN 1 ELSE 0 END) AS VARCHAR) MEASURE_NULLCOUNT '||
          					'from '||dbschema||'.'||tablenm||' group by TO_CHAR('||grouping_col||',''YYYY-MM'')'
          	      WHEN data_type IN ('numeric', 'bigint', 'integer','double precision','smallint') 
           	        THEN 'SELECT MAX(UPPER('''||dbschema||''')) SCHEMA_NAME,'||'MAX(UPPER('''||tablenm||''')) TABLE_NAME,'||'MAX(UPPER('''||column_name||''')) COL_NAME,'||'TO_CHAR('||grouping_col||',''YYYY-MM'') MEASURE_BASE'||', CAST(COUNT('||column_name||
           	        		') AS VARCHAR) MEASURE_COUNT, CAST(SUM('||column_name||') AS VARCHAR) MEASURE_SUM, CAST(FLOOR(AVG('||column_name||
           	        		')) AS VARCHAR) MEASURE_AVG, CAST(MIN('||column_name||') AS VARCHAR) MEASURE_MIN, CAST(MAX('||column_name||') AS VARCHAR) MEASURE_MAX, CAST(SUM(CASE WHEN '||column_name||
           	        		' IS NULL THEN 1 ELSE 0 END) AS VARCHAR) MEASURE_NULLCOUNT '||
           	        		'from '||dbschema||'.'||tablenm||' group by TO_CHAR('||grouping_col||',''YYYY-MM'')'
           	      WHEN data_type IN ('date', 'timestamp without time zone') 
           	        THEN 'SELECT MAX(UPPER('''||dbschema||''')) SCHEMA_NAME,'||'MAX(UPPER('''||tablenm||''')) TABLE_NAME,'||'MAX(UPPER('''||column_name||''')) COL_NAME,'||'TO_CHAR('||grouping_col||',''YYYY-MM'') MEASURE_BASE'||', CAST(COUNT('||column_name||
           	        		') AS VARCHAR) MEASURE_COUNT, CAST(SUM(0) AS VARCHAR) MEASURE_SUM, CAST(FLOOR(AVG(0)) AS VARCHAR) MEASURE_AVG, MIN(TO_CHAR('||column_name||
           	        		',''YYYY-MM-DD HH24:MI:SS.US'')) MEASURE_MIN, MAX(TO_CHAR('||column_name||',''YYYY-MM-DD HH24:MI:SS.US'')) MEASURE_MAX, CAST(SUM(CASE WHEN '
           	        		||column_name||' IS NULL THEN 1 ELSE 0 END) AS VARCHAR) MEASURE_NULLCOUNT '
           	        		||'from '||dbschema||'.'||tablenm||' group by TO_CHAR('||grouping_col||',''YYYY-MM'')'
           	 ELSE '' END) MEASURES
      FROM SVV_COLUMNS sc
      INNER JOIN MZC.inspect_table_data_condition itdc
      ON upper(sc.table_schema) = upper(itdc.dbschema) and upper(sc.table_name) = upper(itdc.tablenm) and upper(sc.column_name) = upper(itdc.col)
     WHERE 1=1
     --and itdc.inspect_yn='Y'
     and upper(itdc.tablenm)='{}'
      order by table_name, ordinal_position asc
    """.format(strVerifyTable)
    return get_list_sql

try:
    ibm_db_conn = ibm_db.connect("DATABASE={};HOSTNAME={};PORT={};PROTOCOL=TCPIP;UID={};PWD={}".format(dictDb2Conn['database_name'], dictDb2Conn['db_url'], dictDb2Conn['port'], dictDb2Conn['db_username'], dictDb2Conn['db_password']), "", "")
    db2_conn = ibm_db_dbi.Connection(ibm_db_conn)
    redshift_conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(dictRedshiftConn['database_name'],dictRedshiftConn['db_url'],dictRedshiftConn['port'],dictRedshiftConn['db_username'],dictRedshiftConn['db_password']))
    redshift_conn.autocommit = True
    redshift_cursor = redshift_conn.cursor()
    #redshift_cursor.execute(get_inspect_list())

    df = pd.read_sql(get_inspect_list(), redshift_conn, coerce_float=False)
    print(df['measures'].head(4))
    union_sql = ''
    schema_list = []
    schema_list.append(make_pandas_schema('InsertDt'.lower(), 'timestamp'))
    schema_list.append(make_pandas_schema('schema_name'.upper(), 'str'))
    schema_list.append(make_pandas_schema('table_name'.upper(), 'str'))
    schema_list.append(make_pandas_schema('col_name'.upper(), 'str'))
    schema_list.append(make_pandas_schema('measure_base'.upper(), 'str'))
    schema_list.append(make_pandas_schema('variable'.lower(), 'str'))
    schema_list.append(make_pandas_schema('value'.lower(), 'str'))

    columns = ['insertdt','SCHEMA_NAME', 'TABLE_NAME', 'COL_NAME', 'MEASURE_BASE', 'variable', 'value']
    all_result_unpivoted_df = pd.DataFrame(columns=columns)
    startTime = time.time()
    for i, row in enumerate(df['measures']):
        union_sql += row
        if i < len(df)-1:
            union_sql += "\n UNION ALL \n"
        print(row)
        result_df = pd.read_sql(row, db2_conn, coerce_float=False)
        result_df['insertdt'] = InsertDt
        print(result_df.head(10))
        result_unpivoted_df = result_df.melt(id_vars=['insertdt','SCHEMA_NAME', 'TABLE_NAME', 'COL_NAME', 'MEASURE_BASE'],
                                                     value_vars=['MEASURE_COUNT', 'MEASURE_SUM', 'MEASURE_AVG',
                                                                 'MEASURE_MIN', 'MEASURE_MAX', 'MEASURE_NULLCOUNT'])
        all_result_unpivoted_df=all_result_unpivoted_df.append(result_unpivoted_df)
        print(result_unpivoted_df.head(100))

    saveParquetFileFullPath = '{0}/{1}/{2}.{3}.{4}.{5}.snappy.parquet'.format(strSavePath, strJobId,
                                                                                  strVerifyTable.upper(), saveInsertDt,
                                                                                  len(all_result_unpivoted_df),
                                                                                  SourceDb)
    pandas_schema = pa.schema(schema_list)
    table = pa.Table.from_pandas(all_result_unpivoted_df, schema=pandas_schema, preserve_index=False)
    writer = pq.ParquetWriter(saveParquetFileFullPath, table.schema, use_deprecated_int96_timestamps=True,
                              compression='snappy')
    writer.write_table(table)
    if writer:
        writer.close()

    print(union_sql)
    fSrc = open('{}/{}/{}_IIAS.sql'.format(strSavePath, strJobId, strVerifyTable), 'w')
    fSrc.write(union_sql)
    fSrc.close()
    print('Elapsed time : ' + str(time.time() - startTime))

except Exception() as ex:
    print(ex)

finally:
    redshift_cursor.close()
    redshift_conn.close()
    db2_conn.close()
    print('Finish!!~')



