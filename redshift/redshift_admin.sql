1. getting data skew table list.

WITH base_skew AS (
    SELECT id as tbl FROM stv_tbl_perm GROUP BY id
    HAVING SUM(rows) > (SELECT 1000000 * ( MAX(node) + 1) FROM stv_slices)
        AND (MAX(rows) / (MIN(rows) + 1.0)) >= 2.0
        AND SUM(rows) <> (MAX(rows) * (SELECT MAX(node) + 1 FROM stv_slices))
),
table_slices AS (
    SELECT t.slice, t.id, COUNT(b.blocknum) s_mb, COUNT(DISTINCT col) * COUNT(DISTINCT unsorted) ss_mb
    FROM stv_tbl_perm t LEFT JOIN stv_blocklist b
        ON (t.id = b.tbl AND t.slice = b.slice AND b.temporary = 0 AND b.tombstone = 0)
    WHERE t.slice < 6400 AND t.temp = 0 AND t.id IN (SELECT tbl FROM base_skew) GROUP BY t.id, t.slice
),
skew_dist AS (
    SELECT slice, id AS tbl, SUM(s_mb) AS total_size_mb, MAX(ss_mb) AS slice_size_mb,
        SUM(s_mb)::float - AVG(SUM(s_mb)::float) OVER (PARTITION BY tbl) AS dist_from_mean
    FROM table_slices GROUP BY tbl, slice
),
skew_degree AS (
    SELECT tbl, SUM(total_size_mb) table_size_mb, MIN(total_size_mb) min_slice_mb,
        MAX(total_size_mb) max_slice_mb, MAX(slice_size_mb) slice_size_mb,
        COALESCE(SUM(dist_from_mean^2)::float / NULLIF(SUM(total_size_mb^2)::float,0),0) AS slice_skew_degree
    FROM skew_dist GROUP BY tbl
)
SELECT
    schema || '.' || "table" AS tablename, diststyle, ROUND(slice_skew_degree, 5) AS slice_skew_degree,
    min_slice_mb, max_slice_mb, table_size_mb, skew_rows,
    CASE WHEN max_slice_mb>slice_size_mb*10 THEN '*' ELSE '' END as advisor_flagged
FROM skew_degree JOIN svv_table_info ti ON ti.table_id = tbl
WHERE slice_skew_degree >= 0.50 ORDER BY advisor_flagged DESC, slice_skew_degree DESC;

2. get privilege by user name

SELECT
	*
	,(case when sel = True and ins = True 
    		and upd = True and del = True
    		and ref = True then 'grant all on table '||schemaname||'.'||tablename||' to '||usename||';'
     	  when sel = True and ins = False 
    		and upd = False and del = False
    		and ref = False then 'grant select on table '||schemaname||'.'||tablename||' to '||usename||';'
    	  when sel = True and ins = True 
    		and upd = False and del = False
    		and ref = False then 'grant select,insert on table '||schemaname||'.'||tablename||' to '||usename||';'
    	  when sel = True and ins = True 
    		and upd = True and del = False
    		and ref = False then 'grant select,insert,update on table '||schemaname||'.'||tablename||' to '||usename||';'
    	  when sel = True and ins = True 
    		and upd = True and del = True
    		and ref = False then 'grant select,insert,update,delete on table '||schemaname||'.'||tablename||' to '||usename||';'
      else '-' end) as grant_sql
FROM 
	(
	SELECT 
		schemaname
		,tablename
		,usename
		,HAS_TABLE_PRIVILEGE(u.usename, obj, 'select') AS sel
		,HAS_TABLE_PRIVILEGE(u.usename, obj, 'insert') AS ins
		,HAS_TABLE_PRIVILEGE(u.usename, obj, 'update') AS upd
		,HAS_TABLE_PRIVILEGE(u.usename, obj, 'delete') AS del
		,HAS_TABLE_PRIVILEGE(u.usename, obj, 'references') AS ref
	FROM
		(SELECT schemaname, tablename, '\"' + schemaname + '\"' + '.' + '\"' + tablename + '\"' AS obj FROM pg_tables 
			where schemaname not in ('pg_internal') and tablename in ('table_name_test') ) AS t
		,(SELECT * FROM pg_user ) AS u
	ORDER BY obj
	)
WHERE sel = true or ins = true or upd = true or del = true or ref = true;
         
3. alter distkey, sortkey
alter table schema_name.table_name alter diststyle key distkey column_name;
alter table schema_name.table_name alter compound sortkey (column_name);
alter table schema_name.table_name alter interleaved sortkey (column_name);
		 
		 
4. user
create user user_id password 'test_pass';
ALTER USER user_id SET timezone to 'Asia/Seoul';
ALTER USER user_id SET search_path to 'test_schema','test_schema1','test_schema2';

SELECT 
	pg_group.groname
	,pg_group.grosysid
	,pg_user.*
FROM pg_group, pg_user 
WHERE pg_user.usesysid = ANY(pg_group.grolist) 
ORDER BY 1,2 

5. Grant, Revoke Role
--사용자에 적용된 Role 보기
select * from svv_user_grants;

--사용자를 Role에서 제외하기
REVOKE Role masking_role FROM pssdev;

show grants for pssdev;

6. Grant, Revoke Column Select

grant select on table 스키마.테이블 to 유저명;
grant select (col1, col2) on table 스키마.테이블 to 유저명;
revoke select on 스키마.테이블 from 유저명;
revoke select (col1, col2) on 스키마.테이블 from 유저명;
