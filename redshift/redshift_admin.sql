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

