-- ideas:
-- * always add id or ctid to sort_cols_t, so that it will fetch all rows with the same values for col1, ..., colN
-- * if all cols are NOT NULL in the sort_cols_t, do not create a composite type, but use a plain ROW(col1, ..., colN) - that can use a plain B-tree index on col1, ..., colN -, and use the plain list of cols in ORDER BY
-- * instead of using composite types, write out the comparisons (eg. (col1A, col2A, col3A)::sort_cols_t > (col1B, col2B, col3B)::sort_cols_t -> (col1A > col1B OR ((col1A = col1B OR (col1A IS NULL AND col1B IS NULL)) AND (col2A > col2B OR ((col2A = col2B OR (col2A IS NULL AND col2B IS NULL)) AND (col3A > col3B))))) ) and allow "ASC/DESC [NULLS FIRST|LAST]" in the ORDER BY
--  * pro:
--   * makes possible to order different columns differently (ASC/DESC per column)
--   * makes possible to use volatile functions in the sort "column" defs (cannot create an index for a composite type value containing expressions containing volatile functions; but if the index is on an expression not containing the volatile function but it's compared to the volatile function, it can use an index)
--    * example:
--     * this cannot use an index: (col1A, col2A + '3 days'::interval < current_timestamp)::sort_cols_t < (col1B, col2B + '3 days'::interval < current_timestamp)::sort_cols_t
--     * but this can use indexes (one on col1, another on (col2 + '3 days'::interval)): col1A < col1B OR (col1A = col2A AND (col2A + '3 days'::interval < current_timestamp) < (col2B + '3 days'::interval < current_timestamp))
--  * con: that cannot use an index efficiently OTOH an index on sort_cols_t can be used
--   * example:
--    * this requires only a single B-tree index scan (and it's used for the lookup of all cols): (col1A, col2A)::sort_cols_t < (col1B, col2B)::sort_cols_t
--    * this requires 6 B-tree index scans (and the number of scans increases with the number of cols): col1A < col2A OR ((col1A = col2A OR (col1A IS NULL AND col1B IS NULL)) AND col2A < col2B)
-- * using ctid and not id
-- * fetch more than one row in each table/index scan (ie. "LIMIT 20 - next_job.rank + 1" instead of "LIMIT 1" in the recursive CTE parts, but also calculating the rank must be fixed, and then the current select-list-subquery form cannot return more than one row)
--TODO: documentation
-- * not a full-fledged message queue system, lacking lots of features (not distributed, prolly not as effective - never benchmarked it, etc.)
-- * but for simple tasks it's good enough, and one can avoid using one more middleware component
-- * and where the origin of the jobs is in the db, it makes sense to keep the jobs in the db
-- * workflows
--  * when the processing of a job is fast: the one documented below
--      * open the txn
--      * lock the rows using txn-level advisory locks and FOR UPDATE
--      * process the jobs
--      * UPDATE the state of the jobs to done (or DELETE them)
--      * commit
--      * it can be augmented with LISTEN/NOTIFY (the client LISTENs, and a trigger on the job queue table sends NOTIFYs when new jobs are inserted)
--  * when the processing of a job takes a lot of time: avoid keeping the txn open for long, instead 2 txns
--      * first txn: lock the rows using session level advisory locks
--      * process the jobs
--      * second txn: UPDATE the jobs to done (or DELETE them)
--      * it can be augmented with LISTEN/NOTIFY (the client LISTENs, and a trigger on the job queue table sends NOTIFYs when new jobs are inserted)
--TODO: design
-- * the job queue table is created by the user (flexibility, allow integration to existing systems, etc.)
--  * but also provide a function to generate it, so that one can set up everything from a sproc easily:
--      * create_job_queue_table(table_schema text, table_name text, columns text[])
-- * the extension provides a set of functions
--  * functions that can generate the actual job fetching function(s) (they generate the function, the composite types, the index on the composite type on the sort cols if missing, makes sure pk_columns is indeed a PK)
--      * generate_fetcher_for_single_job(function_schema text, function_name text, queue_table regclass, where_condition text, pk_columns text[], sort_columns text[])
--      * generate_fetcher_for_single_job(function_name text, queue_table regclass, where_condition text, pk_columns text[], sort_columns text[])
--          * if function_schema is missing/NULL then uses unqualified name (ie. uses the first elem of search_path)
--          * if queue_schema is NULL then uses unqualified name (ie. uses search_path)
--      * generate_fetcher_for_multiple_jobs(function_schema text, function_name text, queue_table regclass, where_condition text, pk_columns text[], group_and_sort_columns text[], sort_columns text[])
--      * generate_fetcher_for_multiple_jobs(function_name text, queue_table regclass, where_condition text, pk_columns text[], group_and_sort_columns text[], sort_columns text[])
--      * generate_cleanup_daemon_function(function_schema text, function_name text, queue_table regclass, where_condition text, pk_columns text[], state_filter text)
--  * functions that just return the SQL statements to generate the fetching functions (ie. output of these is EXECUTE'd in the actual generator functions)
--TODO: testing
-- testing for parameter validation
-- testing for parameter defaults
-- testing for PK checking
-- testing for index checking/creation
-- testing usage of the where condition
-- testing for ordering of the returned rows
-- testing for the number of rows returned
--  * when there are less than N rows
--  * when there are exactly N rows
--  * when there are more than N rows
-- testing for group+sort cols usage
-- testing if rows are advisory-locked (looking up locks in pg_catalog)
-- testing if rows are FOR UPDATE locked (looking up locks in pg_catalog)
-- testing for index usage (looking up index usage in pg_catalog)
-- testing concurrently fetching rows (using dblink for concurrent queries)
-- testing 


-- implementing a job queue with parallel workers:
-- (makes sure that the same job is only processed by one worker and if a worker
--  dies the job can be picked up by another worker, and workers do not block on
--  waiting for each other)
-- note: putting the pg_try_advisory_xact_lock() in a query with LIMIT may inadvertedly lock rows (b/c WHERE is executed before LIMIT, though pg usually only executes the WHERE expression on only as much rows as necessary), this approach works around that

CREATE TABLE queue (
	id serial,
	is_done boolean,
	priority integer,
	created_at timestamp,
	...
);
CREATE TYPE sort_cols_t AS (
    col1 integer,
    col2 integer,
    col3 integer
);
CREATE TYPE attrs_t AS (
    id integer,
    cols sort_cols_t,
    is_locked boolean
);

-- worker processing one job from the queue, jobs are prioritized by (col1, col2, col3):

BEGIN;

WITH RECURSIVE
    next_job AS (
        SELECT attrs
            FROM (
                SELECT (id, (col1, col2, col3 - 1)::sort_cols_t, false)::attrs_t AS attrs
                    FROM queue
                    ORDER BY col1, col2, col3
                    LIMIT 1
            )
        UNION ALL
        SELECT
                (
                    SELECT (id, cols, pg_try_advisory_xact_lock('queue'::regclass::int, id))::attrs_t
                        FROM (
                            SELECT id, cols, is_done
                                FROM (
                                    SELECT id, (col1, col2, col3)::sort_cols_t AS cols, is_done
                                        FROM queue
                                ) AS q
                                WHERE NOT is_done
                                    AND cols > (next_job.attrs).cols
                                ORDER BY cols
                                LIMIT 1
                        ) AS q
                ) AS attrs
            FROM next_job
            WHERE NOT (next_job.attrs).is_locked
    )
SELECT queue.*
    FROM queue
        JOIN next_job
            ON ((next_job.attrs).id = queue.id AND (next_job.attrs).is_locked)
    ORDER BY (next_job.attrs).cols
    FOR UPDATE;

<do the actual work>

UPDATE queue SET processed = true WHERE id = ...; -- or you can even delete it (and drop "is_done" from the table definition)

COMMIT;

--
-- getting the next 20 jobs from the queue, prioritized by (col1, col2, col2):
--
CREATE TYPE sort_cols_t AS (
    col1 integer,
    col2 integer,
    col3 integer
);
CREATE TYPE attrs_t AS (
    id integer,
    cols sort_cols_t,
    is_locked boolean
);
WITH RECURSIVE
    next_job AS (
        SELECT (min(id) - 1, (min(col1), min(col2), min(col3) - 1)::sort_cols_t, false)::attrs_t AS attrs, 1 AS rank
            FROM queue
        UNION ALL
        SELECT
                (
                    SELECT (id, cols, pg_try_advisory_xact_lock('queue'::regclass::int, id))::attrs_t
                        FROM (
                            SELECT id, cols, is_done
                                FROM (
                                    SELECT id, (col1, col2, col3)::sort_cols_t AS cols, is_done
                                        FROM queue
                                ) AS q
                                WHERE NOT is_done
                                    AND cols > (next_job.attrs).cols
                                ORDER BY cols
                                LIMIT 20
                        ) AS q
                ) AS attrs,
                CASE
                    WHEN (next_job.attrs).is_locked THEN rank + 1
                    ELSE rank
                END AS rank
            FROM next_job
            WHERE (next_job.attrs).id IS NOT NULL AND next_job.rank < 20
    )
SELECT queue.*
    FROM queue
        JOIN next_job
            ON ((next_job.attrs).id = queue.id AND (next_job.attrs).is_locked)
    ORDER BY (next_job.attrs).cols
    FOR UPDATE;

--
-- getting the next 20 jobs from the queue, prioritized by and grouped by (col1, col2, col3) and then prioritized by (col4, col5, col6):
-- the grouping means that all the rows returned will have the same values for (col1, col2, col3), and 
--
CREATE TYPE group_cols_t AS (
    col1 integer,
    col2 integer,
    col3 integer
);
CREATE TYPE sort_cols_t AS (
    col1 integer,
    col2 integer,
    col3 integer,
    col4 integer,
    col5 integer,
    col6 integer
);
CREATE TYPE attrs_t AS (
    id integer,
    group_cols group_cols_t,
    sort_cols sort_cols_t,
    is_locked boolean
);
WITH RECURSIVE
    next_job AS (
        SELECT attrs, 1 AS rank
            FROM (
                SELECT (id - 1, (col1, col2, col3 - 1)::group_cols_t,
                        (col1, col2, col3, col4, col5, col6 - 1)::sort_cols_t, false)::attrs_t AS attrs
                    FROM queue
                    ORDER BY (col1, col2, col3, col4, col5, col6)::sort_cols_t
                    LIMIT 1
            ) AS q
        UNION ALL
        SELECT
                (
                    SELECT (id, group_cols, sort_cols, pg_try_advisory_xact_lock('queue'::regclass::int, id))::attrs_t
                        FROM (
                            SELECT id, group_cols, sort_cols, is_done
                                FROM (
                                    SELECT id, (col1, col2, col3)::group_cols_t AS group_cols,
                                            (col1, col2, col3, col4, col5, col6)::sort_cols_t AS sort_cols, is_done
                                        FROM queue
                                ) AS q
                                WHERE NOT is_done
                                    AND sort_cols > (next_job.attrs).sort_cols
                                    AND (next_job.rank = 1 AND NOT (next_job.attrs).is_locked OR (next_job.attrs).group_cols = group_cols)
                                ORDER BY sort_cols
                                LIMIT 20
                        ) AS q
                ) AS attrs,
                CASE
                    WHEN (next_job.attrs).is_locked THEN rank + 1
                    ELSE rank
                END AS rank
            FROM next_job
            WHERE (next_job.attrs).id IS NOT NULL AND next_job.rank < 20
    )
SELECT queue.*
    FROM queue
        JOIN next_job
            ON ((next_job.attrs).id = queue.id AND (next_job.attrs).is_locked)
    ORDER BY (next_job.attrs).sort_cols
    FOR UPDATE;
