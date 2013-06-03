-- ideas:
-- * using ctid and not id
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

CREATE TABLE queue (
	id serial,
	is_done boolean,
	priority integer,
	created_at timestamp,
	...
);
CREATE TYPE next_job_attrs_t AS (
    id integer,
    is_locked boolean
);

-- worker processing one job from the queue:

-- note: putting the pg_try_advisory_xact_lock() in a query with LIMIT may inadvertedly lock rows (b/c WHERE is executed before LIMIT, though pg usually only executes the WHERE expression on only as much rows as necessary), this approach works around that:
WITH RECURSIVE
    next_job AS (
        SELECT ((SELECT min(id) - 1  FROM queue), false)::next_job_attrs_t AS attrs
        UNION ALL
        SELECT
                (
                    SELECT (min(id), pg_try_advisory_xact_lock('queue'::regclass::int, min(id)))::next_job_attrs_t AS attrs
                        FROM queue
                        WHERE NOT is_done AND id > (next_job.attrs).id
                )
            FROM next_job
            WHERE NOT (next_job.attrs).is_locked
    )
SELECT queue.*
    FROM queue
        JOIN next_job
            ON ((next_job.attrs).id = queue.id AND (next_job.attrs).is_locked)
    FOR UPDATE;


<do the actual work>

UPDATE queue SET processed = true WHERE id = ...; -- or you can even delete it (and drop "is_done" from the table definition)

COMMIT;

--
-- the same as the above, but returns multiple (0..20) jobs in one batch:
--
CREATE TYPE next_job_attrs_t AS (
    id integer,
    is_locked boolean
);
WITH RECURSIVE
    next_job AS (
        SELECT ((SELECT min(id) - 1  FROM queue), false)::next_job_attrs_t AS attrs, 1 AS rank
        UNION ALL
        SELECT
                (
                    SELECT (min(id), pg_try_advisory_xact_lock('queue'::regclass::int, min(id)))::next_job_attrs_t AS attrs
                        FROM queue
                        WHERE NOT is_done AND id > (next_job.attrs).id
                ),
                CASE
                    WHEN (next_job.attrs).is_locked THEN next_job.rank + 1
                    ELSE next_job.rank
                END AS rank
            FROM next_job
            WHERE (next_job.attrs).id IS NOT NULL AND next_job.rank < 20
    )
SELECT queue.*
    FROM queue
        JOIN next_job
            ON ((next_job.attrs).id = queue.id AND (next_job.attrs).is_locked)
    FOR UPDATE;

--
-- getting the next job from the queue, but ordered by (col1, col2, col2) and not id:
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

--
-- getting the next 20 jobs from the queue, but ordered by (col1, col2, col2) and not id:
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
                                LIMIT 1
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
-- getting the next 20 jobs from the queue, but ordered and grouped by (col1, col2, col3) and then ordered by (col4, col5, col6):
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
                                LIMIT 1
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
