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
-- worker processing one job from the queue:
BEGIN;
SELECT *
	FROM queue
	WHERE NOT is_done
		AND pg_try_advisory_xact_lock('queue'::regclass::int, id)
	FOR UPDATE
	ORDER BY priority, created_at
	LIMIT 1;
<do the actual work>
UPDATE queue SET processed = true WHERE id = ...;
-- or we can even delete it (and drop "is_done")
COMMIT;

--
-- note: putting the pg_try_advisory_xact_lock() in a query with LIMIT may inadvertedly lock rows (b/c WHERE is executed before LIMIT, though pg usually only executes the WHERE expression on only as much rows as necessary), this approach works around that:
CREATE TYPE next_job_attrs_t AS (
    id integer,
    is_locked boolean
);
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

--
-- the same as the above, but returns multiple jobs in one batch (as if LIMIT 20 was used in the original query):
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
        SELECT (min(id) - 1, (min(col1), min(col2), min(col3) - 1)::sort_cols_t, false)::attrs_t AS attrs FROM queue
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
