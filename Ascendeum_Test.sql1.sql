WITH ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY resource_id, date ORDER BY start_time) AS rn
  FROM test_dev
  WHERE is_clock_in = 1 OR is_clock_out = 1
),
paired_sessions AS (
  SELECT
    ci.resource_id,
    ci.date,
    ci.start_time AS clock_in_time,
    co.end_time AS clock_out_time,
    (julianday(co.end_time) - julianday(ci.start_time)) * 24 AS duration_hours
  FROM ranked ci
  JOIN ranked co
    ON ci.resource_id = co.resource_id
   AND ci.date = co.date
   AND ci.rn + 1 = co.rn
   AND ci.is_clock_in = 1
   AND co.is_clock_out = 1
)
SELECT
  resource_id,
  date,
  MIN(clock_in_time) AS clock_in_time,
  MAX(clock_out_time) AS clock_out_time,
  ROUND(SUM(duration_hours), 2) AS total_hours
FROM paired_sessions
GROUP BY resource_id, date;
