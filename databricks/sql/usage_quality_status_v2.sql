CREATE OR REPLACE VIEW __NAMESPACE__.usage_quality_status_v2
COMMENT 'Custom SQL contract checks, not Databricks managed quality monitoring. Require PASS; missing layers and invalid keys/counts must fail.'
AS
WITH silver AS (
  SELECT use_ymd, SUM(total_count) AS silver_total_count,
         COUNT(*) - COUNT(DISTINCT struct(line_name, station_name)) AS duplicate_keys,
         SUM(CASE WHEN line_name IS NULL OR trim(line_name) = ''
                   OR station_name IS NULL OR trim(station_name) = ''
                   OR ride_count IS NULL OR alight_count IS NULL OR total_count IS NULL
                   OR ride_count < 0 OR alight_count < 0 OR total_count < 0
                   OR total_count <> ride_count + alight_count
             THEN 1 ELSE 0 END) AS invalid_rows
  FROM __NAMESPACE__.silver_subway_usage_v2 GROUP BY use_ymd
), station AS (
  SELECT use_ymd, SUM(total_count) AS station_total_count,
         COUNT(*) - COUNT(DISTINCT struct(line_name, station_name)) AS station_duplicate_keys,
         SUM(CASE WHEN line_name IS NULL OR trim(line_name) = ''
                   OR station_name IS NULL OR trim(station_name) = ''
                   OR ride_count IS NULL OR alight_count IS NULL OR total_count IS NULL
                   OR ride_count < 0 OR alight_count < 0 OR total_count < 0
                   OR total_count <> ride_count + alight_count
             THEN 1 ELSE 0 END) AS station_invalid_rows
  FROM __NAMESPACE__.gold_subway_usage_station_v2 GROUP BY use_ymd
), line AS (
  SELECT use_ymd, SUM(total_count) AS line_total_count,
         COUNT(*) - COUNT(DISTINCT line_name) AS line_duplicate_keys,
         SUM(CASE WHEN line_name IS NULL OR trim(line_name) = ''
                   OR ride_count IS NULL OR alight_count IS NULL OR total_count IS NULL
                   OR station_count IS NULL OR station_count <= 0
                   OR ride_count < 0 OR alight_count < 0 OR total_count < 0
                   OR total_count <> ride_count + alight_count
             THEN 1 ELSE 0 END) AS line_invalid_rows
  FROM __NAMESPACE__.gold_subway_usage_line_v2 GROUP BY use_ymd
), station_difference AS (
  SELECT COALESCE(s.use_ymd, g.use_ymd) AS use_ymd,
         SUM(CASE WHEN s.use_ymd IS NULL OR g.use_ymd IS NULL
                   OR s.ride_count <> g.ride_count OR s.alight_count <> g.alight_count
                   OR s.total_count <> g.total_count THEN 1 ELSE 0 END) AS station_different_rows
  FROM __NAMESPACE__.silver_subway_usage_v2 s
  FULL OUTER JOIN __NAMESPACE__.gold_subway_usage_station_v2 g
    ON s.use_ymd = g.use_ymd AND s.line_name = g.line_name AND s.station_name = g.station_name
  GROUP BY COALESCE(s.use_ymd, g.use_ymd)
), expected_line AS (
  SELECT use_ymd, line_name, SUM(ride_count) AS ride_count, SUM(alight_count) AS alight_count,
         SUM(total_count) AS total_count, COUNT(*) AS station_count
  FROM __NAMESPACE__.silver_subway_usage_v2 GROUP BY use_ymd, line_name
), line_difference AS (
  SELECT COALESCE(s.use_ymd, g.use_ymd) AS use_ymd,
         SUM(CASE WHEN s.use_ymd IS NULL OR g.use_ymd IS NULL
                   OR s.ride_count <> g.ride_count OR s.alight_count <> g.alight_count
                   OR s.total_count <> g.total_count OR s.station_count <> g.station_count
              THEN 1 ELSE 0 END) AS line_different_rows
  FROM expected_line s FULL OUTER JOIN __NAMESPACE__.gold_subway_usage_line_v2 g
    ON s.use_ymd = g.use_ymd AND s.line_name = g.line_name
  GROUP BY COALESCE(s.use_ymd, g.use_ymd)
)
SELECT COALESCE(s.use_ymd, g.use_ymd, l.use_ymd) AS use_ymd, 'synthetic' AS data_kind,
       s.silver_total_count, g.station_total_count, l.line_total_count,
       s.duplicate_keys, s.invalid_rows, g.station_duplicate_keys, g.station_invalid_rows,
       l.line_duplicate_keys, l.line_invalid_rows,
       sd.station_different_rows, ld.line_different_rows,
       CASE WHEN s.use_ymd IS NOT NULL
                  AND try_to_timestamp(s.use_ymd, 'yyyyMMdd') IS NOT NULL
                  AND s.silver_total_count = g.station_total_count
                  AND s.silver_total_count = l.line_total_count
                  AND s.duplicate_keys = 0 AND s.invalid_rows = 0
                  AND g.station_duplicate_keys = 0 AND g.station_invalid_rows = 0
                  AND l.line_duplicate_keys = 0 AND l.line_invalid_rows = 0
                  AND sd.station_different_rows = 0 AND ld.line_different_rows = 0
            THEN 'PASS' ELSE 'FAIL' END AS quality_status
FROM silver s FULL OUTER JOIN station g ON s.use_ymd = g.use_ymd
FULL OUTER JOIN line l ON COALESCE(s.use_ymd, g.use_ymd) = l.use_ymd
LEFT JOIN station_difference sd ON COALESCE(s.use_ymd, g.use_ymd, l.use_ymd) = sd.use_ymd
LEFT JOIN line_difference ld ON COALESCE(s.use_ymd, g.use_ymd, l.use_ymd) = ld.use_ymd
