CREATE OR REPLACE VIEW __NAMESPACE__.daily_usage_overview_v2
COMMENT 'Synthetic portfolio data. use_ymd is the actual usage date; not current crowding. Only dates passing the custom quality contract are served.'
AS
SELECT g.use_ymd, 'synthetic' AS data_kind,
       SUM(g.ride_count) AS ride_count, SUM(g.alight_count) AS alight_count,
       SUM(g.total_count) AS total_count,
       COUNT(*) AS station_line_count, COUNT(DISTINCT g.line_name) AS line_count
FROM __NAMESPACE__.gold_subway_usage_station_v2 g
JOIN __NAMESPACE__.usage_quality_status_v2 q ON g.use_ymd = q.use_ymd AND q.quality_status = 'PASS'
GROUP BY g.use_ymd
