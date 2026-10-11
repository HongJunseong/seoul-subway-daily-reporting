CREATE OR REPLACE VIEW __NAMESPACE__.station_usage_rank_v2
COMMENT 'Synthetic station-line usage ranking for each actual usage date. Transfer lines stay separate. Only dates passing the custom quality contract are served.'
AS
SELECT g.use_ymd, 'synthetic' AS data_kind, g.line_name, g.station_name,
       g.ride_count, g.alight_count, g.total_count,
       DENSE_RANK() OVER (PARTITION BY g.use_ymd ORDER BY g.total_count DESC) AS usage_rank
FROM __NAMESPACE__.gold_subway_usage_station_v2 g
JOIN __NAMESPACE__.usage_quality_status_v2 q ON g.use_ymd = q.use_ymd AND q.quality_status = 'PASS'
