-- Milestone 8: Analytical & Data Quality Views for Looker Studio

CREATE OR REPLACE VIEW vw_weather_analytics AS
SELECT 
    c.city_name,
    c.country,
    c.latitude,
    c.longitude,
    d.full_date,
    d.day_of_week,
    d.is_weekend,
    f.temperature,
    f.feels_like,
    f.temp_min,
    f.temp_max,
    f.pressure,
    f.humidity,
    f.wind_speed,
    f.wind_deg,
    f.ingested_at
FROM fact_weather f
INNER JOIN dim_city c ON f.city_name = c.city_name
INNER JOIN dim_date d ON f.date_id = d.date_id;

CREATE OR REPLACE VIEW vw_dq_summary AS
SELECT 
    DATE(logged_at) AS log_date,
    rule_failed,
    COUNT(*) AS total_violations
FROM data_quality_log
GROUP BY DATE(logged_at), rule_failed
ORDER BY log_date DESC, total_violations DESC;
