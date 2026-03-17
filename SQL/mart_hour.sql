DROP TABLE IF EXISTS mart_hourly;

CREATE TABLE mart_hourly AS
SELECT
    hour,
    energy_category,
    ROUND(AVG(production_mwh)::numeric, 2)  AS avg_production_mwh,
    SUM(production_mwh)                     AS total_production_mwh
FROM staging_energy
GROUP BY hour, energy_category
ORDER BY hour;

select * from mart_hourly;