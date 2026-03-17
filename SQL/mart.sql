DROP TABLE IF EXISTS mart_energy;

CREATE TABLE mart_energy AS
SELECT
    date,
    energy_category,
    energy_type,
    SUM(production_mwh)                     AS total_production_mwh,
    ROUND(AVG(production_mwh)::numeric, 2)  AS avg_production_mwh,
    COUNT(*)                                AS total_readings
FROM staging_energy
GROUP BY date, energy_category, energy_type
ORDER BY date;


select * from mart_energy;
