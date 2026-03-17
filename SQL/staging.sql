DROP TABLE IF EXISTS staging_energy;

CREATE TABLE staging_energy AS
SELECT
    id,
    timestamp,
    DATE(timestamp)                 AS date,
    EXTRACT(HOUR FROM timestamp)    AS hour,
    energy_type,
    CASE
        WHEN energy_type IN ('Wind Offshore','Wind Onshore',
                             'Solar','Hydropower','Biomass')
        THEN 'Renewable'
        ELSE 'Non-Renewable'
    END                             AS energy_category,
    production_mwh,
    ingested_at
FROM raw_energy
WHERE production_mwh >= 0
  AND production_mwh < 100000;

select * from staging_energy;

SELECT COUNT(*) FROM staging_energy;
