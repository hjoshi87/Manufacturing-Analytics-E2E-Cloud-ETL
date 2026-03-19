{{ config(
    materialized='table'
) }}

SELECT
    DATE(timestamp_15min) as measurement_date,
    machine_id,
    operation,
    
    COUNT(*) as total_measurements,
    SUM(CASE WHEN has_anomaly THEN 1 ELSE 0 END) as anomaly_count,
    ROUND(100.0 * SUM(CASE WHEN has_anomaly THEN 1 ELSE 0 END) / COUNT(*), 2) as anomaly_rate_pct,
    
    AVG(vibration_rms_x) as avg_vibration_x,
    AVG(vibration_rms_y) as avg_vibration_y,
    AVG(vibration_rms_z) as avg_vibration_z,
    MAX(vibration_rms_x) as max_vibration_x,
    MAX(vibration_rms_y) as max_vibration_y,
    MAX(vibration_rms_z) as max_vibration_z,

    -- Vibration Magnitude
    ROUND(
        SQRT(
            POWER(AVG(vibration_rms_x), 2) + 
            POWER(AVG(vibration_rms_y), 2) + 
            POWER(AVG(vibration_rms_z), 2)
        ),
        3
    ) as vibration_magnitude,
    
    AVG(energy_price_eur_mwh) as avg_energy_price,
    MIN(energy_price_eur_mwh) as min_energy_price,
    MAX(energy_price_eur_mwh) as max_energy_price,
    SUM(energy_cost_eur) as daily_energy_cost,
    AVG(estimated_power_kwh) as avg_power_consumption,
    
    AVG(industrial_production_index) as avg_industrial_index,
    AVG(anomaly_risk_score) as avg_anomaly_risk

FROM {{ ref('stg_silver_harmonized') }}

GROUP BY
    measurement_date,
    machine_id,
    operation