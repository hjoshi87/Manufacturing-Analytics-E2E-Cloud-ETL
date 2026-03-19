{{ config(
    materialized='table'
) }}

SELECT
    machine_id,
    operation,
    
    -- Date range
    MIN(measurement_date) as first_measurement,
    MAX(measurement_date) as last_measurement,
    COUNT(DISTINCT measurement_date) as days_with_data,
    
    -- Total metrics
    SUM(total_measurements) as total_measurements,
    SUM(anomaly_count) as total_anomalies,
    ROUND(100.0 * SUM(anomaly_count) / SUM(total_measurements), 2) as overall_anomaly_rate,
    
    -- Vibration stats
    AVG(avg_vibration_x) as overall_avg_vibration_x,
    AVG(avg_vibration_y) as overall_avg_vibration_y,
    AVG(avg_vibration_z) as overall_avg_vibration_z,
    MAX(max_vibration_x) as peak_vibration_x,
    MAX(max_vibration_y) as peak_vibration_y,
    MAX(max_vibration_z) as peak_vibration_z,
    AVG(vibration_magnitude) as avg_vibration_magnitude,
    
    -- Energy stats
    AVG(avg_energy_price) as avg_energy_price_overall,
    SUM(daily_energy_cost) as total_energy_cost,
    AVG(avg_power_consumption) as avg_power_consumption_overall,
    
    -- Industrial index
    AVG(avg_industrial_index) as avg_industrial_index_overall,
    
    -- OVERALL Risk Level (based on overall anomaly rate)
    CASE 
        WHEN ROUND(100.0 * SUM(anomaly_count) / SUM(total_measurements), 2) >= 40 THEN 'CRITICAL'
        WHEN ROUND(100.0 * SUM(anomaly_count) / SUM(total_measurements), 2) >= 25 THEN 'HIGH_RISK'
        WHEN ROUND(100.0 * SUM(anomaly_count) / SUM(total_measurements), 2) >= 10 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END as risk_level

FROM {{ ref('fct_machine_daily') }}

GROUP BY
    machine_id,
    operation