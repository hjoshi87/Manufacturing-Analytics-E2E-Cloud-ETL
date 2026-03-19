{{ config(
    materialized='table'
) }}

WITH percentiles AS (
    SELECT
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY vibration_magnitude) as p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY vibration_magnitude) as p75
    FROM {{ ref('int_daily_metrics') }}
)

SELECT
    m.measurement_date,
    m.machine_id,
    m.operation,
    
    -- Measurement counts
    m.total_measurements,
    m.anomaly_count,
    m.anomaly_rate_pct,
    
    -- Vibration metrics
    m.avg_vibration_x,
    m.avg_vibration_y,
    m.avg_vibration_z,
    m.max_vibration_x,
    m.max_vibration_y,
    m.max_vibration_z,
    m.vibration_magnitude,
    
    -- Energy metrics
    m.avg_energy_price,
    m.min_energy_price,
    m.max_energy_price,
    m.daily_energy_cost,
    m.avg_power_consumption,
    
    -- Industrial data
    m.avg_industrial_index,
    m.avg_anomaly_risk,
    
    -- Risk assessment
    CASE 
        WHEN m.anomaly_rate_pct > 20 THEN 'CRITICAL'
        WHEN m.anomaly_rate_pct > 10 THEN 'HIGH_RISK'
        WHEN m.anomaly_rate_pct > 5 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END as machine_health_status,
    
    -- Vibration stability
    CASE
        WHEN m.vibration_magnitude >= p.p75 THEN 'UNSTABLE'    -- Top 25%
        WHEN m.vibration_magnitude >= p.p50 THEN 'MODERATE'    -- Top 50%
        ELSE 'STABLE'                                          -- Bottom 50%
    END as vibration_stability,
    
    -- Cost efficiency
    ROUND(m.daily_energy_cost / (m.avg_power_consumption / 1000.0), 2) as cost_per_kwh,
    CURRENT_TIMESTAMP as created_at

FROM {{ ref('int_daily_metrics') }} m
CROSS JOIN percentiles p