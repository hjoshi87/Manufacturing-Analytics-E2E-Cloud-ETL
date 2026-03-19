{{ config(materialized='table') }}

SELECT
    timestamp_15min,
    machine_id,
    operation,
    vibration_rms_x,
    vibration_rms_y,
    vibration_rms_z,
    has_anomaly,
    energy_price_eur_mwh,
    industrial_production_index,
    estimated_power_kwh,
    energy_cost_eur,
    anomaly_risk_score,
    CURRENT_TIMESTAMP as loaded_at

FROM public.silver_harmonized_final
WHERE timestamp_15min >= '2019-02-01'
AND timestamp_15min <= '2021-08-31'