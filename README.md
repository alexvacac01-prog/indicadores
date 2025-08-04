{{ config(materialized='view') }}

-- Modelo dbt para KPIs curados (Monitoreo MPPR)
WITH base AS (
    SELECT 
        *,
        DATE_TRUNC('day', fecha) AS fecha_dia
    FROM {{ ref('kpi_raw') }}
)

SELECT
    fecha_dia,
    municipio,
    servicio,
    COUNT(*) AS total_registros,
    AVG(tiempo_respuesta) AS kpi_tiempo_respuesta,
    SUM(CASE WHEN tiempo_respuesta > 48 THEN 1 ELSE 0 END) AS alertas_caps,
    SUM(CASE WHEN status = 'resuelto' THEN 1 ELSE 0 END) AS kpi_resueltos,
    SUM(CASE WHEN prioridad = 'alta' THEN 1 ELSE 0 END) AS kpi_alta_prioridad,
    SUM(CASE WHEN meta_cumplida = 1 THEN 1 ELSE 0 END) AS kpi_meta_cumplida
FROM base
GROUP BY fecha_dia, municipio, servicio
