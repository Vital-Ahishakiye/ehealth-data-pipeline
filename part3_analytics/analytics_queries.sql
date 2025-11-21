-- ============================================================
-- Analytics Queries - Data Warehouse Reporting
-- Purpose: Business intelligence queries for eHealth analytics
-- ============================================================

-- ============================================================
-- METRIC 1: Number of Encounters Per Month
-- ============================================================
SELECT
  dt.year,
  dt.month,
  dt.month_name,
  COUNT(DISTINCT f.encounter_key) AS total_encounters,
  COUNT(DISTINCT f.patient_key) AS unique_patients,
  ROUND(
    COUNT(DISTINCT f.encounter_key)::NUMERIC /
    NULLIF(COUNT(DISTINCT f.patient_key), 0)
  , 2) AS encounters_per_patient,
  SUM(f.procedure_count) AS total_procedures_performed,
  ROUND(AVG(f.procedure_count), 2) AS avg_procedures_per_encounter
FROM fact_encounters f
JOIN dim_time dt ON f.date_id = dt.date_id
GROUP BY dt.year, dt.month, dt.month_name
ORDER BY dt.year, dt.month;

-- ============================================================
-- METRIC 2: Most Frequent Primary Diagnoses Per Age Group
-- ============================================================
WITH primary_diag AS (
  SELECT
    f.patient_key,
    dp.age_group,
    bd.diagnosis_key
  FROM fact_encounters f
  JOIN dim_patient dp ON f.patient_key = dp.patient_key
  JOIN bridge_encounter_diagnoses bd ON f.encounter_key = bd.encounter_key
  WHERE bd.diagnosis_type = 'Primary'
)
SELECT
  p.age_group,
  dd.diagnosis_name,
  dd.category,
  dd.severity,
  COUNT(*) AS diagnosis_frequency,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY p.age_group), 2) AS pct_in_age_group,
  RANK() OVER (PARTITION BY p.age_group ORDER BY COUNT(*) DESC) AS rank_in_age_group
FROM primary_diag p
JOIN dim_diagnosis dd ON p.diagnosis_key = dd.diagnosis_key
GROUP BY p.age_group, dd.diagnosis_name, dd.category, dd.severity
HAVING COUNT(*) >= 10
ORDER BY p.age_group, diagnosis_frequency DESC;

-- Top 5 Diagnoses Per Age Group (summary)
WITH ranked_diagnoses AS (
  SELECT
    p.age_group,
    dd.diagnosis_name,
    dd.severity,
    COUNT(*) AS frequency,
    RANK() OVER (PARTITION BY p.age_group ORDER BY COUNT(*) DESC) AS rnk
  FROM fact_encounters f
  JOIN dim_patient p ON f.patient_key = p.patient_key
  JOIN bridge_encounter_diagnoses bd ON f.encounter_key = bd.encounter_key
  JOIN dim_diagnosis dd ON bd.diagnosis_key = dd.diagnosis_key
  WHERE bd.diagnosis_type = 'Primary'
  GROUP BY p.age_group, dd.diagnosis_name, dd.severity
)
SELECT age_group, diagnosis_name, severity, frequency, rnk AS rank
FROM ranked_diagnoses
WHERE rnk <= 5
ORDER BY age_group, rank;

-- ============================================================
-- METRIC 3: Average Studies (Procedures) Per Patient
-- ============================================================
-- Overall statistics
SELECT
  COUNT(DISTINCT dp.patient_key) AS total_patients,
  COUNT(DISTINCT f.encounter_key) AS total_encounters,
  SUM(f.procedure_count) AS total_procedures,
  ROUND(AVG(f.procedure_count), 2) AS avg_procedures_per_encounter,
  ROUND(SUM(f.procedure_count)::NUMERIC / NULLIF(COUNT(DISTINCT dp.patient_key),0), 2) AS avg_procedures_per_patient
FROM fact_encounters f
JOIN dim_patient dp ON f.patient_key = dp.patient_key;

-- Avg studies per patient by age group
SELECT
  dp.age_group,
  COUNT(DISTINCT dp.patient_key) AS patient_count,
  SUM(f.procedure_count) AS total_procedures,
  ROUND(AVG(f.procedure_count), 2) AS avg_procedures_per_encounter,
  ROUND(SUM(f.procedure_count)::NUMERIC / NULLIF(COUNT(DISTINCT dp.patient_key),0), 2) AS avg_procedures_per_patient,
  ROUND(STDDEV(f.procedure_count), 2) AS stddev_procedures
FROM fact_encounters f
JOIN dim_patient dp ON f.patient_key = dp.patient_key
GROUP BY dp.age_group
ORDER BY dp.age_group;

-- Avg studies per patient by sex
SELECT
  dp.sex,
  COUNT(DISTINCT dp.patient_key) AS patient_count,
  SUM(f.procedure_count) AS total_procedures,
  ROUND(SUM(f.procedure_count)::NUMERIC / NULLIF(COUNT(DISTINCT dp.patient_key),0), 2) AS avg_procedures_per_patient
FROM fact_encounters f
JOIN dim_patient dp ON f.patient_key = dp.patient_key
GROUP BY dp.sex
ORDER BY dp.sex;

-- ============================================================
-- METRIC 4: Top Procedure Volumes and Modalities
-- ============================================================
-- Procedures by modality and month
SELECT
  dt.year,
  dt.month,
  dp.modality,
  COUNT(bep.bridge_id) AS procedure_count
FROM bridge_encounter_procedures bep
JOIN fact_encounters f ON bep.encounter_key = f.encounter_key
JOIN dim_time dt ON f.date_id = dt.date_id
JOIN dim_procedure dp ON bep.procedure_key = dp.procedure_key
GROUP BY dt.year, dt.month, dp.modality
ORDER BY dt.year, dt.month, dp.modality;

-- Top 20 procedures by code last 12 months
SELECT
  dpr.procedure_code,
  dpr.procedure_name,
  COUNT(bep.bridge_id) AS occurrences
FROM bridge_encounter_procedures bep
JOIN fact_encounters f ON bep.encounter_key = f.encounter_key
JOIN dim_procedure dpr ON bep.procedure_key = dpr.procedure_key
JOIN dim_time dt ON f.date_id = dt.date_id
WHERE dt.full_date >= (CURRENT_DATE - INTERVAL '12 months')
GROUP BY dpr.procedure_code, dpr.procedure_name
ORDER BY occurrences DESC
LIMIT 20;

-- ============================================================
-- METRIC 5a: Diagnosis Distribution (Top 20)
-- ============================================================
SELECT
  dd.diagnosis_code,
  dd.diagnosis_name,
  COUNT(bd.bridge_id) AS occurrences,
  ROUND(100.0 * COUNT(bd.bridge_id)::NUMERIC / SUM(COUNT(bd.bridge_id)) OVER (), 2) AS pct_share
FROM bridge_encounter_diagnoses bd
JOIN dim_diagnosis dd ON bd.diagnosis_key = dd.diagnosis_key
GROUP BY dd.diagnosis_code, dd.diagnosis_name
ORDER BY occurrences DESC
LIMIT 20;

-- ============================================================
-- METRIC 7: Patient Demographics Summary
-- ============================================================
SELECT
  dp.age_group,
  dp.sex,
  COUNT(DISTINCT dp.patient_key) AS patient_count,
  COUNT(DISTINCT f.encounter_key) AS total_encounters,
  ROUND(
    COUNT(DISTINCT f.encounter_key)::NUMERIC /
    NULLIF(COUNT(DISTINCT dp.patient_key),0)
  , 2) AS encounters_per_patient
FROM dim_patient dp
LEFT JOIN fact_encounters f ON dp.patient_key = f.patient_key
GROUP BY dp.age_group, dp.sex
ORDER BY dp.age_group, dp.sex;

-- ============================================================
-- METRIC 8: Top Facilities by Volume
-- ============================================================
SELECT
  fac.facility_name,
  fac.facility_type,
  COUNT(DISTINCT f.encounter_key) AS encounter_count,
  COUNT(DISTINCT f.patient_key) AS unique_patients,
  SUM(f.procedure_count) AS total_procedures
FROM fact_encounters f
JOIN facilities fac ON f.facility_id = fac.facility_id
GROUP BY fac.facility_id, fac.facility_name, fac.facility_type
ORDER BY encounter_count DESC
LIMIT 10;

-- ============================================================
-- END OF Analytics Queries
-- ============================================================
