-- ============================================================
-- Data Warehouse QA Script
-- Purpose: Validate star schema integrity and data quality
-- ============================================================

-- 1. Dimension Integrity Checks
-- ------------------------------------------------------------

-- 1.1 Duplicate natural keys in dimensions
SELECT patient_id, COUNT(*) 
FROM dim_patient 
GROUP BY patient_id HAVING COUNT(*) > 1;

SELECT procedure_id, COUNT(*) 
FROM dim_procedure 
GROUP BY procedure_id HAVING COUNT(*) > 1;

SELECT diagnosis_id, COUNT(*) 
FROM dim_diagnosis 
GROUP BY diagnosis_id HAVING COUNT(*) > 1;

-- 1.2 Missing surrogate keys
SELECT 'dim_patient' AS table_name, COUNT(*) AS missing_keys
FROM dim_patient WHERE patient_key IS NULL
UNION ALL
SELECT 'dim_procedure', COUNT(*) FROM dim_procedure WHERE procedure_key IS NULL
UNION ALL
SELECT 'dim_diagnosis', COUNT(*) FROM dim_diagnosis WHERE diagnosis_key IS NULL;

-- ------------------------------------------------------------

-- 2. Fact Table QA
-- ------------------------------------------------------------

-- 2.1 Orphan fact rows (no matching dimension)
SELECT f.encounter_key, f.encounter_id
FROM fact_encounters f
LEFT JOIN dim_patient dp ON f.patient_key = dp.patient_key
WHERE dp.patient_key IS NULL
LIMIT 50;

SELECT f.encounter_key, f.encounter_id
FROM fact_encounters f
LEFT JOIN dim_time dt ON f.date_id = dt.date_id
WHERE dt.date_id IS NULL
LIMIT 50;

-- 2.2 Duplicate encounter IDs in fact table
SELECT encounter_id, COUNT(*)
FROM fact_encounters
GROUP BY encounter_id HAVING COUNT(*) > 1;

-- 2.3 Fact counts reconciliation
SELECT 
    (SELECT COUNT(*) FROM encounters) AS operational_encounters,
    (SELECT COUNT(*) FROM fact_encounters) AS warehouse_encounters;

-- ------------------------------------------------------------

-- 3. Bridge Table QA
-- ------------------------------------------------------------

-- 3.1 Orphan bridge rows (no matching fact or dimension)
SELECT b.encounter_key
FROM bridge_encounter_procedures b
LEFT JOIN fact_encounters f ON b.encounter_key = f.encounter_key
WHERE f.encounter_key IS NULL
LIMIT 50;

SELECT b.procedure_key
FROM bridge_encounter_procedures b
LEFT JOIN dim_procedure dp ON b.procedure_key = dp.procedure_key
WHERE dp.procedure_key IS NULL
LIMIT 50;

SELECT b.encounter_key
FROM bridge_encounter_diagnoses b
LEFT JOIN fact_encounters f ON b.encounter_key = f.encounter_key
WHERE f.encounter_key IS NULL
LIMIT 50;

SELECT b.diagnosis_key
FROM bridge_encounter_diagnoses b
LEFT JOIN dim_diagnosis dd ON b.diagnosis_key = dd.diagnosis_key
WHERE dd.diagnosis_key IS NULL
LIMIT 50;

-- 3.2 Duplicate bridge entries
SELECT encounter_key, procedure_key, COUNT(*)
FROM bridge_encounter_procedures
GROUP BY encounter_key, procedure_key HAVING COUNT(*) > 1;

SELECT encounter_key, diagnosis_key, COUNT(*)
FROM bridge_encounter_diagnoses
GROUP BY encounter_key, diagnosis_key HAVING COUNT(*) > 1;

-- ------------------------------------------------------------

-- 4. Row Count Comparisons
-- ------------------------------------------------------------

-- 4.1 Procedures count reconciliation
SELECT 
    (SELECT COUNT(*) FROM procedures) AS operational_procedures,
    (SELECT COUNT(*) FROM dim_procedure) AS warehouse_procedures,
    (SELECT COUNT(*) FROM bridge_encounter_procedures) AS bridge_procedures;

-- 4.2 Diagnoses count reconciliation
SELECT 
    (SELECT COUNT(*) FROM diagnoses) AS operational_diagnoses,
    (SELECT COUNT(*) FROM dim_diagnosis) AS warehouse_diagnoses,
    (SELECT COUNT(*) FROM bridge_encounter_diagnoses) AS bridge_diagnoses;

-- ------------------------------------------------------------

-- 5. Temporal QA
-- ------------------------------------------------------------

-- 5.1 Fact encounters date_id validity
SELECT f.encounter_key, f.date_id
FROM fact_encounters f
LEFT JOIN dim_time dt ON f.date_id = dt.date_id
WHERE dt.date_id IS NULL
LIMIT 50;

-- 5.2 Dim_time coverage
SELECT MIN(full_date) AS earliest_date, MAX(full_date) AS latest_date
FROM dim_time;

-- ------------------------------------------------------------
-- END OF QA SCRIPT
-- ============================================================
