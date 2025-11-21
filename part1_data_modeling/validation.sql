-- ============================================================
-- Part 1: Data Modeling - Test & Validation Queries
-- ============================================================
-- Purpose: Verify data quality and schema integrity
-- Run these queries after synthetic data generation
-- ============================================================

-- ============================================================
-- 1. DATA VOLUME CHECKS
-- ============================================================

-- 1.1 Record counts per table
SELECT 'facilities' as table_name, COUNT(*) as record_count FROM facilities
UNION ALL
SELECT 'patients', COUNT(*) FROM patients
UNION ALL
SELECT 'diagnoses', COUNT(*) FROM diagnoses
UNION ALL
SELECT 'encounters', COUNT(*) FROM encounters
UNION ALL
SELECT 'procedures', COUNT(*) FROM procedures
UNION ALL
SELECT 'encounter_diagnoses', COUNT(*) FROM encounter_diagnoses
UNION ALL
SELECT 'reports', COUNT(*) FROM reports
ORDER BY table_name;

-- ============================================================
-- 2. DATA QUALITY CHECKS
-- ============================================================

-- 2.1 Check for NULL values in required fields
SELECT 'facilities - missing facility_name' as check_name, COUNT(*) as violations
FROM facilities WHERE facility_name IS NULL
UNION ALL
SELECT 'patients - missing date_of_birth', COUNT(*) 
FROM patients WHERE date_of_birth IS NULL
UNION ALL
SELECT 'encounters - missing encounter_date', COUNT(*) 
FROM encounters WHERE encounter_date IS NULL
UNION ALL
SELECT 'procedures - missing modality', COUNT(*) 
FROM procedures WHERE modality IS NULL
UNION ALL
SELECT 'reports - missing report_text', COUNT(*) 
FROM reports WHERE report_text IS NULL;

-- 2.2 Verify CHECK constraints (should return 0 violations)
SELECT 'facilities - invalid facility_type' as check_name, COUNT(*) as violations
FROM facilities 
WHERE facility_type NOT IN ('Hospital', 'Clinic', 'Imaging Center', 'Urgent Care')
UNION ALL
SELECT 'patients - invalid gender', COUNT(*) 
FROM patients 
WHERE gender NOT IN ('M', 'F', 'Other')
UNION ALL
SELECT 'encounters - invalid encounter_type', COUNT(*) 
FROM encounters 
WHERE encounter_type NOT IN ('Inpatient', 'Outpatient', 'Emergency')
UNION ALL
SELECT 'procedures - invalid modality', COUNT(*) 
FROM procedures 
WHERE modality NOT IN ('DX', 'CR', 'CT', 'MR')
UNION ALL
SELECT 'procedures - invalid laterality', COUNT(*) 
FROM procedures 
WHERE laterality NOT IN ('Left', 'Right', 'Bilateral', 'N/A')
UNION ALL
SELECT 'diagnoses - invalid severity', COUNT(*) 
FROM diagnoses 
WHERE severity NOT IN ('Mild', 'Moderate', 'Severe', 'Critical', 'N/A')
UNION ALL
SELECT 'reports - invalid report_type', COUNT(*) 
FROM reports 
WHERE report_type NOT IN ('Preliminary', 'Final', 'Addendum', 'Corrected')
UNION ALL
SELECT 'reports - invalid report_status', COUNT(*) 
FROM reports 
WHERE report_status NOT IN ('Draft', 'Signed', 'Amended');

-- ============================================================
-- 3. REFERENTIAL INTEGRITY CHECKS
-- ============================================================

-- 3.1 Orphaned records (should return 0)
SELECT 'encounters - orphaned patient_id' as check_name, COUNT(*) as violations
FROM encounters e
LEFT JOIN patients p ON e.patient_id = p.patient_id
WHERE p.patient_id IS NULL
UNION ALL
SELECT 'encounters - orphaned facility_id', COUNT(*) 
FROM encounters e
LEFT JOIN facilities f ON e.facility_id = f.facility_id
WHERE f.facility_id IS NULL
UNION ALL
SELECT 'procedures - orphaned encounter_id', COUNT(*) 
FROM procedures p
LEFT JOIN encounters e ON p.encounter_id = e.encounter_id
WHERE e.encounter_id IS NULL
UNION ALL
SELECT 'encounter_diagnoses - orphaned encounter_id', COUNT(*) 
FROM encounter_diagnoses ed
LEFT JOIN encounters e ON ed.encounter_id = e.encounter_id
WHERE e.encounter_id IS NULL
UNION ALL
SELECT 'encounter_diagnoses - orphaned diagnosis_id', COUNT(*) 
FROM encounter_diagnoses ed
LEFT JOIN diagnoses d ON ed.diagnosis_id = d.diagnosis_id
WHERE d.diagnosis_id IS NULL
UNION ALL
SELECT 'reports - orphaned encounter_id', COUNT(*) 
FROM reports r
LEFT JOIN encounters e ON r.encounter_id = e.encounter_id
WHERE e.encounter_id IS NULL;

-- ============================================================
-- 4. DATA DISTRIBUTION ANALYSIS
-- ============================================================

-- 4.1 Facilities by type
SELECT 
    facility_type,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM facilities
GROUP BY facility_type
ORDER BY count DESC;

-- 4.2 Patients by gender
SELECT 
    gender,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM patients
GROUP BY gender
ORDER BY count DESC;

-- 4.3 Encounters by type
SELECT 
    encounter_type,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM encounters
GROUP BY encounter_type
ORDER BY count DESC;

-- 4.4 Procedures by modality
SELECT 
    modality,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM procedures
GROUP BY modality
ORDER BY count DESC;

-- 4.5 Diagnoses by severity
SELECT 
    severity,
    COUNT(DISTINCT ed.encounter_id) as encounter_count,
    ROUND(COUNT(DISTINCT ed.encounter_id) * 100.0 / 
          (SELECT COUNT(DISTINCT encounter_id) FROM encounter_diagnoses), 2) as percentage
FROM encounter_diagnoses ed
JOIN diagnoses d ON ed.diagnosis_id = d.diagnosis_id
GROUP BY severity
ORDER BY 
    CASE severity
        WHEN 'Critical' THEN 1
        WHEN 'Severe' THEN 2
        WHEN 'Moderate' THEN 3
        WHEN 'Mild' THEN 4
        WHEN 'N/A' THEN 5
    END;

-- ============================================================
-- 5. RELATIONSHIP VALIDATION
-- ============================================================

-- 5.1 Encounters per patient (should be varied)
SELECT 
    CASE 
        WHEN encounter_count = 1 THEN '1 encounter'
        WHEN encounter_count BETWEEN 2 AND 3 THEN '2-3 encounters'
        WHEN encounter_count BETWEEN 4 AND 5 THEN '4-5 encounters'
        ELSE '6+ encounters'
    END as encounter_range,
    COUNT(*) as patient_count
FROM (
    SELECT patient_id, COUNT(*) as encounter_count
    FROM encounters
    GROUP BY patient_id
) subq
GROUP BY encounter_range
ORDER BY encounter_range;

-- 5.2 Procedures per encounter (should be 1-3)
SELECT 
    procedure_count,
    COUNT(*) as encounter_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM (
    SELECT encounter_id, COUNT(*) as procedure_count
    FROM procedures
    GROUP BY encounter_id
) subq
GROUP BY procedure_count
ORDER BY procedure_count;

-- 5.3 Diagnoses per encounter (should be 1-3)
SELECT 
    diagnosis_count,
    COUNT(*) as encounter_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM (
    SELECT encounter_id, COUNT(*) as diagnosis_count
    FROM encounter_diagnoses
    GROUP BY encounter_id
) subq
GROUP BY diagnosis_count
ORDER BY diagnosis_count;

-- ============================================================
-- 6. TEMPORAL DATA VALIDATION
-- ============================================================

-- 6.1 Encounter date range
SELECT 
    'encounter_date' as field,
    MIN(encounter_date) as earliest,
    MAX(encounter_date) as latest,
    MAX(encounter_date) - MIN(encounter_date) as date_range_days
FROM encounters;

-- 6.2 Reports per month (time-series check)
SELECT 
    DATE_TRUNC('month', dictated_datetime) as month,
    COUNT(*) as report_count
FROM reports
GROUP BY month
ORDER BY month
LIMIT 12;

-- ============================================================
-- 7. BUSINESS LOGIC VALIDATION
-- ============================================================

-- 7.1 Primary diagnoses per encounter (should be exactly 1 per encounter)
SELECT 
    encounter_id,
    COUNT(*) as primary_diagnosis_count
FROM encounter_diagnoses
WHERE is_primary = true
GROUP BY encounter_id
HAVING COUNT(*) != 1
ORDER BY encounter_id
LIMIT 10;
-- Expected: 0 rows (each encounter should have exactly 1 primary diagnosis)

-- 7.2 Critical reports with notification timestamps
SELECT 
    report_type,
    report_status,
    critical_finding,
    COUNT(*) as count,
    COUNT(critical_notification_datetime) as notifications_sent
FROM reports
WHERE critical_finding = true
GROUP BY report_type, report_status, critical_finding
ORDER BY count DESC;

-- 7.3 Radiation dose tracking (procedures with dose recorded)
SELECT 
    modality,
    COUNT(*) as total_procedures,
    COUNT(radiation_dose_mgy) as procedures_with_dose,
    ROUND(COUNT(radiation_dose_mgy) * 100.0 / COUNT(*), 2) as dose_recorded_pct,
    ROUND(AVG(radiation_dose_mgy), 2) as avg_dose_mgy
FROM procedures
GROUP BY modality
ORDER BY modality;

-- ============================================================
-- 8. TOP 10 ANALYTICS
-- ============================================================

-- 8.1 Top 10 facilities by encounter volume
SELECT 
    f.facility_name,
    f.facility_type,
    COUNT(e.encounter_id) as encounter_count
FROM facilities f
JOIN encounters e ON f.facility_id = e.facility_id
GROUP BY f.facility_id, f.facility_name, f.facility_type
ORDER BY encounter_count DESC
LIMIT 10;

-- 8.2 Top 10 most common diagnoses
SELECT 
    d.diagnosis_code,
    d.diagnosis_name,
    d.diagnosis_category,
    d.severity,
    COUNT(*) as frequency
FROM encounter_diagnoses ed
JOIN diagnoses d ON ed.diagnosis_id = d.diagnosis_id
GROUP BY d.diagnosis_id, d.diagnosis_code, d.diagnosis_name, d.diagnosis_category, d.severity
ORDER BY frequency DESC
LIMIT 10;

-- 8.3 Top 10 procedures by body part
SELECT 
    body_part,
    modality,
    COUNT(*) as procedure_count
FROM procedures
GROUP BY body_part, modality
ORDER BY procedure_count DESC
LIMIT 10;

-- ============================================================
-- 9. REPORT TEXT QUALITY CHECKS
-- ============================================================

-- 9.1 Report text length distribution
SELECT 
    CASE 
        WHEN LENGTH(report_text) < 100 THEN 'Very Short (<100 chars)'
        WHEN LENGTH(report_text) BETWEEN 100 AND 300 THEN 'Short (100-300 chars)'
        WHEN LENGTH(report_text) BETWEEN 301 AND 500 THEN 'Medium (301-500 chars)'
        ELSE 'Long (>500 chars)'
    END as text_length_category,
    COUNT(*) as count,
    ROUND(AVG(LENGTH(report_text)), 0) as avg_length
FROM reports
GROUP BY text_length_category
ORDER BY avg_length;

-- 9.2 Sample reports for manual review
SELECT 
    r.report_id,
    r.report_type,
    r.report_status,
    d.diagnosis_name,
    d.severity,
    LEFT(r.report_text, 150) as report_sample
FROM reports r
JOIN encounters e ON r.encounter_id = e.encounter_id
LEFT JOIN encounter_diagnoses ed ON e.encounter_id = ed.encounter_id AND ed.is_primary = true
LEFT JOIN diagnoses d ON ed.diagnosis_id = d.diagnosis_id
ORDER BY RANDOM()
LIMIT 5;

-- ============================================================
-- 10. PERFORMANCE BASELINE METRICS
-- ============================================================

-- 10.1 Table sizes
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
    AND tablename IN ('facilities', 'patients', 'diagnoses', 'encounters', 
                      'procedures', 'encounter_diagnoses', 'reports')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- ============================================================
-- END OF VALIDATION QUERIES
-- ============================================================
-- All queries should complete without errors
-- Review any violations or unexpected distributions
-- ============================================================