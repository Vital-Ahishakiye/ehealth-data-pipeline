# Part 1: Data Modeling - Design Rationale

## Overview
This document explains the design decisions for the eFiche operational schema and data warehouse layer.

---

## 1. Operational Schema Design

### 1.1 Core Entities

#### **Facilities**
- **Purpose**: Healthcare locations where encounters occur
- **Key Fields**: `facility_id` (PK), `facility_type`, location details
- **Design Choices**:
  - Separate address fields for query flexibility
  - Boolean flags for emergency/ICU capabilities (fast filtering)
  - CHECK constraint on `facility_type` for data quality

#### **Patients**
- **Purpose**: Patient demographics and contact information
- **Key Fields**: `patient_id` (PK), demographics, insurance
- **Design Choices**:
  - Separate address fields for location-based analytics
  - `is_active` flag for soft deletes (audit trail preservation)
  - `primary_language` for multilingual support
  - CHECK constraint on `gender` for standardization

#### **Encounters**
- **Purpose**: Patient visits to healthcare facilities
- **Key Fields**: `encounter_id` (PK), `patient_id` (FK), `facility_id` (FK)
- **Design Choices**:
  - Both `encounter_date` and `encounter_datetime` for day-level and time-level analytics
  - Separate `admission_source` and `discharge_disposition` for care pathway tracking
  - `visit_reason` for quick filtering
  - CHECK constraint on `encounter_type` (Inpatient, Outpatient, Emergency)

#### **Procedures**
- **Purpose**: Medical imaging studies performed during encounters
- **Key Fields**: `procedure_id` (PK, auto-increment), `encounter_id` (FK)
- **Design Choices**:
  - `modality` uses DICOM standard codes (DX, CR, CT, MR)
  - `laterality` and `view_position` for radiology-specific metadata
  - `radiation_dose_mgy` for patient safety tracking
  - Separate `body_part` field for anatomy-based queries
  - CHECK constraints on `modality` and `laterality` for standardization

#### **Diagnoses**
- **Purpose**: Master catalog of diagnosis codes (ICD-10)
- **Key Fields**: `diagnosis_id` (PK), `diagnosis_code` (ICD-10)
- **Design Choices**:
  - Separate `diagnosis_category` for grouping (Respiratory, Cardiovascular, etc.)
  - `severity` field (Mild, Moderate, Severe, Critical, N/A) for risk stratification
  - Boolean flags: `is_chronic`, `is_reportable` for public health tracking
  - CHECK constraint on `severity` for standardization

#### **Encounter_Diagnoses** (Junction Table)
- **Purpose**: Many-to-many relationship between encounters and diagnoses
- **Key Fields**: Composite PK (`encounter_id`, `diagnosis_id`)
- **Design Choices**:
  - `diagnosis_rank` and `is_primary` for prioritization
  - `diagnosis_confidence` (0.0-1.0) for AI/ML model outputs
  - `diagnosed_by` and `diagnosis_datetime` for audit trails
  - Allows multiple diagnoses per encounter (realistic clinical workflow)

#### **Reports**
- **Purpose**: Radiology reports (structured + free text)
- **Key Fields**: `report_id` (PK, auto-increment), `encounter_id` (FK)
- **Design Choices**:
  - Separate fields: `findings`, `impression`, `recommendations` (structured components)
  - `report_text` (full unstructured text) for NLP/embeddings
  - `report_type` and `report_status` for workflow tracking
  - `critical_finding` boolean + `critical_notification_datetime` for patient safety
  - CHECK constraints on `report_type` and `report_status`

---

## 2. Schema Normalization

### **Normal Form: 3NF (Third Normal Form)**

**Reasoning:**
- ✅ Eliminates data redundancy
- ✅ Maintains data integrity through foreign keys
- ✅ Supports flexible querying
- ✅ Standard for OLTP systems

**Example:**
- Diagnosis details stored once in `diagnoses` table
- Referenced multiple times via `encounter_diagnoses` junction table
- No diagnosis name/code duplication

---

## 3. Indexing Strategy

### **Primary Keys**
- All tables use appropriate primary keys (natural or surrogate)
- `facilities`, `patients`, `diagnoses`: Natural keys (readable IDs)
- `procedures`, `reports`: Auto-increment surrogate keys (high volume)

### **Foreign Keys**
- All relationships enforced with foreign key constraints
- Ensures referential integrity
- Enables CASCADE operations where appropriate

### **Recommended Additional Indexes** (Production):
```sql
-- For encounter queries
CREATE INDEX idx_encounters_patient ON encounters(patient_id);
CREATE INDEX idx_encounters_facility ON encounters(facility_id);
CREATE INDEX idx_encounters_date ON encounters(encounter_date);

-- For procedure queries
CREATE INDEX idx_procedures_encounter ON procedures(encounter_id);
CREATE INDEX idx_procedures_modality ON procedures(modality);
CREATE INDEX idx_procedures_body_part ON procedures(body_part);

-- For diagnosis queries
CREATE INDEX idx_encounter_diagnoses_encounter ON encounter_diagnoses(encounter_id);
CREATE INDEX idx_encounter_diagnoses_diagnosis ON encounter_diagnoses(diagnosis_id);

-- For report text search (PostgreSQL-specific)
CREATE INDEX idx_reports_text ON reports USING gin(to_tsvector('english', report_text));
```

---

## 4. Data Warehouse Design (Star Schema)

### **Why Star Schema?**
- ✅ Optimized for analytics (fast aggregations)
- ✅ Simple for business users to understand
- ✅ Efficient for BI tools (Tableau, Power BI)
- ✅ Denormalized for query performance

### **Fact Table: fact_encounters**
- **Grain**: One row per encounter
- **Measures**: Numeric/quantifiable metrics
- **Foreign Keys**: Links to all dimension tables
- **Design Choice**: Encounter-centric (not procedure-centric) for hospital analytics

### **Dimension Tables**

#### **dim_patient**
- **Type**: Type 1 SCD (Slowly Changing Dimension)
- **Purpose**: Patient demographics for cohort analysis
- **Denormalization**: Age groups pre-calculated

#### **dim_procedure**
- **Type**: Type 1 SCD
- **Purpose**: Procedure metadata for utilization analysis
- **Denormalization**: Body part, modality, laterality combined

#### **dim_diagnosis**
- **Type**: Type 1 SCD
- **Purpose**: Diagnosis grouping for disease analytics
- **Denormalization**: Category, severity pre-joined

#### **dim_time**
- **Type**: Static dimension
- **Purpose**: Calendar attributes for time-series analysis
- **Grain**: Day-level (can be extended to hour-level)
- **Pre-populated**: Includes year, quarter, month, day attributes

### **Bridge Tables**
- `bridge_encounter_procedures`: Handles many-to-many (encounter → procedures)
- `bridge_encounter_diagnoses`: Handles many-to-many (encounter → diagnoses)
- **Purpose**: Allows multiple procedures/diagnoses per encounter in warehouse

---

## 5. Scalability Considerations

### **Partitioning Strategy (Future)**
- Partition `encounters` and `procedures` by `encounter_date` (monthly)
- Partition `reports` by `dictated_datetime` (monthly)
- **Benefit**: Faster queries, easier archival

### **Data Archival**
- Move encounters older than 7 years to cold storage
- Retain `patients` and `diagnoses` indefinitely (master data)

### **Concurrency**
- PostgreSQL MVCC handles concurrent reads/writes
- Use connection pooling (pgBouncer) for high load

---

## 6. Future Extensibility

### **Embeddings Support**
- `reports.report_text` → Ready for NLP/embeddings
- Can add `report_embedding` column (vector type) for similarity search
- PostgreSQL pgvector extension support

### **Audio/Voice Data**
- Can add `voice_recordings` table linked to encounters
- Store audio file paths or binary data
- Link to `reports` for transcription workflows

### **Multi-Modal Data**
- Can add `images` table for DICOM storage
- Link to `procedures` via `procedure_id`

---

## 7. Data Quality & Validation

### **CHECK Constraints**
- Enforced on critical fields: `facility_type`, `modality`, `severity`, etc.
- Prevents invalid data entry
- Self-documenting schema

### **NOT NULL Constraints**
- Required fields enforced at database level
- Reduces application logic complexity

### **Default Values**
- `created_at`, `updated_at` auto-populated
- Boolean flags default to `false`

---

## 8. Compliance & Security (Not Implemented)

### **HIPAA Considerations** (Production Requirements):
- Encrypt PHI columns (patient names, addresses)
- Audit logging (who accessed what, when)
- Role-based access control (RBAC)
- De-identification for analytics (remove names, dates)

### **GDPR Considerations**:
- Right to erasure: Soft deletes via `is_active` flag
- Data minimization: Only collect necessary fields
- Consent tracking: Can add `consents` table

---

## 9. Trade-offs & Decisions

| Decision | Alternative Considered | Rationale |
|----------|----------------------|-----------|
| Natural keys for patients | Auto-increment surrogate | Readable IDs, easier debugging |
| Separate `encounter_diagnoses` | Embed in encounters JSON | Flexibility, queryability |
| Star schema for warehouse | Snowflake schema | Simplicity, performance |
| Day-level time dimension | Hour-level | Sufficient for hospital analytics |
| PostgreSQL full-text search | Elasticsearch | Simpler stack, good-enough performance |

---

## 10. Summary

This schema design balances:
- ✅ **Normalization** (3NF) for data integrity in operational layer
- ✅ **Denormalization** (star schema) for performance in analytics layer
- ✅ **Scalability** through partitioning and indexing
- ✅ **Extensibility** for future features (embeddings, audio)
- ✅ **Data Quality** through constraints and validation

**Result**: Production-ready schema for healthcare data engineering.