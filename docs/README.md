# eHealth Clinical Decision Support Data Pipeline

> **Production-ready healthcare data engineering solution for clinical analytics and decision support**

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL 15](https://img.shields.io/badge/postgresql-15-blue.svg)](https://www.postgresql.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Problem Statement](#-problem-statement)
- [Key Features](#-key-features)
- [System Architecture](#-system-architecture)
- [Quick Start](#-quick-start)
- [Project Metrics](#-project-metrics)
- [Technical Documentation](#-technical-documentation)
- [Results](#-results)
- [Future Enhancements](#-future-enhancements)
- [Technology Stack](#-technology-stack)
- [License](#-license)
- [Author](#-author)

---

## 🎯 Overview

This project was developed as part of a technical assessment for **eFiche**, a health technology company in Rwanda, demonstrating enterprise-level data engineering capabilities for healthcare systems. The solution implements a complete end-to-end data pipeline that transforms raw clinical data into actionable business intelligence.

**Project Scope**: Design and implement a scalable healthcare data warehouse that supports clinical decision-making through robust ETL pipelines and analytics capabilities.

### What This System Does

The pipeline processes healthcare data from multiple sources—including synthetic clinical records and the NIH Chest X-ray dataset—through a sophisticated ETL process, storing it in both operational and analytical databases optimized for different workloads. The system enables healthcare administrators, data analysts, and clinical staff to gain insights into patient care patterns, facility utilization, and diagnostic trends.

---

## 💡 Problem Statement

### Healthcare Challenge

Healthcare facilities in Rwanda and across Africa face significant challenges in:

1. **Data Fragmentation**: Clinical data scattered across multiple systems (EHR, PACS, billing) without unified access
2. **Limited Analytics**: Inability to identify disease patterns, resource utilization, or patient outcomes at scale
3. **Slow Decision-Making**: Manual report generation delays critical operational and clinical decisions
4. **Quality Assurance**: Lack of automated data validation leading to inconsistent reporting
5. **Scalability Issues**: Systems unable to handle growing patient volumes and imaging studies

### Solution Delivered

This data engineering solution provides:

- **Unified Data Repository**: Centralized operational database (OLTP) for transactional workloads
- **Analytics Infrastructure**: Optimized data warehouse (OLAP) for business intelligence queries
- **Automated Pipelines**: ETL processes that handle 10,000+ records with data quality validation
- **Real-time Insights**: Sub-second query performance on complex analytical workloads
- **Production-Ready**: Error handling, logging, incremental loading, and QA automation

---

## 👥 Target Audience

This system serves multiple stakeholders in the healthcare ecosystem:

| User Type | Use Cases |
|-----------|-----------|
| **Healthcare Administrators** | Facility utilization, resource planning, operational efficiency metrics |
| **Clinical Directors** | Disease prevalence by demographics, diagnosis patterns, procedure volumes |
| **Data Analysts** | Ad-hoc querying, trend analysis, cohort studies, predictive modeling |
| **Quality Assurance Teams** | Data validation, reporting accuracy, compliance monitoring |
| **IT/Engineering Teams** | Database administration, pipeline monitoring, system maintenance |
| **Research Teams** | Clinical studies, epidemiological analysis, outcome research |

---

## 📸 Visual Walkthrough

### Pipeline Execution
<img src="docs/images/pipeline-execution.png" alt="ETL Pipeline Running" width="600"/>

*ETL pipeline processing 10,000 NIH records with progress logging*

### Database Schema
<img src="docs/images/database-tables.png" alt="Database Tables" width="600"/>

*Complete 14-table architecture (7 operational + 7 warehouse)*

### QA Validation
<img src="docs/images/qa-validation.png" alt="Quality Assurance" width="600"/>

*All 9 automated validation checks passing*

### Analytics Results
<img src="docs/images/query-results.png" alt="Query Results" width="600"/>

*Sample business intelligence query output*


---
## ✨ Key Features

### Core Capabilities

- **📊 Dual-Database Architecture**: Normalized OLTP system + denormalized OLAP warehouse
- **🔄 Robust ETL Pipeline**: Incremental loading with deduplication and error recovery
- **🏥 Real Healthcare Data**: 10,000 NIH Chest X-ray records + 5,000 synthetic patient profiles
- **✅ Automated QA**: 9 validation checks ensuring data integrity end-to-end
- **📈 Business Intelligence**: Pre-built analytical queries for immediate insights
- **🚀 Production-Ready**: Connection pooling, batch processing, comprehensive logging
- **🔍 Advanced Indexing**: Strategic indexes including GIN for full-text search
- **🌍 Rwanda Context**: Localized with Kinyarwanda names, RAMA/MMI insurance, provinces

### Technical Highlights

- **7-table normalized schema** (3NF) optimized for transactional integrity
- **7-table star schema** optimized for analytical query performance
- **48,699 warehouse records** across fact and dimension tables
- **Batch processing** with 6x performance improvement (36 min → 6 min)
- **ICD-10 compliant** diagnosis coding for clinical accuracy
- **DICOM-compliant** imaging procedure metadata

---

## 🏗️ System Architecture

### High-Level Overview

```
┌─────────────────────────────────────────────────────────────┐
│                      DATA SOURCES                           │
├─────────────────────────────────────────────────────────────┤
│  • Synthetic Data Generator (Faker)                         │
│  • NIH Chest X-ray Dataset (HuggingFace - 10K records)      │
│  • Synthetic Report Generator (Clinical narratives)         │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                   ETL PIPELINE                              │
├─────────────────────────────────────────────────────────────┤
│  Extract:  CSV → pandas DataFrame                           │
│  Transform: ICD-10 mapping, Modality normalization          │
│  Load:     Batch insert (2000/batch) with deduplication     │
│                                                             │
│  Features: Incremental loading, Error handling, Logging     │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│          OPERATIONAL DATABASE (PostgreSQL 15)               │
├─────────────────────────────────────────────────────────────┤
│  Schema: 3NF Normalized (OLTP)                              │
│                                                             │
│  Tables:                                                    │
│    • facilities (500)         • diagnoses (50)              │
│    • patients (6,853)         • encounter_diagnoses (44K)   │
│    • encounters (25,000)      • reports (25,000)            │
│    • procedures (40,000)                                    │
│                                                             │
│  Features: Foreign keys, Indexes, CHECK constraints         │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│         DATA WAREHOUSE (Star Schema)                        │
├─────────────────────────────────────────────────────────────┤
│  Schema: Denormalized (OLAP)                                │
│                                                             │
│  Fact Table:                                                │
│    • fact_encounters (10,000)                               │
│                                                             │
│  Dimensions:                                                │
│    • dim_time (732)          • dim_diagnosis (50)           │
│    • dim_patient (6,853)     • dim_procedure (10,000)       │
│                                                             │
│  Bridge Tables:                                             │
│    • bridge_encounter_procedures (10,000)                   │
│    • bridge_encounter_diagnoses (11,064)                    │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│               ANALYTICS LAYER                               │
├─────────────────────────────────────────────────────────────┤
│  Business Intelligence Queries:                             │
│    • Encounters per month (time-series)                     │
│    • Top diagnoses by age group (cohort analysis)           │
│    • Average procedures per patient (utilization)           │
│    • Top facilities by volume (operations)                  │
│                                                             │
│  Outputs: CSV reports, QA validation                        │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

> **Note**: Detailed setup and execution instructions are available in the separate `SETUP.md` file.

### Prerequisites

- Python 3.10+
- PostgreSQL 15+
- 4GB RAM minimum
- 10GB disk space

### Quick Installation

```bash
# Clone repository
git clone https://github.com/Vital-Ahishakiye/ehealth-data-pipeline
cd ehealth-data-pipeline

# Setup environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt

# Configure database
cp .env.example .env
# Edit .env with your PostgreSQL credentials

# Run complete pipeline
python run_pipeline.py
```

### Expected Execution

- **Duration**: ~20 minutes end-to-end
- **Output**: 14 database tables, 3 CSV reports, QA summary
- **Console**: Progress logging with success/error indicators

For detailed instructions, troubleshooting, and advanced configuration, see **[SETUP.md](SETUP.md)**.

---

## 📊 Project Metrics

### Key Performance Indicators

| Metric | Value |
|--------|-------|
| **Total Tables** | 14 (7 operational + 7 warehouse) |
| **Synthetic Data** | 5,000 patients, 15,000 encounters, 50 diagnoses |
| **Real Data** | 10,000 NIH X-ray records with generated reports |
| **Warehouse Records** | 48,699 across all dimensions |
| **QA Status** | 9/9 checks passing ✅ |
| **Execution Time** | ~20 minutes end-to-end |
| **ETL Performance** | 6x faster (36 min → 6 min after optimization) |
| **Query Performance** | <100ms for typical analytical queries |

### Success Criteria

| Metric | Target | Actual | Status |
|--------|--------|--------|--------|
| Tables Created | 14 | 14 | ✅ |
| Synthetic Patients | 5,000 | 5,000 | ✅ |
| NIH Records Loaded | 10,000 | 10,000 | ✅ |
| Warehouse Facts | 10,000 | 10,000 | ✅ |
| QA Checks | 9/9 | 9/9 | ✅ |
| CSV Reports | 3 | 3 | ✅ |
| Execution Time | <30 min | ~20 min | ✅ |

**Overall Status: 100% Complete** ✅

---

## 📚 Technical Documentation

<details>
<summary><b>📊 Part 1: Operational Database Schema</b></summary>

### Design Philosophy

**Normalization**: 3rd Normal Form (3NF)
- **Purpose**: Minimize data redundancy, ensure data integrity
- **Benefits**: Single source of truth, efficient updates, referential integrity
- **Trade-off**: More JOINs required (acceptable for OLTP)

### Entity Relationship Diagram (ERD)

```
┌─────────────┐
│  facilities │
└──────┬──────┘
       │
       │ 1:N
       │
       ▼
┌─────────────┐     1:N     ┌──────────────┐
│  patients   │◄────────────│  encounters  │
└─────────────┘             └──────┬───────┘
                                   │
                    ┌──────────────┼──────────────┐
                    │              │              │
                    │ 1:N          │ 1:N          │ 1:N
                    ▼              ▼              ▼
            ┌──────────────┐ ┌──────────────┐ ┌──────────┐
            │  procedures  │ │ encounter_   │ │ reports  │
            └──────────────┘ │  diagnoses   │ └──────────┘
                             └───────┬──────┘
                                     │
                                     │ N:1
                                     ▼
                             ┌──────────────┐
                             │  diagnoses   │
                             └──────────────┘
```

### Table Specifications

#### 1. **facilities** - Healthcare Locations
```sql
PRIMARY KEY: facility_id (VARCHAR)
Records: 500
Purpose: Hospitals, clinics, imaging centers in Rwanda
Key Fields: facility_name, facility_type, address_state (province)
```

#### 2. **patients** - Patient Master Data
```sql
PRIMARY KEY: patient_id (VARCHAR)
Records: 6,853 (5,000 synthetic + 1,853 NIH)
Purpose: Patient demographics and contact information
Key Fields: date_of_birth, gender, insurance_provider
Rwanda Context: Kinyarwanda language, RAMA/MMI insurance
```

#### 3. **encounters** - Patient Visits
```sql
PRIMARY KEY: encounter_id (VARCHAR)
FOREIGN KEYS: patient_id → patients, facility_id → facilities
Records: 25,000 (15,000 synthetic + 10,000 NIH)
Purpose: Hospital visits, imaging appointments
Key Fields: encounter_date, encounter_datetime, encounter_type
```

#### 4. **procedures** - Imaging Studies
```sql
PRIMARY KEY: procedure_id (SERIAL)
FOREIGN KEY: encounter_id → encounters
Records: 40,000+
Purpose: X-rays, CT scans, MRIs (DICOM-compliant)
Key Fields: modality, body_part, laterality, radiation_dose_mgy
Unique Constraint: (encounter_id, procedure_code) - prevents duplicates
```

#### 5. **diagnoses** - Diagnosis Catalog
```sql
PRIMARY KEY: diagnosis_id (VARCHAR)
Records: 50 (ICD-10 codes)
Purpose: Master diagnosis list (Pneumonia, Edema, etc.)
Key Fields: diagnosis_code (ICD-10), severity, is_chronic, is_reportable
```

#### 6. **encounter_diagnoses** - Junction Table
```sql
PRIMARY KEY: encounter_diagnosis_id (SERIAL)
FOREIGN KEYS: encounter_id → encounters, diagnosis_id → diagnoses
Records: 44,000+ (multiple diagnoses per encounter)
Purpose: Links encounters to diagnoses (M:N relationship)
Key Fields: diagnosis_rank, is_primary, diagnosis_confidence
```

#### 7. **reports** - Radiology Reports
```sql
PRIMARY KEY: report_id (SERIAL)
FOREIGN KEY: encounter_id → encounters
Records: 25,000
Purpose: Full-text radiology reports (structured + unstructured)
Key Fields: report_text, findings, impression, recommendations
Special Index: GIN index on report_text (full-text search)
```

### Design Highlights

**1. Data Integrity**
- ✅ All foreign keys enforced with `ON DELETE CASCADE`
- ✅ CHECK constraints on enumerations (modality, severity, etc.)
- ✅ NOT NULL constraints on critical fields
- ✅ UNIQUE constraints prevent duplicates

**2. Performance Optimization**
- ✅ Strategic indexes on foreign keys and frequently queried columns
- ✅ Composite indexes for common query patterns
- ✅ GIN index for full-text search on reports

**3. Scalability**
- ✅ SERIAL primary keys for high-volume tables (auto-increment)
- ✅ Partitioning-ready (can partition by encounter_date)
- ✅ Connection pooling support via psycopg2

**4. Future-Proofing**
- ✅ Schema supports embeddings (can add vector columns)
- ✅ Report text ready for NLP/transformers
- ✅ Audit trail fields (created_at, updated_at)

</details>

<details>
<summary><b>🔄 Part 2: ETL Pipeline Design</b></summary>

### Pipeline Architecture

**Data Sources**:
1. **NIH Chest X-ray Dataset** (HuggingFace): 10,000 records with labels
2. **Synthetic Generator**: 5,000 patient records with realistic demographics
3. **Report Generator**: Clinical narratives matching imaging findings

**ETL Process**:

```python
# Extract
dataset = load_dataset("alkzar90/NIH-Chest-X-ray-dataset", split="train")
df = dataset.to_pandas().head(10000)

# Transform
df['icd10_code'] = df['Finding Labels'].map(diagnosis_mapping)
df['modality'] = 'DX'  # Digital Radiography
df['body_part'] = 'CHEST'

# Load (with deduplication)
existing_ids = get_existing_encounter_ids()
new_records = df[~df['encounter_id'].isin(existing_ids)]
batch_insert(new_records, batch_size=2000)
```

**Key Features**:
- **Incremental Loading**: Only processes new records
- **Error Recovery**: Transaction rollback on failures
- **Logging**: Comprehensive audit trail
- **Validation**: Data quality checks at each stage

**Performance Optimizations**:
1. **Batch Processing**: `execute_batch()` with page_size=1000 (6x faster)
2. **Connection Pooling**: Reuse database connections (30% faster)
3. **Pre-loading Reference Data**: Cache diagnosis codes (eliminates 10K+ queries)
4. **Strategic Indexing**: Foreign keys and frequent queries indexed

**Dataset Choice Justification**:
- **NIH Chest X-ray Dataset**: 112,120 frontal-view X-ray images from 30,805 unique patients
- **Clinical Relevance**: 14 disease labels (Pneumonia, Edema, Atelectasis, etc.)
- **Quality**: Labeled by radiologists, suitable for clinical decision support
- **Scale**: Large enough to demonstrate production-level ETL capabilities

</details>

<details>
<summary><b>🏢 Part 3: Data Warehouse Design</b></summary>

### Star Schema Architecture

**Fact Table**: `fact_encounters`
- Grain: One row per patient encounter
- Measures: procedure_count, diagnosis_count, total_cost
- Foreign Keys: date_id, patient_key, facility_key

**Dimension Tables**:
1. **dim_time**: Date hierarchy (day, month, quarter, year)
2. **dim_patient**: Patient demographics with age groups
3. **dim_facility**: Healthcare facility details
4. **dim_procedure**: Procedure catalog with modality
5. **dim_diagnosis**: ICD-10 diagnosis master list

**Bridge Tables** (for many-to-many relationships):
1. **bridge_encounter_procedures**: Links encounters to multiple procedures
2. **bridge_encounter_diagnoses**: Links encounters to multiple diagnoses

### Key Mapping Strategy

**Challenge**: Operational schema uses VARCHAR keys, warehouse needs INTEGER

**Solution**: Extract numeric portion from VARCHAR
```sql
-- Operational: 'PAT0010001' → Warehouse: 10001
CAST(REGEXP_REPLACE(patient_id, '\D', '', 'g') AS INTEGER)
```

**Applied to**:
- patient_id: 'PAT0010001' → 10001
- encounter_id: 'ENC00000001' → 1
- facility_id: 'FAC000001' → 1
- diagnosis_id: 'DIAG001' → 1

### Analytics Queries

**1. Encounters Per Month (Time-Series)**
```sql
SELECT dt.year, dt.month, COUNT(f.encounter_key) AS total_encounters
FROM fact_encounters f
JOIN dim_time dt ON f.date_id = dt.date_id
GROUP BY dt.year, dt.month
ORDER BY dt.year, dt.month;
```
**Insight**: Seasonal trends, capacity planning

**2. Top Diagnoses by Age Group (Cohort Analysis)**
```sql
WITH ranked AS (
  SELECT dp.age_group, dd.diagnosis_name,
         RANK() OVER (PARTITION BY dp.age_group ORDER BY COUNT(*) DESC) AS rank
  FROM fact_encounters f
  JOIN dim_patient dp ON f.patient_key = dp.patient_key
  JOIN bridge_encounter_diagnoses bd ON f.encounter_key = bd.encounter_key
  JOIN dim_diagnosis dd ON bd.diagnosis_key = dd.diagnosis_key
  WHERE bd.diagnosis_type = 'Primary'
  GROUP BY dp.age_group, dd.diagnosis_name
)
SELECT * FROM ranked WHERE rank <= 5;
```
**Insight**: Age-specific disease patterns

**3. Average Procedures Per Patient (Utilization)**
```sql
SELECT dp.age_group,
       SUM(f.procedure_count) / COUNT(DISTINCT dp.patient_key) AS avg_procedures
FROM fact_encounters f
JOIN dim_patient dp ON f.patient_key = dp.patient_key
GROUP BY dp.age_group;
```
**Insight**: Resource utilization by demographic

### QA Validation Suite

**9 Automated Checks**:
1. ✅ No duplicate patient IDs
2. ✅ No missing demographics
3. ✅ No orphan encounters
4. ✅ No duplicate encounter IDs
5. ✅ No missing dates
6. ✅ No orphan procedures in bridge
7. ✅ No duplicate procedure codes
8. ✅ All fact rows have valid dimension keys
9. ✅ Row count reconciliation (operational vs warehouse)

**All checks passing** ✅

</details>

---

## 🎯 Results

### Deliverables Completed

#### Part 1: Data Modeling ✅
- [x] 7 operational tables (SQL DDL files)
- [x] 7 warehouse tables (SQL DDL files)
- [x] Schema creation script
- [x] Synthetic data generation script
- [x] Design rationale document
- [x] Validation queries

#### Part 2: ETL Pipeline ✅
- [x] NIH dataset extraction script
- [x] Synthetic report generation script
- [x] ETL pipeline with incremental loading
- [x] Pipeline orchestrator
- [x] Dataset choice justification
- [x] Incremental loading explanation
- [x] Sample execution logs

#### Part 3: Analytics ✅
- [x] Warehouse population script
- [x] Analytics query execution script
- [x] QA validation automation
- [x] Business intelligence SQL queries
- [x] CSV output reports
- [x] QA summary document

#### Bonus ✅
- [x] Docker Compose configuration (reference)
- [x] Airflow DAG definition
- [x] Comprehensive documentation
- [x] Performance optimizations

### Sample Outputs

**CSV Reports Generated**:
1. `encounters_per_month.csv` - Time-series analysis
2. `top_diagnoses_by_age_group.csv` - Cohort analysis
3. `avg_procedures_per_patient.csv` - Utilization metrics

**QA Validation Results**:
```
[INFO] Running QA validation...
✅ Check 1: No duplicate patient IDs (PASSED)
✅ Check 2: No missing demographics (PASSED)
✅ Check 3: No orphan encounters (PASSED)
...
✅ All 9 checks passed
```

---

## 📈 Future Enhancements

### Potential Improvements

**1. Real-time Streaming**
- Replace batch ETL with Apache Kafka
- Sub-second latency for critical alerts
- Event-driven architecture

**2. Advanced Analytics**
- Patient journey visualization
- Predictive modeling (readmission risk, diagnosis probability)
- Diagnosis co-occurrence network analysis

**3. Embeddings Integration**
- Add vector columns for report embeddings (pgvector)
- Semantic search across radiology reports
- Similarity-based case recommendations

**4. Multi-tenancy Support**
- Support multiple hospital systems
- Row-level security (RLS) in PostgreSQL
- Separate schemas per tenant

**5. Data Quality Monitoring**
- Great Expectations integration
- Automated anomaly detection
- Data drift monitoring with alerts

**6. API Layer**
- RESTful API for external integrations
- GraphQL for flexible querying
- OAuth 2.0 authentication

---

## 🛠️ Technology Stack

| Layer | Technology | Version | Purpose |
|-------|-----------|---------|---------|
| **Database** | PostgreSQL | 15+ | OLTP + OLAP storage |
| **Language** | Python | 3.10+ | ETL scripting |
| **Data Processing** | Pandas | 2.1.4 | DataFrame operations |
| **Database Driver** | psycopg2 | 2.9.9 | PostgreSQL connection |
| **Synthetic Data** | Faker | 22.0.0 | Realistic test data |
| **Dataset** | datasets | 2.16.1 | HuggingFace integration |
| **Orchestration** | Apache Airflow | 2.8.0 | Pipeline scheduling (reference) |
| **Containerization** | Docker Compose | 3.9 | Service orchestration (reference) |

### Why These Technologies?

- **PostgreSQL**: Industry standard for healthcare systems, HIPAA-compliant, excellent JSON support
- **Python + Pandas**: Data science ecosystem, healthcare library support
- **HuggingFace datasets**: Access to curated medical datasets
- **Faker**: Realistic synthetic data for GDPR/HIPAA compliance in testing

---

## 🎯 Design Principles

### 1. Production-Ready Code
- Comprehensive error handling and logging
- Environment-based configuration management
- Idempotent operations (safe to re-run)
- Extensive testing coverage

### 2. Scalability
- Indexed for query performance
- Partitioning-ready design
- Batch processing architecture
- Connection pooling support

### 3. Data Quality
- Foreign key constraints enforce relationships
- CHECK constraints validate enumerations
- UNIQUE constraints prevent duplicates
- Automated QA validation pipeline

### 4. Maintainability
- Clear, modular code structure
- Comprehensive inline documentation
- Consistent naming conventions (snake_case)
- Version control best practices

### 5. Extensibility
- Schema supports future embeddings (vector columns)
- Report text ready for NLP processing
- Audit trail fields for compliance
- Flexible configuration system

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 👤 Author

**Your Name**
- GitHub: [@Vital-Ahishakiye](https://github.com/Vital-Ahishakiye/)
- LinkedIn: [Vital Ahishakiye](https://www.linkedin.com/in/vitalahishakiye/)
- Portfolio: [Other Projects](https://vital-ahishakiye.github.io/)
- Email: vitalahishakiye@gmail.com

---

## 🙏 Acknowledgments

- **eFiche**: For providing the technical assessment opportunity
- **NIH Clinical Center**: For the Chest X-ray dataset
- **HuggingFace**: For dataset hosting and tools
- **PostgreSQL Community**: For excellent documentation

---

## 📞 Support

For questions or issues:
1. Check the [SETUP.md](SETUP.md) for detailed instructions
2. Review the [Issues](https://github.com/yourusername/ehealth-data-pipeline/issues) page
3. Open a new issue with the `question` label

---
