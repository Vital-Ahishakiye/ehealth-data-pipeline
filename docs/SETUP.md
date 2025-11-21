# Deployment Guide - How to Run the System

> **Quick reference for setting up and executing the ehealth data pipeline**

---

## 🚀 Prerequisites (5 minutes)

### Required Software

| Software | Version | Download Link |
|----------|---------|---------------|
| Python | 3.10+ | https://www.python.org/downloads/ |
| PostgreSQL | 15+ | https://www.postgresql.org/download/ |

### Verify Installation

```bash
python --version    # Should show 3.10+
psql --version     # Should show 15+
```

---

## ⚡ Quick Setup (5 minutes)

### Step 1: Create Virtual Environment

```bash
# Create venv
python -m venv venv

# Activate venv
# Windows:
venv\Scripts\activate

# macOS/Linux:
source venv/bin/activate
```

### Step 2: Install Dependencies

```bash
pip install -r requirements.txt
```

Expected output:
```
Successfully installed pandas-2.1.4 psycopg2-binary-2.9.9 faker-22.0.0 ...
```

### Step 3: Setup Database

```bash
# Create database
psql -U postgres -c "CREATE DATABASE ehealth_db;"

# Or using psql shell:
psql -U postgres
CREATE DATABASE ehealth_db;
\q
```

### Step 4: Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit .env file and set your PostgreSQL password
# DB_PASSWORD=your_password_here
```

**Edit `.env` file:**
```ini
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ehealth_db
DB_USER=postgres
DB_PASSWORD=your_password_here  # ⚠️ CHANGE THIS

NIH_DATASET_SIZE=10000
SYNTHETIC_PATIENTS=5000
SYNTHETIC_ENCOUNTERS=15000
```

---

## 🎯 Execution Guide (15-20 minutes)

### Option 1: Complete End-to-End Run (Recommended)

Execute all parts sequentially:

```bash
# Part 1: Create schema + generate synthetic data (2 min)
python part1_data_modeling/run_part1.py

# Part 2: ETL Pipeline
python part2_pipeline/extract_nih_dataset.py          # Download NIH (2 min)
python part2_pipeline/generate_synthetic_reports.py   # Generate reports (3 min)
python part2_pipeline/etl_pipeline.py                 # Load data (5-7 min)

# Part 3: Warehouse + Analytics
python part3_analytics/run_analytics.py               # Run analytics (2 min)
python part3_analytics/run_warehouse_qa.py            # QA checks (30 sec)
```

**Total time: ~15-20 minutes**

---

### Option 2: Step-by-Step with Verification

Execute each component individually with validation:

#### **PART 1: Database Schema (2 minutes)**

```bash
# Create all tables
python part1_data_modeling/create_schema.py
```

**Expected output:**
```
✅ Operational schema: 7/7 tables created
✅ Warehouse schema: 7/7 tables created
```

**Verify:**
```bash
psql -U postgres -d ehealth_db -c "\dt"
# Should show 14 tables
```

```bash
# Generate synthetic data
python part1_data_modeling/generate_synthetic_data.py
```

**Expected output:**
```
✅ Generated 500 facilities
✅ Generated 5,000 patients
✅ Generated 15,000 encounters
✅ Generated 30,000+ procedures
```

**Verify:**
```bash
psql -U postgres -d ehealth_db -c "SELECT COUNT(*) FROM patients;"
# Should return 5000
```

---

#### **PART 2: ETL Pipeline (10-12 minutes)**

```bash
# Step 1: Download NIH dataset
python part2_pipeline/extract_nih_dataset.py
```

**Expected output:**
```
✅ Downloaded 112,120 records
✅ Sampled 10,000 records
✅ Saved to: part2_pipeline/data/nih_subset_10k.csv
```

**Verify:**
```bash
ls -lh part2_pipeline/data/nih_subset_10k.csv
# Should exist (~2MB)
```

```bash
# Step 2: Generate radiology reports
python part2_pipeline/generate_synthetic_reports.py
```

**Expected output:**
```
✅ Loaded 10,000 records
✅ Generated 10,000 reports
✅ Saved to: part2_pipeline/data/nih_with_reports.csv
```

**Verify:**
```bash
head -5 part2_pipeline/data/nih_with_reports.csv
# Should show CSV with report_text column
```

```bash
# Step 3: Run ETL pipeline (incremental load)
python part2_pipeline/etl_pipeline.py
```

**Expected output:**
```
✅ Extracted 10,000 records
✅ Transformed 10,000 records
✅ Loaded 10,000 records

Statistics:
  Records Processed: 10,000
  Records Skipped: 0
  Patients Created: ~1,245
  Encounters Created: 10,000
  Procedures Created: 10,000
```

**Verify:**
```bash
psql -U postgres -d ehealth_db -c "
  SELECT COUNT(*) FROM procedures WHERE procedure_code LIKE 'NIH_%';
"
# Should return 10000
```

**Test Incremental Loading (Optional):**
```bash
# Run again - should skip all duplicates
python part2_pipeline/etl_pipeline.py

# Expected: Records Skipped: 10,000
```

---

#### **PART 3: Warehouse & Analytics (2-3 minutes)**

```bash
# Populate data warehouse
python part3_analytics/populate_warehouse.py
```

**Expected output:**
```
✅ Dim Time: 732 records
✅ Dim Patient: 6,853 records
✅ Dim Procedure: 10,000 records
✅ Dim Diagnosis: 50 records
✅ Fact Encounters: 10,000 records
✅ Bridge tables: 21,064 records
```

**Verify:**
```bash
psql -U postgres -d ehealth_db -c "SELECT COUNT(*) FROM fact_encounters;"
# Should return 10000
```

```bash
# Run analytics queries
python part3_analytics/run_analytics.py
```

**Expected output:**
```
✅ Query: encounters_per_month (24 rows)
✅ Query: top_diagnoses_by_age (35 rows)
✅ Query: avg_procedures_per_patient (5 rows)
```

**Verify:**
```bash
ls part3_analytics/*_results.csv
# Should show 3 CSV files
```

```bash
# Run QA validation
python part3_analytics/run_warehouse_qa.py
```

**Expected output:**
```
✅ All 9 QA checks: PASS
✅ Summary: warehouse_qa_summary.md
```

**Verify:**
```bash
cat part3_analytics/warehouse_qa_summary.md
# Should show all checks PASSING
```

---

## ✅ Final Verification

Run complete validation suite:

```bash
# Check database state
psql -U postgres -d ehealth_db -c "
  SELECT 
    'Patients' AS entity, COUNT(*) FROM patients
  UNION ALL SELECT 'Encounters', COUNT(*) FROM encounters
  UNION ALL SELECT 'Procedures', COUNT(*) FROM procedures
  UNION ALL SELECT 'Diagnoses', COUNT(*) FROM encounter_diagnoses
  UNION ALL SELECT 'Reports', COUNT(*) FROM reports
  UNION ALL SELECT 'Fact Encounters', COUNT(*) FROM fact_encounters
  UNION ALL SELECT 'Dim Patient', COUNT(*) FROM dim_patient
  UNION ALL SELECT 'Dim Procedure', COUNT(*) FROM dim_procedure;
"
```

**Expected output:**
```
      entity       | count
-------------------+-------
 Patients          |  6853
 Encounters        | 25000
 Procedures        | 40000
 Diagnoses         | 44000
 Reports           | 25000
 Fact Encounters   | 10000
 Dim Patient       |  6853
 Dim Procedure     | 10000
```

```bash
# Run validation queries
psql -U postgres -d ehealth_db -f part1_data_modeling/sql/validation.sql

# Check outputs
ls -lh part3_analytics/*_results.csv
cat part3_analytics/warehouse_qa_summary.md
```

**✅ If all numbers match, the system is working correctly!**

---

## 🐳 Docker Setup (Optional - If Docker is Installed)

### Prerequisites

- Docker Desktop installed and running
- Docker Compose installed

### Missing Dockerfile

Create `Dockerfile` in project root:

```dockerfile
FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Set Python path
ENV PYTHONPATH=/app

# Default command
CMD ["python", "part1_data_modeling/run_part1.py"]
```

### Run with Docker

```bash
# Start all services
docker-compose up -d

# Check logs
docker-compose logs -f app

# Access Airflow UI
# http://localhost:8080
# Username: admin
# Password: admin

# Stop services
docker-compose down
```

### Airflow Setup

```bash
# Initialize Airflow database
docker-compose run airflow-webserver airflow db init

# Create admin user
docker-compose run airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@ehealth.rw

# Enable DAG in UI
# Navigate to http://localhost:8080
# Toggle switch for "ehealth_pipeline"
```

---

## 🔧 Troubleshooting

### Issue 1: Database Connection Failed

**Error:**
```
psycopg2.OperationalError: could not connect to server
```

**Solution:**
```bash
# Windows: Start PostgreSQL service
# Services → PostgreSQL → Start

# macOS:
brew services start postgresql@18

# Linux:
sudo systemctl start postgresql

# Test connection
psql -U postgres -c "SELECT version();"
```

---

### Issue 2: Module Not Found

**Error:**
```
ModuleNotFoundError: No module named 'psycopg2'
```

**Solution:**
```bash
# Ensure venv is activated
source venv/bin/activate  # Linux/macOS
venv\Scripts\activate     # Windows

# Reinstall dependencies
pip install -r requirements.txt
```

---

### Issue 3: Permission Denied (PostgreSQL)

**Error:**
```
FATAL: password authentication failed for user "postgres"
```

**Solution:**
```bash
# Reset PostgreSQL password
psql -U postgres
ALTER USER postgres PASSWORD 'new_password';
\q

# Update .env file
DB_PASSWORD=new_password
```

---

### Issue 4: NIH Dataset Download Fails

**Error:**
```
HTTPError: 503 Server Error
```

**Solution:**
```bash
# Retry download (temporary HuggingFace issue)
python part2_pipeline/extract_nih_dataset.py

# If persists, check internet connection or if the link has been updated
# Dataset URL: https://huggingface.co/datasets/alkzar90/NIH-Chest-X-ray-dataset
```

---

### Issue 5: Out of Memory

**Error:**
```
MemoryError: Unable to allocate array
```

**Solution:**
```python
# Edit part2_pipeline/config.py
# Reduce batch size
BATCH_SIZE = 1000  # Instead of 2000

# Or reduce dataset size
NIH_DATASET_SIZE = 5000  # Instead of 10000
```

---

## 📊 Performance Benchmarks

Tested on:
- **CPU**: Intel Core i5 (4 cores)
- **RAM**: 16GB
- **Storage**: SSD
- **OS**: Windows 11 / Ubuntu 22.04

| Task | Time | Notes |
|------|------|-------|
| Schema creation | 30s | All 14 tables |
| Synthetic data | 2min | 5K patients, 15K encounters |
| NIH download | 2min | 10K records from HuggingFace |
| Report generation | 3min | 10K radiology reports |
| ETL pipeline | 5-7min | Batch processing (2000/batch) |
| Warehouse population | 2min | Star schema transformation |
| Analytics queries | 1min | 3 queries + CSV export |
| QA validation | 30s | 9 automated checks |
| **Total** | **15-20min** | **Complete end-to-end** |

---

## 📧 Support

If you encounter issues:

1. Check this guide's troubleshooting section
2. Review logs in `part2_pipeline/logs/`
3. Verify database connection with `psql -U postgres -d ehealth_db`
4. Check `.env` configuration
5. Ensure all prerequisites are installed

---

