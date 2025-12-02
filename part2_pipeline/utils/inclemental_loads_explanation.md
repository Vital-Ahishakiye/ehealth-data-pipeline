# Incremental Loads - Implementation & Strategy

## Overview

This document explains how the ETL pipeline handles **incremental data loading**, ensuring efficient processing of new data while preventing duplicates.

---

## 1. What is Incremental Loading?

**Incremental Loading** is an ETL strategy where only **new or changed records** are processed, rather than reprocessing the entire dataset on each run.

**Benefits:**
- ⚡ **Performance**: Faster execution (process only new data)
- 💾 **Resource Efficiency**: Less CPU, memory, and I/O
- 🔒 **Data Integrity**: Prevents duplicate records
- 📊 **Scalability**: Handles growing datasets efficiently

---

## 2. Implementation Strategy

### 2.1 Change Data Capture (CDC) Approach

Our pipeline uses **key-based CDC** to identify new records:

```python
# Step 1: Query existing records
existing_indices = db.get_existing_image_indices()
# Returns: {'NIH_00000001_000', 'NIH_00000002_000', ...}

# Step 2: Filter new records only
df_new = df[~df['procedure_code'].isin(existing_indices)]

# Step 3: Process only new records
etl_pipeline.process(df_new)
```

### 2.2 Unique Key Strategy

Each NIH image is assigned a **unique procedure code**:

```python
procedure_code = f"NIH_{image_index.replace('.png', '')}"
# Example: "NIH_00000123_000"
```

**Why this works:**
- NIH `Image Index` is **globally unique**
- Maps directly to our `procedures.procedure_code` column
- Fast lookup via index
- No collisions with synthetic data (different prefix pattern)

---

## 3. Database-Level Duplicate Prevention

### 3.1 Constraints

```sql
-- Procedures table
ALTER TABLE procedures 
ADD CONSTRAINT procedures_pkey PRIMARY KEY (procedure_code);

-- Encounters table
ALTER TABLE encounters 
ADD CONSTRAINT encounters_pkey PRIMARY KEY (encounter_id);
```

### 3.2 Upsert Logic

```sql
INSERT INTO procedures (encounter_id, procedure_code, ...)
VALUES (%s, %s, ...)
ON CONFLICT (procedure_code) DO NOTHING;
```

**Behavior:**
- If `procedure_code` exists → **SKIP** (no error, no update)
- If `procedure_code` is new → **INSERT**

This ensures **idempotency**: running the pipeline multiple times produces the same result.

---

## 4. Pipeline Execution Flow

### 4.1 First Run (Initial Load)

```
┌─────────────────────────────────────────────────┐
│ RUN #1: Initial Load                            │
├─────────────────────────────────────────────────┤
│ 1. Extract: Load 10,000 records from CSV       │
│ 2. Transform: All 10,000 are new               │
│ 3. Load: Insert all 10,000 records             │
│                                                 │
│ Result:                                         │
│    Processed: 10,000                            │
│    Skipped: 0                                   │
│    New Patients: ~500                           │
│    New Encounters: 10,000                       │
│    New Procedures: 10,000                       │
└─────────────────────────────────────────────────┘
```

### 4.2 Second Run (Incremental - No New Data)

```
┌─────────────────────────────────────────────────┐
│ RUN #2: Incremental (Same Data)                │
├─────────────────────────────────────────────────┤
│ 1. Extract: Load 10,000 records from CSV       │
│ 2. Transform: Check existing_indices           │
│    - Found: 10,000 existing records             │
│    - Filter: 0 new records                      │
│ 3. Load: SKIP (nothing to load)                │
│                                                 │
│ Result:                                         │
│    Processed: 0                                 │
│    Skipped: 10,000                              │
│    New Patients: 0                              │
│    New Encounters: 0                            │
│    New Procedures: 0                            │
└─────────────────────────────────────────────────┘
```

### 4.3 Third Run (Incremental - New Data Added)

```
┌─────────────────────────────────────────────────┐
│ RUN #3: Incremental (2,000 New Records)        │
├─────────────────────────────────────────────────┤
│ 1. Extract: Load 12,000 records from CSV       │
│    (10,000 old + 2,000 new)                     │
│ 2. Transform: Check existing_indices           │
│    - Found: 10,000 existing records             │
│    - Filter: 2,000 new records                  │
│ 3. Load: Insert 2,000 new records               │
│                                                 │
│ Result:                                         │
│    Processed: 2,000                             │
│    Skipped: 10,000                              │
│    New Patients: ~100                           │
│    New Encounters: 2,000                        │
│    New Procedures: 2,000                        │
└─────────────────────────────────────────────────┘
```

---

## 5. Code Implementation

### 5.1 Extract Phase

```python
def extract(self):
    """Extract data from CSV"""
    csv_path = DATA_DIR / "nih_with_reports.csv"
    return pd.read_csv(csv_path)
```

### 5.2 Transform Phase (Incremental Logic)

```python
def transform(self, df):
    """Transform with incremental filtering"""
    
    # Get existing records
    existing_indices = set()
    if self.incremental:
        existing_indices = self.db.get_existing_image_indices()
        print(f"Found {len(existing_indices):,} existing records")
    
    # Generate procedure codes
    df['nih_procedure_code'] = 'NIH_' + df['Image Index'].str.replace('.png', '')
    
    # Filter new records only
    if self.incremental and existing_indices:
        df_new = df[~df['nih_procedure_code'].isin(existing_indices)].copy()
        self.stats['records_skipped'] = len(df) - len(df_new)
        df = df_new
    
    if len(df) == 0:
        print("No new records to process")
        return None
    
    self.stats['records_processed'] = len(df)
    return df
```

### 5.3 Load Phase (Upsert)

```python
def load(self, df):
    """Load with duplicate prevention"""
    
    self.db.execute_many("""
        INSERT INTO procedures (encounter_id, procedure_code, ...)
        VALUES (%s, %s, ...)
        ON CONFLICT (procedure_code) DO NOTHING
    """, procedures)
```

---

## 6. Performance Optimization

### 6.1 Batch Processing

```python
BATCH_SIZE = 500  # Process 500 records at a time

for batch_num in range(total_batches):
    batch_df = df.iloc[start_idx:end_idx]
    self._load_batch(batch_df, logger)
```

**Benefits:**
- Reduces memory usage
- Commits data incrementally
- Better error recovery

### 6.2 Index Usage

```sql
-- Fast lookup of existing records
CREATE INDEX idx_procedures_code ON procedures(procedure_code);

-- Efficient filtering
SELECT procedure_code FROM procedures WHERE procedure_code LIKE 'NIH_%';
```

### 6.3 Connection Pooling

```python
# Uses psycopg2's connection pooling
with self.db.get_connection() as conn:
    # Reuse connections
    cursor.execute(...)
```

---

## 7. Handling Edge Cases

### 7.1 Partial Failures

**Scenario:** Pipeline fails midway through batch processing.

**Solution:**
- Each batch is committed independently
- Partially loaded data remains in database
- Re-running pipeline will skip already-loaded records
- No data loss or duplicates

### 7.2 Concurrent Executions

**Scenario:** Two pipeline instances run simultaneously.

**Solution:**
- `ON CONFLICT DO NOTHING` prevents duplicates
- PostgreSQL MVCC handles concurrent writes
- Both instances complete successfully
- No duplicate records created

### 7.3 Data Updates

**Scenario:** NIH dataset is updated with corrected metadata.

**Current Behavior:**
- Pipeline skips existing records (no updates)

**Future Enhancement:**
- Implement `ON CONFLICT DO UPDATE` for specific fields
- Add `last_updated` timestamp tracking
- Compare timestamps to detect changes

---

## 8. Testing Incremental Loads

### 8.1 Simulation Script

```bash
# Run pipeline 3 times with 5-second delay
python run_pipeline.py --mode simulate --runs 3 --delay 5
```

**Output:**
```
RUN #1: Processed: 10,000 | Skipped: 0
RUN #2: Processed: 0      | Skipped: 10,000
RUN #3: Processed: 0      | Skipped: 10,000
```

### 8.2 Manual Testing

```bash
# Run 1: Initial load
python etl_pipeline.py

# Run 2: Verify no duplicates
python etl_pipeline.py

# Check database
psql -U postgres -d efiche_db -c "SELECT COUNT(*) FROM procedures WHERE procedure_code LIKE 'NIH_%';"
```

---

## 9. Monitoring & Logging

### 9.1 Pipeline Statistics

Every run logs:
- **Records Processed**: New records loaded
- **Records Skipped**: Existing records (duplicates)
- **Patients Created**: New patient records
- **Encounters Created**: New encounters
- **Procedures Created**: New imaging studies

### 9.2 Log Files

```
logs/
├── pipeline_20250116_103045.log  # Run #1
├── pipeline_20250116_104120.log  # Run #2
└── pipeline_20250116_105230.log  # Run #3
```

Each log includes:
- Timestamp
- Records processed/skipped
- Errors (if any)
- Execution duration

---

## 10. Production Considerations

### 10.1 Scheduling

**Apache Airflow DAG (example):**

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

dag = DAG(
    'nih_etl_pipeline',
    schedule_interval=timedelta(hours=6),  # Every 6 hours
    start_date=datetime(2025, 1, 1),
    catchup=False
)

run_etl = PythonOperator(
    task_id='run_nih_etl',
    python_callable=run_pipeline,
    dag=dag
)
```

### 10.2 Alert System

- **Success**: Log completion, send success email
- **Failure**: Alert on-call engineer, retry with backoff
- **No New Data**: Log info, no alert needed

### 10.3 Data Quality Checks

```sql
-- Post-load validation
SELECT 
    COUNT(DISTINCT procedure_code) as unique_procedures,
    COUNT(*) as total_procedures
FROM procedures 
WHERE procedure_code LIKE 'NIH_%';

-- Should match: unique_procedures == total_procedures
```

---

## 11. Future Enhancements

### 11.1 Change Tracking

- Add `last_modified` timestamp to source data
- Compare timestamps to detect updates
- Implement `ON CONFLICT DO UPDATE` for changed records

### 11.2 Soft Deletes

- Track deleted records in source
- Mark as `is_deleted = true` in database
- Preserve audit trail

### 11.3 Delta Files

- Process only daily delta files (e.g., `nih_delta_20250116.csv`)
- Reduce file size
- Faster processing

---

## 12. Summary

**Key Takeaways:**

1. ✅ **Idempotent**: Safe to re-run multiple times
2. ✅ **Efficient**: Processes only new data
3. ✅ **Scalable**: Handles growing datasets
4. ✅ **Reliable**: Prevents duplicates at database level
5. ✅ **Monitorable**: Comprehensive logging and statistics

**Implementation Pattern:**
```
Extract → Check Existing → Filter New → Load (Upsert) → Log Stats
```

This strategy ensures the pipeline can run continuously (hourly, daily) without data duplication or performance degradation.