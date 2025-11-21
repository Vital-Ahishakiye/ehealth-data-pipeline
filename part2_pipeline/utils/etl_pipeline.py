"""
ETL Pipeline - OPTIMIZED VERSION
Reduces execution time from 36 minutes to ~5 minutes
"""

import sys, io
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
import random

sys.path.insert(0, str(Path(__file__).parent))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
from psycopg2.extras import execute_batch
from part2_pipeline.config import DATA_DIR, NIH_TO_ICD10_MAPPING, MODALITY_MAPPING, BATCH_SIZE
from part2_pipeline.utils.db_helper import DatabaseHelper
from part2_pipeline.utils.logger import PipelineLogger

fake = Faker()
Faker.seed(42)
random.seed(42)


class NIH_ETL_Pipeline:
    """ETL Pipeline for NIH dataset - OPTIMIZED"""
    
    def __init__(self, incremental=True):
        self.db = DatabaseHelper()
        self.incremental = incremental
        self.stats = {
            'records_processed': 0,
            'records_skipped': 0,
            'patients_created': 0,
            'encounters_created': 0,
            'procedures_created': 0,
            'diagnoses_assigned': 0,
            'reports_created': 0
        }
        # ✅ OPTIMIZATION: Pre-load reference data
        self.diagnosis_cache = self._load_diagnosis_cache()
        self.facility_ids = self._load_facility_ids()
    
    def _load_diagnosis_cache(self):
        """Pre-load all diagnoses into memory"""
        query = "SELECT diagnosis_code, diagnosis_id FROM diagnoses"
        results = self.db.execute_query(query, fetch=True)
        return {code: diag_id for code, diag_id in results}
    
    def _load_facility_ids(self):
        """Pre-load hospital facility IDs"""
        query = "SELECT facility_id FROM facilities WHERE facility_type = 'Hospital'"
        results = self.db.execute_query(query, fetch=True)
        return [row[0] for row in results]
    
    def extract(self):
        """Extract data from CSV"""
        csv_path = DATA_DIR / "nih_with_reports.csv"
        
        if not csv_path.exists():
            raise FileNotFoundError(f"Dataset not found: {csv_path}")
        
        return pd.read_csv(csv_path)
    
    def transform(self, df):
        """Transform NIH data to match eHealth schema"""
        
        existing_indices = set()
        if self.incremental:
            existing_indices = self.db.get_existing_image_indices()
            print(f"   Found {len(existing_indices):,} existing records")
        
        if self.incremental and existing_indices:
            df['nih_procedure_code'] = 'NIH_' + df['Image Index'].str.replace('.png', '')
            df_new = df[~df['nih_procedure_code'].isin(existing_indices)].copy()
            self.stats['records_skipped'] = len(df) - len(df_new)
            df = df_new
        else:
            df['nih_procedure_code'] = 'NIH_' + df['Image Index'].str.replace('.png', '')
        
        if len(df) == 0:
            print("    No new records to process")
            return None
        
        self.stats['records_processed'] = len(df)
        
        print(f"   Transforming {len(df):,} records...")
        
        df['modality_mapped'] = df['View Position'].map(lambda x: MODALITY_MAPPING.get(x, 'X-Ray'))
        
        start_date = datetime.now() - timedelta(days=730)
        df['encounter_datetime'] = [
            start_date + timedelta(days=random.randint(0, 730), hours=random.randint(0, 23))
            for _ in range(len(df))
        ]
        df['encounter_date'] = df['encounter_datetime'].dt.date
        
        df['diagnosis_list'] = df['Finding Labels'].str.split('|')
        df['primary_diagnosis'] = df['diagnosis_list'].apply(lambda x: x[0].strip())
        
        return df
    
    def load(self, df, logger):
        """Load with SINGLE connection and bulk inserts"""
        
        if df is None or len(df) == 0:
            return
        
        logger.info(f"\n Loading {len(df):,} records...")
        
        # ✅ OPTIMIZATION: Use SINGLE database connection for entire batch
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            
            # Process in larger batches
            total_batches = (len(df) // BATCH_SIZE) + 1
            
            for batch_num in range(total_batches):
                start_idx = batch_num * BATCH_SIZE
                end_idx = min(start_idx + BATCH_SIZE, len(df))
                batch_df = df.iloc[start_idx:end_idx]
                
                logger.info(f"   Batch {batch_num + 1}/{total_batches} ({len(batch_df)} records)...")
                
                self._load_batch_optimized(batch_df, cursor, logger)
                
                # Commit after each batch
                conn.commit()
            
            cursor.close()
    
    def _load_batch_optimized(self, batch_df, cursor, logger):
        """Optimized batch loading with minimal DB queries"""
        
        # ✅ OPTIMIZATION 1: Bulk patient creation
        logger.info("      1/5 Processing patients (bulk)...")
        patient_map = self._bulk_create_patients(batch_df, cursor)
        
        # ✅ OPTIMIZATION 2: Pre-select random facilities
        logger.info("      2/5 Creating encounters...")
        facility_ids_sample = random.choices(self.facility_ids, k=len(batch_df))
        
        encounters = []
        encounter_map = {}
        
        for idx, (_, row) in enumerate(batch_df.iterrows()):
            patient_id = patient_map[str(row['Patient ID'])]
            facility_id = facility_ids_sample[idx]
            encounter_id = f"NIH_{row['Image Index'].replace('.png', '')}_ENC"
            
            encounter = (
                encounter_id, patient_id, facility_id,
                row['encounter_date'], row['encounter_datetime'],
                random.choice(['Inpatient', 'Outpatient', 'Emergency']),
                'Direct Admission', 'Home',
                fake.name(), None, 'Scheduled Imaging'
            )
            encounters.append(encounter)
            encounter_map[row['Image Index']] = encounter_id
        
        from psycopg2.extras import execute_batch
        execute_batch(cursor, """
            INSERT INTO encounters (encounter_id, patient_id, facility_id, encounter_date,
                                   encounter_datetime, encounter_type, admission_source,
                                   discharge_disposition, primary_physician, referring_physician, visit_reason)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (encounter_id) DO NOTHING
        """, encounters, page_size=1000)
        self.stats['encounters_created'] += len(encounters)
        
        # Step 3: Procedures
        logger.info("      3/5 Creating procedures...")
        procedures = []
        
        for _, row in batch_df.iterrows():
            encounter_id = encounter_map[row['Image Index']]
            modality = row['modality_mapped']
            
            procedure = (
                encounter_id, row['nih_procedure_code'],
                f"{modality} Chest", 'Diagnostic', 'Chest', 'N/A',
                row['View Position'], modality, fake.name(),
                row['encounter_datetime'], random.randint(5, 15),
                round(random.uniform(0.1, 2.0), 2) if modality in ['X-Ray', 'CT'] else None
            )
            procedures.append(procedure)
        
        execute_batch(cursor, """
            INSERT INTO procedures (encounter_id, procedure_code, procedure_name, procedure_category,
                                   body_part, laterality, view_position, modality, performing_radiologist,
                                   procedure_datetime, procedure_duration_minutes, radiation_dose_mgy)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (encounter_id, procedure_code) DO NOTHING
        """, procedures, page_size=1000)
        self.stats['procedures_created'] += len(procedures)
        
        # Step 4: Diagnoses (using cache)
        logger.info("      4/5 Assigning diagnoses...")
        encounter_diagnoses = []
        
        for _, row in batch_df.iterrows():
            encounter_id = encounter_map[row['Image Index']]
            diagnosis_list = row['diagnosis_list']
            
            for rank, nih_diagnosis in enumerate(diagnosis_list[:3], start=1):
                nih_diagnosis = nih_diagnosis.strip()
                
                if nih_diagnosis in NIH_TO_ICD10_MAPPING:
                    icd_code, _ = NIH_TO_ICD10_MAPPING[nih_diagnosis]
                    
                    # ✅ OPTIMIZATION: Use pre-loaded cache
                    diagnosis_id = self.diagnosis_cache.get(icd_code)
                    
                    if diagnosis_id:
                        encounter_diagnosis = (
                            encounter_id, diagnosis_id, rank, (rank == 1),
                            0.95, fake.name(), row['encounter_datetime'],
                            f"NIH diagnosis: {nih_diagnosis}"
                        )
                        encounter_diagnoses.append(encounter_diagnosis)
        
        if encounter_diagnoses:
            execute_batch(cursor, """
                INSERT INTO encounter_diagnoses (encounter_id, diagnosis_id, diagnosis_rank,
                                                is_primary, diagnosis_confidence, diagnosed_by,
                                                diagnosis_datetime, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (encounter_id, diagnosis_id) DO NOTHING
            """, encounter_diagnoses, page_size=1000)
            self.stats['diagnoses_assigned'] += len(encounter_diagnoses)
        
        # Step 5: Reports
        logger.info("      5/5 Creating reports...")
        reports = []
        
        for _, row in batch_df.iterrows():
            encounter_id = encounter_map[row['Image Index']]
            
            report = (
                encounter_id, row['report_type'], row['report_status'],
                row['report_text'], row['findings'], row['impression'],
                row['recommendations'], fake.name(),
                row['encounter_datetime'],
                row['encounter_datetime'] + timedelta(hours=2),
                False, None
            )
            reports.append(report)
        
        execute_batch(cursor, """
            INSERT INTO reports (encounter_id, report_type, report_status, report_text,
                               findings, impression, recommendations, radiologist_name,
                               dictated_datetime, signed_datetime, critical_finding,
                               critical_notification_datetime)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, reports, page_size=1000)
        self.stats['reports_created'] += len(reports)
    
    def _bulk_create_patients(self, batch_df, cursor):
        """Bulk patient creation with empty list guard"""

        # Get unique patients from batch
        unique_patients = batch_df[["Patient ID", "Patient Age", "Patient Gender"]].drop_duplicates()
        if unique_patients.empty:
            return {}

        # Build list of synthetic emails
        emails = [f"nih_patient_{pid}@external.com" for pid in unique_patients["Patient ID"]]

        # Query existing patients safely using = ANY(%s)
        cursor.execute("""
            SELECT contact_email, patient_id
            FROM patients
            WHERE contact_email = ANY(%s)
        """, (emails,))
        existing_patients = {row[0]: row[1] for row in cursor.fetchall()}

        # Prepare new patients
        new_patients = []
        patient_map = {}

        # Get next patient ID
        cursor.execute("SELECT MAX(patient_id) FROM patients WHERE patient_id LIKE 'PAT%'")
        result = cursor.fetchone()
        last_id = int(result[0].replace("PAT", "")) if result[0] else 5000

        for _, row in unique_patients.iterrows():
            nih_id = str(row["Patient ID"])
            email = f"nih_patient_{nih_id}@external.com"

            if email in existing_patients:
                patient_map[nih_id] = existing_patients[email]
            else:
                last_id += 1
                new_patient_id = f"PAT{str(last_id).zfill(7)}"
                dob = datetime.now() - timedelta(days=int(row["Patient Age"]) * 365)
                gender = "M" if row["Patient Gender"] == "M" else "F" if row["Patient Gender"] == "F" else "Other"

                new_patients.append((
                    new_patient_id, dob.date(), gender, None, "English",
                    email, fake.phone_number()[:20],
                    fake.street_address()[:200], "Kigali", "Kigali Province",
                    fake.postcode()[:10], "Private Insurance",
                    f"INS{nih_id}", True
                ))
                patient_map[nih_id] = new_patient_id
                self.stats["patients_created"] += 1

        # Bulk insert new patients if any
        if new_patients:
            execute_batch(cursor, """
                INSERT INTO patients (patient_id, date_of_birth, gender, ethnicity, primary_language,
                                      contact_email, contact_phone, address_line1, address_city,
                                      address_state, address_zipcode, insurance_provider, insurance_id, is_active)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, new_patients, page_size=1000)

        return patient_map

    
    def run(self, logger):
        """Execute complete ETL pipeline"""
        
        logger.info("\n Step 1: Extract")
        logger.info("-" * 60)
        df = self.extract()
        logger.info(f" Extracted {len(df):,} records from CSV")
        
        logger.info("\n Step 2: Transform")
        logger.info("-" * 60)
        df_transformed = self.transform(df)
        
        if df_transformed is None:
            logger.info("  No new data to load")
            return
        
        logger.info(f" Transformed {len(df_transformed):,} records")
        
        logger.info("\n Step 3: Load")
        logger.info("-" * 60)
        self.load(df_transformed, logger)
        
        logger.info("\n ETL Statistics:")
        logger.info("=" * 60)
        for key, value in self.stats.items():
            logger.info(f"   {key.replace('_', ' ').title():.<40} {value:>10,}")
        logger.info("=" * 60)


def main():
    """Main execution"""
    
    with PipelineLogger("NIH ETL Pipeline - OPTIMIZED") as logger:
        
        logger.info(" Starting ETL Pipeline...")
        logger.info(f"   Mode: Incremental Load")
        logger.info(f"   Batch Size: {BATCH_SIZE}")
        
        pipeline = NIH_ETL_Pipeline(incremental=True)
        pipeline.run(logger)
        
        logger.info("\n ETL Pipeline completed successfully!")


if __name__ == "__main__":
    main()