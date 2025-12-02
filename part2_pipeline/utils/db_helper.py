"""
Database Helper Utilities
Provides connection management and common database operations
"""

import psycopg2
from psycopg2.extras import execute_batch
from contextlib import contextmanager
from efiche_data_engineer_assessment.part2_pipeline.config import DB_CONFIG

class DatabaseHelper:
    """Helper class for database operations"""
    
    def __init__(self):
        self.config = DB_CONFIG
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = psycopg2.connect(**self.config)
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                conn.close()
    
    def execute_query(self, query, params=None, fetch=False):
        """Execute a single query"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            
            if fetch:
                result = cursor.fetchall()
                cursor.close()
                return result
            
            conn.commit()
            cursor.close()
    
    def execute_many(self, query, data, batch_size=500):
        """Execute batch insert with execute_batch for performance"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            execute_batch(cursor, query, data, page_size=batch_size)
            conn.commit()
            cursor.close()
    
    def get_existing_image_indices(self):
        """Get all NIH image indices already loaded"""
        query = """
            SELECT DISTINCT procedure_code 
            FROM procedures 
            WHERE procedure_code LIKE 'NIH_%'
        """
        results = self.execute_query(query, fetch=True)
        return {row[0] for row in results}
    
    def get_patient_by_external_id(self, external_id):
        """Find patient by external ID (NIH Patient ID)"""
        query = """
            SELECT patient_id 
            FROM patients 
            WHERE contact_email = %s
            LIMIT 1
        """
        # Store NIH patient ID in email field for tracking
        result = self.execute_query(query, (f"nih_patient_{external_id}@external.com",), fetch=True)
        return result[0][0] if result else None
    
    def get_facility_by_type(self, facility_type='Hospital'):
        """Get a random facility of specified type"""
        query = """
            SELECT facility_id 
            FROM facilities 
            WHERE facility_type = %s 
            ORDER BY RANDOM() 
            LIMIT 1
        """
        result = self.execute_query(query, (facility_type,), fetch=True)
        return result[0][0] if result else None
    
    def get_diagnosis_by_code(self, diagnosis_code):
        """Get diagnosis_id by ICD-10 code"""
        query = """
            SELECT diagnosis_id 
            FROM diagnoses 
            WHERE diagnosis_code = %s
            LIMIT 1
        """
        result = self.execute_query(query, (diagnosis_code,), fetch=True)
        return result[0][0] if result else None
    
    def get_or_create_patient(self, nih_patient_id, age, gender):
        """Get existing patient or create new one"""
        # Check if patient exists
        patient_id = self.get_patient_by_external_id(nih_patient_id)
        
        if patient_id:
            return patient_id
        
        # Create new patient
        from datetime import datetime, timedelta
        from faker import Faker
        fake = Faker()
        
        # Generate patient_id
        query_max_id = "SELECT MAX(patient_id) FROM patients WHERE patient_id LIKE 'PAT%'"
        result = self.execute_query(query_max_id, fetch=True)
        
        if result[0][0]:
            last_id = int(result[0][0].replace('PAT', ''))
            new_id = f"PAT{str(last_id + 1).zfill(7)}"
        else:
            new_id = "PAT0010001"  # Start after synthetic data
        
        # Calculate date of birth from age
        dob = datetime.now() - timedelta(days=age * 365)
        
        # Insert new patient
        insert_query = """
            INSERT INTO patients (
                patient_id, date_of_birth, gender, ethnicity, primary_language,
                contact_email, contact_phone, address_line1, address_city, 
                address_state, address_zipcode, insurance_provider, insurance_id, is_active
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Map gender
        gender_map = {'M': 'M', 'F': 'F'}
        mapped_gender = gender_map.get(gender, 'Other')
        
        data = (
            new_id,
            dob.date(),
            mapped_gender,
            'Other',
            'English',
            f"nih_patient_{nih_patient_id}@external.com",  # Store NIH ID
            fake.phone_number()[:20],
            fake.street_address()[:200],
            'Kigali',
            'Kigali Province',
            fake.postcode()[:10],
            'Private Insurance',
            f"INS{nih_patient_id}",
            True
        )
        
        self.execute_query(insert_query, data)
        return new_id
    
    def get_pipeline_stats(self):
        """Get pipeline execution statistics"""
        stats = {}
        
        # Total records by source
        query = """
            SELECT 
                CASE 
                    WHEN procedure_code LIKE 'NIH_%' THEN 'NIH Dataset'
                    ELSE 'Synthetic Data'
                END as source,
                COUNT(*) as count
            FROM procedures
            GROUP BY source
        """
        results = self.execute_query(query, fetch=True)
        for row in results:
            stats[row[0]] = row[1]
        
        return stats