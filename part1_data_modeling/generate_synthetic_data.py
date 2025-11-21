"""
Synthetic Data Generation Script
Populates the operational schema with realistic synthetic healthcare data.
Updated to match actual table structure.
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import random
from dotenv import load_dotenv
import psycopg2
from faker import Faker

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
load_dotenv(project_root / '.env')

# Initialize Faker
fake = Faker()
Faker.seed(42)  # For reproducibility
random.seed(42)

# Configuration from .env
NUM_PATIENTS = int(os.getenv('SYNTHETIC_PATIENTS', 5000))
NUM_ENCOUNTERS = int(os.getenv('SYNTHETIC_ENCOUNTERS', 15000))
NUM_FACILITIES = 500

def get_db_connection():
    """Create database connection"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        sys.exit(1)

def generate_facilities(cursor):
    """Generate synthetic healthcare facilities"""
    print("\n🏥 Generating Facilities...")
    
    # Use only allowed facility types from CHECK constraint
    facility_types = ['Hospital', 'Clinic', 'Imaging Center']
    cities = ['Kigali', 'Butare', 'Gisenyi', 'Ruhengeri', 'Byumba', 'Cyangugu', 'Kibungo']
    states = ['Kigali Province', 'Eastern Province', 'Southern Province', 'Western Province', 'Northern Province']
    
    facilities = []
    for i in range(NUM_FACILITIES):
        facility_id = f"FAC{str(i+1).zfill(6)}"
        facility = (
            facility_id,
            f"{fake.company()} {random.choice(facility_types)}",
            random.choice(facility_types),
            fake.street_address()[:200],
            random.choice(cities),
            random.choice(states),
            fake.postcode()[:10],
            fake.phone_number()[:20],
            random.randint(50, 500) if random.random() > 0.3 else None,
            random.choice([True, False]),
            random.choice([True, False])
        )
        facilities.append(facility)
    
    cursor.executemany("""
        INSERT INTO facilities (facility_id, facility_name, facility_type, 
                               address_line1, address_city, address_state, address_zipcode,
                               phone, total_beds, has_emergency, has_icu)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, facilities)
    
    print(f"  ✅ Generated {NUM_FACILITIES} facilities")
    return NUM_FACILITIES

def generate_diagnoses(cursor):
    """Generate comprehensive diagnosis catalog"""
    print("\n🩺 Generating Diagnosis Catalog...")
    
    diagnoses_data = [
        # Respiratory
        ('DIAG001', 'J18.9', 'Pneumonia', 'Respiratory', 'Moderate', True, True, 'Acute inflammation of the lungs'),
        ('DIAG002', 'J12.9', 'Viral Pneumonia', 'Respiratory', 'Moderate', False, True, 'Pneumonia caused by viral infection'),
        ('DIAG003', 'J44.0', 'COPD with Acute Lower Respiratory Infection', 'Respiratory', 'Severe', True, True, 'Chronic obstructive pulmonary disease'),
        ('DIAG004', 'J44.1', 'COPD with Acute Exacerbation', 'Respiratory', 'Severe', True, True, 'COPD flare-up'),
        ('DIAG005', 'J81.0', 'Acute Pulmonary Edema', 'Respiratory', 'Severe', False, True, 'Fluid accumulation in lungs'),
        ('DIAG006', 'J93.0', 'Spontaneous Tension Pneumothorax', 'Respiratory', 'Severe', False, True, 'Collapsed lung'),
        ('DIAG007', 'J98.4', 'Other Disorders of Lung', 'Respiratory', 'Mild', False, False, 'Various lung abnormalities'),
        ('DIAG008', 'J20.9', 'Acute Bronchitis', 'Respiratory', 'Mild', False, False, 'Inflammation of bronchial tubes'),
        ('DIAG009', 'J45.9', 'Asthma', 'Respiratory', 'Moderate', True, False, 'Chronic inflammatory airway disease'),
        ('DIAG010', 'J84.9', 'Interstitial Lung Disease', 'Respiratory', 'Severe', True, True, 'Lung tissue scarring'),
        
        # Cardiovascular
        ('DIAG011', 'I50.9', 'Heart Failure', 'Cardiovascular', 'Severe', True, True, 'Heart unable to pump adequately'),
        ('DIAG012', 'I25.10', 'Coronary Artery Disease', 'Cardiovascular', 'Severe', True, True, 'Narrowing of coronary arteries'),
        ('DIAG013', 'I21.9', 'Acute Myocardial Infarction', 'Cardiovascular', 'Severe', False, True, 'Heart attack'),
        ('DIAG014', 'I48.91', 'Atrial Fibrillation', 'Cardiovascular', 'Moderate', True, False, 'Irregular heart rhythm'),
        ('DIAG015', 'I11.0', 'Hypertensive Heart Disease', 'Cardiovascular', 'Moderate', True, False, 'Heart disease from high blood pressure'),
        ('DIAG016', 'I26.99', 'Pulmonary Embolism', 'Cardiovascular', 'Severe', False, True, 'Blood clot in lungs'),
        ('DIAG017', 'I10', 'Essential Hypertension', 'Cardiovascular', 'Mild', True, False, 'High blood pressure'),
        
        # Infectious
        ('DIAG018', 'A41.9', 'Sepsis', 'Infectious', 'Severe', False, True, 'Life-threatening infection response'),
        ('DIAG019', 'B99.9', 'Infectious Disease', 'Infectious', 'Moderate', False, True, 'General infectious condition'),
        ('DIAG020', 'J22', 'Viral Upper Respiratory Infection', 'Infectious', 'Mild', False, False, 'Common cold/flu'),
        
        # Trauma/Injury
        ('DIAG021', 'S22.9', 'Rib Fracture', 'Trauma', 'Moderate', False, False, 'Broken rib'),
        ('DIAG022', 'S32.9', 'Spinal Fracture', 'Trauma', 'Severe', False, True, 'Vertebral fracture'),
        ('DIAG023', 'S42.9', 'Shoulder Fracture', 'Trauma', 'Moderate', False, False, 'Broken shoulder bone'),
        ('DIAG024', 'S72.9', 'Femur Fracture', 'Trauma', 'Severe', False, True, 'Broken thigh bone'),
        ('DIAG025', 'S06.9', 'Traumatic Brain Injury', 'Trauma', 'Severe', False, True, 'Head trauma'),
        
        # Oncology
        ('DIAG026', 'C34.90', 'Lung Cancer', 'Oncology', 'Severe', True, True, 'Malignant lung neoplasm'),
        ('DIAG027', 'C50.9', 'Breast Cancer', 'Oncology', 'Severe', True, True, 'Malignant breast neoplasm'),
        ('DIAG028', 'D49.2', 'Neoplasm of Uncertain Behavior', 'Oncology', 'Moderate', False, False, 'Tumor requiring further evaluation'),
        
        # Other Common
        ('DIAG029', 'R91.8', 'Abnormal Lung Finding', 'Radiology', 'Mild', False, False, 'Non-specific lung abnormality'),
        ('DIAG030', 'R07.9', 'Chest Pain', 'Symptom', 'Mild', False, False, 'Unspecified chest pain'),
        ('DIAG031', 'R05', 'Cough', 'Symptom', 'Mild', False, False, 'Persistent cough'),
        ('DIAG032', 'R06.02', 'Shortness of Breath', 'Symptom', 'Moderate', False, False, 'Dyspnea'),
        ('DIAG033', 'J96.90', 'Respiratory Failure', 'Respiratory', 'Severe', False, True, 'Inadequate gas exchange'),
        ('DIAG034', 'J47.9', 'Bronchiectasis', 'Respiratory', 'Moderate', True, True, 'Permanent airway dilation'),
        ('DIAG035', 'J43.9', 'Emphysema', 'Respiratory', 'Severe', True, True, 'Lung tissue damage'),
        ('DIAG036', 'J85.2', 'Lung Abscess', 'Infectious', 'Severe', False, True, 'Pus-filled lung cavity'),
        ('DIAG037', 'J94.8', 'Pleural Effusion', 'Respiratory', 'Moderate', False, True, 'Fluid around lungs'),
        ('DIAG038', 'M79.3', 'Chest Wall Pain', 'Musculoskeletal', 'Mild', False, False, 'Musculoskeletal chest pain'),
        ('DIAG039', 'E11.9', 'Type 2 Diabetes', 'Endocrine', 'Moderate', True, False, 'Diabetes mellitus'),
        ('DIAG040', 'E66.9', 'Obesity', 'Endocrine', 'Mild', True, False, 'Excessive body weight'),
        ('DIAG041', 'N18.9', 'Chronic Kidney Disease', 'Renal', 'Severe', True, True, 'Progressive kidney failure'),
        ('DIAG042', 'K21.9', 'GERD', 'Gastrointestinal', 'Mild', True, False, 'Gastroesophageal reflux disease'),
        ('DIAG043', 'F41.9', 'Anxiety Disorder', 'Mental Health', 'Mild', True, False, 'Excessive worry and fear'),
        ('DIAG044', 'G47.33', 'Sleep Apnea', 'Neurological', 'Moderate', True, False, 'Breathing interruptions during sleep'),
        ('DIAG045', 'Z87.891', 'History of Tobacco Use', 'Social History', 'Mild', False, False, 'Former smoker'),
        ('DIAG046', 'Z20.828', 'COVID-19 Contact', 'Infectious', 'Mild', False, True, 'Exposure to coronavirus'),
        ('DIAG047', 'U07.1', 'COVID-19', 'Infectious', 'Moderate', False, True, 'Coronavirus disease 2019'),
        ('DIAG048', 'J80', 'ARDS', 'Respiratory', 'Severe', False, True, 'Acute respiratory distress syndrome'),
        ('DIAG049', 'T78.2', 'Anaphylaxis', 'Allergy', 'Severe', False, True, 'Severe allergic reaction'),
        ('DIAG050', 'J18.1', 'Lobar Pneumonia', 'Respiratory', 'Moderate', False, True, 'Pneumonia affecting lung lobe'),
    ]
    
    cursor.executemany("""
        INSERT INTO diagnoses (diagnosis_id, diagnosis_code, diagnosis_name, 
                              diagnosis_category, severity, is_chronic, is_reportable, description)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, diagnoses_data)
    
    print(f"  ✅ Generated {len(diagnoses_data)} diagnoses")
    return len(diagnoses_data)

def generate_patients(cursor):
    """Generate synthetic patients"""
    print("\n👤 Generating Patients...")
    
    genders = ['M', 'F', 'Other']
    ethnicities = ['Hutu', 'Tutsi', 'Twa', 'Other']
    languages = ['Kinyarwanda', 'English', 'French', 'Swahili']
    cities = ['Kigali', 'Butare', 'Gisenyi', 'Ruhengeri', 'Byumba', 'Cyangugu', 'Kibungo']
    states = ['Kigali Province', 'Eastern Province', 'Southern Province', 'Western Province', 'Northern Province']
    insurances = ['RAMA', 'MMI', 'Private Insurance', 'Self-Pay', 'Community Health']
    
    patients = []
    for i in range(NUM_PATIENTS):
        age = random.randint(0, 95)
        dob = datetime.now() - timedelta(days=age*365 + random.randint(0, 364))
        
        patient = (
            f"PAT{str(i+1).zfill(7)}",  # patient_id
            dob.date(),  # date_of_birth
            random.choice(genders),  # gender
            random.choice(ethnicities),  # ethnicity
            random.choice(languages),  # primary_language
            fake.email(),  # contact_email
            fake.phone_number()[:20],  # contact_phone
            fake.street_address()[:200],  # address_line1
            random.choice(cities),  # address_city
            random.choice(states),  # address_state
            fake.postcode()[:10],  # address_zipcode
            random.choice(insurances),  # insurance_provider
            f"INS{random.randint(100000, 999999)}",  # insurance_id
            True  # is_active
        )
        patients.append(patient)
    
    cursor.executemany("""
        INSERT INTO patients (patient_id, date_of_birth, gender, ethnicity, primary_language,
                             contact_email, contact_phone, address_line1, address_city, 
                             address_state, address_zipcode, insurance_provider, insurance_id, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, patients)
    
    print(f"  ✅ Generated {NUM_PATIENTS} patients")
    return NUM_PATIENTS

def generate_encounters_and_procedures(cursor):
    """Generate encounters with associated procedures"""
    print("\n🏥 Generating Encounters and Procedures...")
    
    # Fetch patient and facility IDs
    cursor.execute("SELECT patient_id FROM patients")
    patient_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT facility_id FROM facilities")
    facility_ids = [row[0] for row in cursor.fetchall()]
    
    encounter_types = ['Outpatient', 'Emergency', 'Inpatient', 'Urgent Care', 'Observation']
    admission_sources = ['Emergency Department', 'Physician Referral', 'Transfer', 'Direct Admission', 'Walk-in']
    discharge_dispositions = ['Home', 'Transferred', 'Admitted', 'Left AMA', 'Deceased']
    visit_reasons = ['Chest Pain', 'Shortness of Breath', 'Cough', 'Fever', 'Trauma', 'Scheduled Imaging', 'Follow-up']
    
    modalities = ['X-Ray', 'CT', 'MRI', 'Ultrasound', 'Fluoroscopy', 'Mammography']
    view_positions = ['AP', 'PA', 'Lateral', 'Oblique', 'Axial', 'Sagittal', 'Coronal']
    body_parts = ['Chest', 'Abdomen', 'Head', 'Spine', 'Pelvis', 'Extremities', 'Neck']
    lateralities = ['Left', 'Right', 'Bilateral', 'N/A']
    procedure_categories = ['Diagnostic', 'Interventional', 'Screening', 'Follow-up']
    
    encounters = []
    procedures = []
    
    start_date = datetime.now() - timedelta(days=730)  # 2 years of data
    
    for i in range(NUM_ENCOUNTERS):
        patient_id = random.choice(patient_ids)
        facility_id = random.choice(facility_ids)
        encounter_datetime = start_date + timedelta(days=random.randint(0, 730), hours=random.randint(0, 23))
        
        encounter = (
            f"ENC{str(i+1).zfill(8)}",  # encounter_id
            patient_id,
            facility_id,
            encounter_datetime.date(),  # encounter_date
            encounter_datetime,  # encounter_datetime
            random.choice(encounter_types),
            random.choice(admission_sources),
            random.choice(discharge_dispositions),
            fake.name(),  # primary_physician
            fake.name() if random.random() > 0.5 else None,  # referring_physician
            random.choice(visit_reasons)
        )
        encounters.append(encounter)
    
    # Insert encounters first
    cursor.executemany("""
        INSERT INTO encounters (encounter_id, patient_id, facility_id, encounter_date, 
                               encounter_datetime, encounter_type, admission_source,
                               discharge_disposition, primary_physician, referring_physician, visit_reason)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, encounters)
    
    print(f"  ✅ Generated {NUM_ENCOUNTERS} encounters")
    
    for encounter_id, _, _, enc_date, enc_datetime, *_ in encounters:
        num_procedures = random.randint(1, 3)
    # pick unique codes for this encounter
    codes_for_encounter = random.sample(range(70000, 79999), num_procedures)

    for code in codes_for_encounter:
        modality = random.choice(modalities)
        body_part = random.choice(body_parts)
        proc_datetime = enc_datetime + timedelta(minutes=random.randint(0, 120))

        procedure = (
            encounter_id,
            f"CPT{code}",  # unique per encounter
            f"{modality} {body_part}",
            random.choice(procedure_categories),
            body_part,
            random.choice(lateralities),
            random.choice(view_positions),
            modality,
            fake.name(),
            proc_datetime,
            random.randint(5, 60),
            round(random.uniform(0.1, 10.0), 2) if modality in ['X-Ray', 'CT', 'Fluoroscopy'] else None
        )
        procedures.append(procedure)

    
    cursor.executemany("""
        INSERT INTO procedures (encounter_id, procedure_code, procedure_name, procedure_category,
                               body_part, laterality, view_position, modality, performing_radiologist,
                               procedure_datetime, procedure_duration_minutes, radiation_dose_mgy)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, procedures)
    
    print(f"  ✅ Generated {len(procedures)} procedures")
    return len(procedures)

def generate_encounter_diagnoses(cursor):
    """Assign diagnoses to encounters"""
    print("\n🩺 Assigning Diagnoses to Encounters...")
    
    cursor.execute("SELECT encounter_id FROM encounters")
    encounter_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT diagnosis_id FROM diagnoses")
    diagnosis_ids = [row[0] for row in cursor.fetchall()]
    
    encounter_diagnoses = []
    
    # Each encounter gets 1-3 diagnoses
    for encounter_id in encounter_ids:
        num_diagnoses = random.randint(1, 3)
        selected_diagnoses = random.sample(diagnosis_ids, min(num_diagnoses, len(diagnosis_ids)))
        
        for rank, diag_id in enumerate(selected_diagnoses, start=1):
            is_primary = (rank == 1)
            diag_datetime = datetime.now() - timedelta(days=random.randint(0, 730))
            
            encounter_diagnosis = (
                encounter_id,
                diag_id,
                rank,  # diagnosis_rank
                is_primary,
                round(random.uniform(0.7, 1.0), 2),  # diagnosis_confidence
                fake.name(),  # diagnosed_by
                diag_datetime,
                f"Clinical notes for {diag_id}" if random.random() > 0.5 else None  # notes
            )
            encounter_diagnoses.append(encounter_diagnosis)
    
    cursor.executemany("""
        INSERT INTO encounter_diagnoses (encounter_id, diagnosis_id, diagnosis_rank, 
                                        is_primary, diagnosis_confidence, diagnosed_by, 
                                        diagnosis_datetime, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, encounter_diagnoses)
    
    print(f"  ✅ Generated {len(encounter_diagnoses)} diagnosis assignments")
    return len(encounter_diagnoses)

def generate_reports(cursor):
    """Generate synthetic radiology reports"""
    print("\n📄 Generating Radiology Reports...")
    
    # Fetch procedures with their associated encounter and diagnosis data
    cursor.execute("""
        SELECT p.procedure_id, p.encounter_id, p.modality, p.body_part,
               d.diagnosis_name, d.severity
        FROM procedures p
        JOIN encounters e ON p.encounter_id = e.encounter_id
        LEFT JOIN encounter_diagnoses ed ON e.encounter_id = ed.encounter_id AND ed.is_primary = true
        LEFT JOIN diagnoses d ON ed.diagnosis_id = d.diagnosis_id
    """)
    
    procedure_data = cursor.fetchall()
    
    reports = []
    report_types = ['Radiology Report', 'Diagnostic Report', 'Preliminary Report']
    report_statuses = ['Draft', 'Preliminary', 'Final', 'Amended']
    
    report_templates = {
        'clinical_history': "Clinical History: {diagnosis}. Patient presents with {symptom}.",
        'findings': "Technique: {modality} of the {body_part} was performed. Findings: {finding}. {additional_detail}",
        'impression': "Impression: {impression}. {recommendation}",
    }
    
    findings_by_severity = {
        'Mild': ['mild abnormality', 'minimal changes', 'subtle findings', 'slight irregularity', 'no acute abnormality'],
        'Moderate': ['moderate abnormality', 'concerning findings', 'significant changes', 'notable abnormality', 'borderline findings'],
        'Severe': ['severe abnormality', 'critical findings', 'extensive changes', 'marked abnormality', 'urgent findings']
    }
    
    impressions_by_severity = {
        'Mild': ['Within normal limits', 'No acute disease process', 'Minimal abnormality noted', 'Clinical correlation advised'],
        'Moderate': ['Further evaluation recommended', 'Suggest additional imaging', 'Correlate with clinical findings', 'Follow-up imaging recommended'],
        'Severe': ['Urgent attention required', 'Critical findings', 'Immediate clinical correlation necessary', 'Immediate intervention advised']
    }
    
    recommendations_by_severity = {
        'Mild': ['Routine follow-up', 'No immediate action needed', 'Monitor clinically'],
        'Moderate': ['Follow-up in 3-6 months', 'Consider additional studies', 'Clinical correlation recommended'],
        'Severe': ['Immediate clinical action required', 'Urgent consultation recommended', 'Critical result communicated to ordering physician']
    }
    
    for proc_id, enc_id, modality, body_part, diagnosis, severity in procedure_data:
        severity = severity or 'Moderate'
        diagnosis = diagnosis or 'Routine examination'
        
        finding = random.choice(findings_by_severity.get(severity, findings_by_severity['Moderate']))
        impression = random.choice(impressions_by_severity.get(severity, impressions_by_severity['Moderate']))
        recommendation = random.choice(recommendations_by_severity.get(severity, recommendations_by_severity['Moderate']))
        
        clinical_history = report_templates['clinical_history'].format(
            diagnosis=diagnosis,
            symptom=random.choice(['chest pain', 'shortness of breath', 'cough', 'trauma'])
        )
        
        findings_text = report_templates['findings'].format(
            modality=modality,
            body_part=body_part,
            finding=finding,
            additional_detail=f"No evidence of acute fracture or dislocation." if 'Trauma' in str(diagnosis) else "Lung fields are clear."
        )
        
        impression_text = report_templates['impression'].format(
            impression=impression,
            recommendation=recommendation
        )
        
        full_report = f"{clinical_history}\n\n{findings_text}\n\n{impression_text}"
        
        is_critical = (severity == 'Severe' and random.random() > 0.5)
        dictated_dt = datetime.now() - timedelta(days=random.randint(0, 730), hours=random.randint(0, 24))
        signed_dt = dictated_dt + timedelta(hours=random.randint(1, 48)) if random.random() > 0.2 else None
        critical_dt = dictated_dt + timedelta(minutes=random.randint(5, 30)) if is_critical else None
        
        report = (
            enc_id,  # encounter_id
            random.choice(report_types),
            random.choice(report_statuses),
            full_report,  # report_text
            findings_text,  # findings
            impression_text,  # impression
            recommendation,  # recommendations
            fake.name(),  # radiologist_name
            dictated_dt,
            signed_dt,
            is_critical,
            critical_dt
        )
        reports.append(report)
    
    cursor.executemany("""
        INSERT INTO reports (encounter_id, report_type, report_status, report_text,
                           findings, impression, recommendations, radiologist_name,
                           dictated_datetime, signed_datetime, critical_finding, 
                           critical_notification_datetime)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, reports)
    
    print(f"  ✅ Generated {len(reports)} radiology reports")
    return len(reports)

def print_summary(cursor):
    """Print data generation summary"""
    print("\n" + "=" * 60)
    print("📊 DATA GENERATION SUMMARY")
    print("=" * 60)
    
    tables = [
        ('facilities', 'Facilities'),
        ('patients', 'Patients'),
        ('diagnoses', 'Diagnoses'),
        ('encounters', 'Encounters'),
        ('procedures', 'Procedures'),
        ('encounter_diagnoses', 'Diagnosis Assignments'),
        ('reports', 'Reports')
    ]
    
    for table_name, display_name in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"  {display_name:.<30} {count:>10,}")
    
    print("=" * 60)

def main():
    """Main execution"""
    print("\n" + "=" * 60)
    print("🎲 eHealth - Synthetic Data Generation")
    print("=" * 60)
    
    conn = None
    try:
        # Connect to database
        print("\n🔌 Connecting to PostgreSQL...")
        conn = get_db_connection()
        conn.autocommit = False
        cursor = conn.cursor()
        print("✅ Connected successfully!")
        
        # Generate data in order (respecting foreign keys)
        generate_facilities(cursor)
        conn.commit()
        
        generate_diagnoses(cursor)
        conn.commit()
        
        generate_patients(cursor)
        conn.commit()
        
        generate_encounters_and_procedures(cursor)
        conn.commit()
        
        generate_encounter_diagnoses(cursor)
        conn.commit()
        
        generate_reports(cursor)
        conn.commit()
        
        # Print summary
        print_summary(cursor)
        
        print("\n✅ Synthetic data generation completed successfully!")
        
        cursor.close()
        
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        if conn:
            conn.rollback()
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        if conn:
            conn.close()
            print("\n🔌 Database connection closed.")

if __name__ == "__main__":
    main()