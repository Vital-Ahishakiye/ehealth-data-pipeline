"""
Configuration file for Part 2: Data Pipeline
Loads environment variables and defines constants
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
project_root = Path(__file__).parent.parent
load_dotenv(project_root / '.env')

# Database Configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'ehealth_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '')
}

# NIH Dataset Configuration
NIH_DATASET_NAME = "alkzar90/NIH-Chest-X-ray-dataset"
NIH_DATASET_SIZE = int(os.getenv('NIH_DATASET_SIZE', 10000))  # Number of records to process
NIH_SUBSET_NAME = "nih_subset_10k.csv"

# File Paths
DATA_DIR = Path(__file__).parent / 'data'
DATA_DIR.mkdir(exist_ok=True)

LOGS_DIR = Path(__file__).parent / 'logs'
LOGS_DIR.mkdir(exist_ok=True)

# NIH to ICD-10 Diagnosis Mapping
NIH_TO_ICD10_MAPPING = {
    'Atelectasis': ('J98.11', 'Atelectasis'),
    'Cardiomegaly': ('I51.7', 'Cardiomegaly'),
    'Effusion': ('J94.8', 'Pleural Effusion'),
    'Infiltration': ('J98.4', 'Other Disorders of Lung'),
    'Mass': ('D49.2', 'Neoplasm of Uncertain Behavior'),
    'Nodule': ('R91.8', 'Abnormal Lung Finding'),
    'Pneumonia': ('J18.9', 'Pneumonia'),
    'Pneumothorax': ('J93.0', 'Spontaneous Tension Pneumothorax'),
    'Consolidation': ('J18.1', 'Lobar Pneumonia'),
    'Edema': ('J81.0', 'Acute Pulmonary Edema'),
    'Emphysema': ('J43.9', 'Emphysema'),
    'Fibrosis': ('J84.9', 'Interstitial Lung Disease'),
    'Pleural_Thickening': ('J94.8', 'Pleural Effusion'),
    'Hernia': ('K44.9', 'Diaphragmatic Hernia'),
    'No Finding': ('R91.8', 'Abnormal Lung Finding')  # Default for normal studies
}

# ✅ FIXED: Modality Mapping to match Part 1 CHECK constraint
# Part 1 schema allows: 'X-Ray','CT','MRI','Ultrasound','Fluoroscopy','Mammography'
MODALITY_MAPPING = {
    'DX': 'X-Ray',       # ✅ Digital Radiography → X-Ray
    'CR': 'X-Ray',       # ✅ Computed Radiography → X-Ray
    'PA': 'X-Ray',       # ✅ Posteroanterior view → X-Ray
    'AP': 'X-Ray',       # ✅ Anteroposterior view → X-Ray
    'CT': 'CT',          # ✅ CT Scan
    'MR': 'MRI',         # ✅ MRI
    'US': 'Ultrasound',  # ✅ Ultrasound
}

# ETL Configuration
BATCH_SIZE = 2000  # Records per batch insert
INCREMENTAL_LOAD_BATCH = 1000  # Simulate new data in batches

# Logging Configuration
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'