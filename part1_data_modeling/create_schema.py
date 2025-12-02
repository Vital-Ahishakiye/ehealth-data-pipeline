"""
Schema Creation Script
Executes all DDL files in the correct order to create operational and warehouse schemas.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
load_dotenv(project_root / '.env')

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

def execute_sql_file(cursor, filepath):
    """Execute SQL file"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        cursor.execute(sql_content)
        print(f"  ✅ Executed: {filepath.name}")
        return True
    except Exception as e:
        print(f"  ❌ Failed: {filepath.name}")
        print(f"     Error: {e}")
        return False

def create_operational_schema(cursor):
    """Create operational database schema"""
    print("\n📋 Creating Operational Schema...")
    print("=" * 60)
    
    ddl_dir = Path(__file__).parent / 'sql' / 'ddl'
    ddl_files = sorted(ddl_dir.glob('*.sql'))
    
    success_count = 0
    for ddl_file in ddl_files:
        if execute_sql_file(cursor, ddl_file):
            success_count += 1
    
    print(f"\n✅ Operational schema: {success_count}/{len(ddl_files)} tables created")
    return success_count == len(ddl_files)

def create_warehouse_schema(cursor):
    """Create data warehouse schema"""
    print("\n🏢 Creating Data Warehouse Schema...")
    print("=" * 60)
    
    warehouse_dir = Path(__file__).parent / 'sql' / 'warehouse'
    warehouse_files = sorted(warehouse_dir.glob('*.sql'))
    
    success_count = 0
    for warehouse_file in warehouse_files:
        if execute_sql_file(cursor, warehouse_file):
            success_count += 1
    
    print(f"\n✅ Warehouse schema: {success_count}/{len(warehouse_files)} tables created")
    return success_count == len(warehouse_files)

def verify_schema(cursor):
    """Verify all tables were created"""
    print("\n🔍 Verifying Schema...")
    print("=" * 60)
    
    # Check operational tables
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """)
    
    tables = cursor.fetchall()
    print(f"\n📊 Total tables created: {len(tables)}")
    print("\nOperational Tables:")
    operational_tables = ['facilities', 'patients', 'encounters', 'procedures', 
                          'diagnoses', 'encounter_diagnoses', 'reports']
    for table in operational_tables:
        exists = any(t[0] == table for t in tables)
        status = "✅" if exists else "❌"
        print(f"  {status} {table}")
    
    print("\nWarehouse Tables:")
    warehouse_tables = ['dim_time', 'dim_patient', 'dim_procedure', 'dim_diagnosis', 
                        'fact_encounters', 'bridge_encounter_procedures', 'bridge_encounter_diagnoses']
    for table in warehouse_tables:
        exists = any(t[0] == table for t in tables)
        status = "✅" if exists else "❌"
        print(f"  {status} {table}")

def main():
    """Main execution"""
    print("\n" + "=" * 60)
    print("🚀 eFiche Data Engineer Assessment - Schema Creation")
    print("=" * 60)
    
    conn = None
    try:
        # Connect to database
        print("\n🔌 Connecting to PostgreSQL...")
        conn = get_db_connection()
        cursor = conn.cursor()
        print("✅ Connected successfully!")
        
        # Create operational schema
        if not create_operational_schema(cursor):
            print("\n⚠️ Warning: Some operational tables failed to create")
        
        conn.commit()
        
        # Create warehouse schema
        if not create_warehouse_schema(cursor):
            print("\n⚠️ Warning: Some warehouse tables failed to create")
        
        conn.commit()
        
        # Verify schema
        verify_schema(cursor)
        
        print("\n" + "=" * 60)
        print("✅ Schema creation completed successfully!")
        print("=" * 60)
        
        cursor.close()
        
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        if conn:
            conn.rollback()
        sys.exit(1)
    
    finally:
        if conn:
            conn.close()
            print("\n🔌 Database connection closed.")

if __name__ == "__main__":
    main()