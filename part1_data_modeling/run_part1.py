"""
Part 1 Complete Runner
Executes schema creation + synthetic data generation in one command
"""

import subprocess
import sys
from pathlib import Path

def run_command(script_path, description):
    """Run a Python script and handle errors"""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            check=True,
            capture_output=False,
            text=True
        )
        print(f"✅ {description} completed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed!")
        print(f"Error: {e}")
        return False

def main():
    """Main execution"""
    print("\n" + "="*60)
    print("🚀 eHealth Part 1 - Complete Setup")
    print("   (Schema Creation + Synthetic Data Generation)")
    print("="*60)
    
    project_root = Path(__file__).parent.parent
    part1_dir = Path(__file__).parent
    
    # Step 1: Create Schema
    schema_script = part1_dir / "create_schema.py"
    if not run_command(schema_script, "Schema Creation"):
        print("\n⚠️ Schema creation failed. Stopping execution.")
        sys.exit(1)
    
    # Step 2: Generate Synthetic Data
    data_script = part1_dir / "generate_synthetic_data.py"
    if not run_command(data_script, "Synthetic Data Generation"):
        print("\n⚠️ Data generation failed. Stopping execution.")
        sys.exit(1)
    
    print("\n" + "="*60)
    print("✅ Part 1 Setup Completed Successfully!")
    print("="*60)
    print("\n📋 Next Steps:")
    print("   1. Review data with: psql -d ehealth_db -f part1_data_modeling/test_schema.sql")
    print("   2. Proceed to Part 2: Data Pipeline")
    print("="*60)

if __name__ == "__main__":
    main()