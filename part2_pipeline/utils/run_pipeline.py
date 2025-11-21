"""
Pipeline Orchestrator - Simulates Incremental Loads
Demonstrates how the pipeline handles periodic data ingestion
"""

import sys
from pathlib import Path
import time
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))
from etl_pipeline import NIH_ETL_Pipeline
from utils.db_helper import DatabaseHelper
from utils.logger import PipelineLogger


def simulate_incremental_loads(num_runs=3, delay_seconds=5):
    """
    Simulate multiple pipeline runs to demonstrate incremental loading
    
    Args:
        num_runs: Number of pipeline executions to simulate
        delay_seconds: Seconds between runs
    """
    
    print("\n" + "=" * 60)
    print("🔄 INCREMENTAL LOAD SIMULATION")
    print("=" * 60)
    print(f"\nConfiguration:")
    print(f"   Number of runs: {num_runs}")
    print(f"   Delay between runs: {delay_seconds}s")
    print(f"   Mode: Incremental (skip duplicates)")
    print("\n" + "=" * 60)
    
    db = DatabaseHelper()
    results = []
    
    for run_num in range(1, num_runs + 1):
        print(f"\n🚀 RUN #{run_num}/{num_runs}")
        print("=" * 60)
        print(f"   Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Get stats before run
        stats_before = db.get_pipeline_stats()
        
        # Execute pipeline
        with PipelineLogger(f"Run_{run_num}") as logger:
            pipeline = NIH_ETL_Pipeline(incremental=True)
            pipeline.run(logger)
            
            # Collect run stats
            run_stats = {
                'run_number': run_num,
                'timestamp': datetime.now(),
                'processed': pipeline.stats['records_processed'],
                'skipped': pipeline.stats['records_skipped'],
                'patients_created': pipeline.stats['patients_created'],
                'encounters_created': pipeline.stats['encounters_created']
            }
            results.append(run_stats)
        
        # Get stats after run
        stats_after = db.get_pipeline_stats()
        
        print(f"\n📊 Run #{run_num} Summary:")
        print("-" * 60)
        print(f"   Records Processed: {run_stats['processed']:,}")
        print(f"   Records Skipped (duplicates): {run_stats['skipped']:,}")
        print(f"   New Patients: {run_stats['patients_created']:,}")
        print(f"   New Encounters: {run_stats['encounters_created']:,}")
        
        # Database state
        print(f"\n📂 Database State:")
        for source, count in stats_after.items():
            print(f"   {source}: {count:,}")
        
        # Wait before next run
        if run_num < num_runs:
            print(f"\n⏳ Waiting {delay_seconds}s before next run...")
            time.sleep(delay_seconds)
    
    # Final summary
    print("\n" + "=" * 60)
    print("📊 SIMULATION SUMMARY")
    print("=" * 60)
    
    print(f"\nTotal Runs: {len(results)}")
    print(f"\nRun-by-Run Breakdown:")
    print("-" * 60)
    print(f"{'Run':<6} {'Processed':<12} {'Skipped':<12} {'New Patients':<15} {'New Encounters':<15}")
    print("-" * 60)
    
    for r in results:
        print(f"{r['run_number']:<6} {r['processed']:<12,} {r['skipped']:<12,} "
              f"{r['patients_created']:<15,} {r['encounters_created']:<15,}")
    
    print("-" * 60)
    
    # Totals
    total_processed = sum(r['processed'] for r in results)
    total_skipped = sum(r['skipped'] for r in results)
    
    print(f"{'TOTAL':<6} {total_processed:<12,} {total_skipped:<12,}")
    
    print("\n✅ Incremental Load Simulation Complete!")
    print("\n📋 Key Observations:")
    print("   1. First run processes all records")
    print("   2. Subsequent runs skip duplicates (incremental)")
    print("   3. ON CONFLICT DO NOTHING prevents duplicates")
    print("   4. Pipeline is idempotent (safe to re-run)")


def run_single_pipeline():
    """Run pipeline once (for manual testing)"""
    
    print("\n" + "=" * 60)
    print("🚀 SINGLE PIPELINE EXECUTION")
    print("=" * 60)
    
    db = DatabaseHelper()
    
    # Show current state
    stats = db.get_pipeline_stats()
    print(f"\n📂 Current Database State:")
    for source, count in stats.items():
        print(f"   {source}: {count:,}")
    
    # Run pipeline
    with PipelineLogger("Manual_Run") as logger:
        pipeline = NIH_ETL_Pipeline(incremental=True)
        pipeline.run(logger)
    
    # Show new state
    stats_after = db.get_pipeline_stats()
    print(f"\n📂 Updated Database State:")
    for source, count in stats_after.items():
        print(f"   {source}: {count:,}")
    
    print("\n✅ Pipeline execution complete!")


def main():
    """Main execution"""
    
    import argparse
    parser = argparse.ArgumentParser(description='Run NIH ETL Pipeline')
    parser.add_argument('--mode', choices=['single', 'simulate'], default='single',
                       help='Execution mode: single run or simulation')
    parser.add_argument('--runs', type=int, default=3,
                       help='Number of runs for simulation mode')
    parser.add_argument('--delay', type=int, default=5,
                       help='Delay between runs (seconds) for simulation mode')
    
    args = parser.parse_args()
    
    try:
        if args.mode == 'simulate':
            simulate_incremental_loads(num_runs=args.runs, delay_seconds=args.delay)
        else:
            run_single_pipeline()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()