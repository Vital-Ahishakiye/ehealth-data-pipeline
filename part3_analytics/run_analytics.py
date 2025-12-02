"""
Analytics Orchestrator
Runs Part 3 analytics: warehouse population and SQL queries
"""

import sys, io
from pathlib import Path
import psycopg2
import pandas as pd
from efiche_data_engineer_assessment.part2_pipeline.config import DB_CONFIG
from efiche_data_engineer_assessment.part2_pipeline.utils.logger import PipelineLogger
from efiche_data_engineer_assessment.part3_analytics.populate_warehouse import WarehouseETL

# Force UTF-8 output for emojis/logs
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')



class AnalyticsOrchestrator:
    """Orchestrate warehouse population and analytics queries"""

    def __init__(self):
        self.config = DB_CONFIG
        self.results = {}

    def run_warehouse_population(self, logger):
        """Step 1: Populate data warehouse"""
        logger.info("\n" + "=" * 60)
        logger.info("STEP 1: Populating Data Warehouse")
        logger.info("=" * 60)

        try:
            etl = WarehouseETL()
            etl.run(logger)
            self.results['warehouse_population'] = 'SUCCESS'
            logger.info("Warehouse populated successfully")
        except Exception as e:
            logger.error(f"Warehouse population failed: {e}")
            self.results['warehouse_population'] = f'FAILED: {e}'
            raise

    def run_sql_analytics(self, logger):
        """Step 2: Run SQL analytics queries"""
        logger.info("\n" + "=" * 60)
        logger.info("STEP 2: Running SQL Analytics Queries")
        logger.info("=" * 60)

        conn = psycopg2.connect(**self.config)

        queries = {
            'encounters_per_month': """
                SELECT 
                    dt.year,
                    dt.month,
                    dt.month_name,
                    COUNT(DISTINCT f.encounter_key) AS total_encounters
                FROM fact_encounters f
                JOIN dim_time dt ON f.date_id = dt.date_id
                GROUP BY dt.year, dt.month, dt.month_name
                ORDER BY dt.year, dt.month
            """,

            'top_diagnoses_by_age': """
                WITH ranked_diagnoses AS (
                    SELECT 
                        dp.age_group,
                        dd.diagnosis_name,
                        COUNT(*) AS frequency,
                        RANK() OVER (PARTITION BY dp.age_group ORDER BY COUNT(*) DESC) AS rank
                    FROM fact_encounters f
                    JOIN dim_patient dp ON f.patient_key = dp.patient_key
                    JOIN bridge_encounter_diagnoses bd ON f.encounter_key = bd.encounter_key
                    JOIN dim_diagnosis dd ON bd.diagnosis_key = dd.diagnosis_key
                    WHERE bd.diagnosis_type = 'Primary'
                    GROUP BY dp.age_group, dd.diagnosis_name
                )
                SELECT age_group, diagnosis_name, frequency, rank
                FROM ranked_diagnoses
                WHERE rank <= 5
                ORDER BY age_group, rank
            """,

            'avg_procedures_per_patient': """
                SELECT 
                    dp.age_group,
                    COUNT(DISTINCT dp.patient_key) AS patient_count,
                    SUM(f.procedure_count) AS total_procedures,
                    ROUND(SUM(f.procedure_count)::NUMERIC / 
                          NULLIF(COUNT(DISTINCT dp.patient_key),0), 2) AS avg_procedures_per_patient
                FROM fact_encounters f
                JOIN dim_patient dp ON f.patient_key = dp.patient_key
                GROUP BY dp.age_group
                ORDER BY dp.age_group
            """
        }

        for query_name, sql in queries.items():
            try:
                logger.info(f"\n   Running query: {query_name}...")
                df = pd.read_sql_query(sql, conn)
                logger.info(f"       Returned {len(df):,} rows")

                # Preview
                logger.info(f"\n      Preview ({query_name}):")
                logger.info(f"\n{df.head(10).to_string(index=False)}")

                # Save to CSV
                output_path = Path(__file__).parent / f"{query_name}_results.csv"
                df.to_csv(output_path, index=False)
                logger.info(f"       Saved to: {output_path}")

                self.results[query_name] = 'SUCCESS'
            except Exception as e:
                logger.error(f"       Query failed: {e}")
                self.results[query_name] = f"FAILED: {e}"

        conn.close()

    def print_summary(self, logger):
        """Print final summary"""
        logger.info("\n" + "=" * 60)
        logger.info(" ANALYTICS EXECUTION SUMMARY")
        logger.info("=" * 60)

        for task, status in self.results.items():
            status_symbol = "✅" if status == 'SUCCESS' else "❌"
            logger.info(f"   {status_symbol} {task:.<40} {status}")

        logger.info("=" * 60)
        logger.info("\n Generated Outputs:")

        output_files = [
            Path(__file__).parent / 'encounters_per_month_results.csv',
            Path(__file__).parent / 'top_diagnoses_by_age_results.csv',
            Path(__file__).parent / 'avg_procedures_per_patient_results.csv'
        ]

        for file_path in output_files:
            if file_path.exists():
                logger.info(f"    {file_path}")

    def run_all(self, logger):
        """Execute warehouse population and analytics queries"""
        self.run_warehouse_population(logger)
        self.run_sql_analytics(logger)
        self.print_summary(logger)


def main():
    """Main execution"""
    with PipelineLogger("Analytics Orchestrator") as logger:
        logger.info(" Starting Analytics Pipeline...")
        orchestrator = AnalyticsOrchestrator()
        orchestrator.run_all(logger)
        logger.info("\n Analytics tasks completed!")
        logger.info("\n Next steps:")
        logger.info("   1. Review CSV outputs in part3_analytics/")
        logger.info("   2. Check visualizations later when embeddings are re-enabled")
        logger.info("   3. Run individual SQL queries from analytics_queries.sql")


if __name__ == "__main__":
    main()
