"""
Populate Data Warehouse - Star Schema ETL
Transforms operational data into dimensional model with key mapping
"""

import sys
from pathlib import Path
from datetime import timedelta
import psycopg2
from psycopg2.extras import execute_batch

sys.path.insert(0, str(Path(__file__).parent.parent))
from efiche_data_engineer_assessment.part2_pipeline.config import DB_CONFIG
from efiche_data_engineer_assessment.part2_pipeline.utils.logger import PipelineLogger


class WarehouseETL:
    """ETL for populating data warehouse star schema"""

    def __init__(self):
        self.config = DB_CONFIG
        self.stats = {
            'dim_time_records': 0,
            'dim_patient_records': 0,
            'dim_procedure_records': 0,
            'dim_diagnosis_records': 0,
            'fact_encounters_records': 0,
            'bridge_procedures_records': 0,
            'bridge_diagnoses_records': 0
        }

    def get_connection(self):
        return psycopg2.connect(**self.config)

    def clear_warehouse(self, cursor):
        """Clear existing warehouse data (order matters due to FKs)"""
        tables = [
            'bridge_encounter_diagnoses',
            'bridge_encounter_procedures',
            'fact_encounters',
            'dim_diagnosis',
            'dim_procedure',
            'dim_patient',
            'dim_time'
        ]
        for table in tables:
            cursor.execute(f"TRUNCATE TABLE {table} CASCADE")

    def populate_dim_time(self, cursor, logger):
        logger.info("\nPopulating dim_time...")

        cursor.execute("""
            SELECT MIN(encounter_date), MAX(encounter_date)
            FROM encounters
        """)
        min_date, max_date = cursor.fetchone()

        if not min_date or not max_date:
            logger.warning("   No encounter dates found!")
            return

        # Generate date rows
        current_date = min_date
        time_records = []
        while current_date <= max_date:
            date_id = int(current_date.strftime('%Y%m%d'))
            time_records.append((
                date_id,
                current_date,
                current_date.year,
                (current_date.month - 1) // 3 + 1,
                current_date.month,
                current_date.strftime('%B'),
                current_date.isocalendar()[1],
                current_date.day,
                current_date.weekday() + 1,
                current_date.strftime('%A'),
                current_date.weekday() >= 5,
                False,
                current_date.year,
                (current_date.month - 1) // 3 + 1
            ))
            current_date += timedelta(days=1)

        execute_batch(cursor, """
            INSERT INTO dim_time (date_id, full_date, year, quarter, month, month_name,
                                  week, day_of_month, day_of_week, day_name, is_weekend,
                                  is_holiday, fiscal_year, fiscal_quarter)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_id) DO NOTHING
        """, time_records, page_size=1000)

        self.stats['dim_time_records'] = len(time_records)
        logger.info(f"   Inserted {len(time_records):,} time records")

    def populate_dim_patient(self, cursor, logger):
        logger.info("\nPopulating dim_patient...")

        # Map operational VARCHAR patient_id (e.g., PAT0010001) to INTEGER by stripping non-digits
        cursor.execute("""
            INSERT INTO dim_patient (patient_id, age, sex, age_group, location)
            SELECT
                CAST(REGEXP_REPLACE(p.patient_id, '\D', '', 'g') AS INTEGER) AS patient_id,
                EXTRACT(YEAR FROM AGE(MIN(e.encounter_date), p.date_of_birth))::INTEGER AS age,
                p.gender AS sex,
                CASE
                    WHEN EXTRACT(YEAR FROM AGE(MIN(e.encounter_date), p.date_of_birth)) < 18 THEN 'Pediatric'
                    WHEN EXTRACT(YEAR FROM AGE(MIN(e.encounter_date), p.date_of_birth)) BETWEEN 18 AND 35 THEN 'Young Adult'
                    WHEN EXTRACT(YEAR FROM AGE(MIN(e.encounter_date), p.date_of_birth)) BETWEEN 36 AND 55 THEN 'Middle Age'
                    WHEN EXTRACT(YEAR FROM AGE(MIN(e.encounter_date), p.date_of_birth)) BETWEEN 56 AND 75 THEN 'Senior'
                    ELSE 'Elderly'
                END AS age_group,
                'Kigali' AS location
            FROM patients p
            LEFT JOIN encounters e ON p.patient_id = e.patient_id
            GROUP BY p.patient_id, p.date_of_birth, p.gender
            ON CONFLICT (patient_id) DO NOTHING
        """)

        cursor.execute("SELECT COUNT(*) FROM dim_patient")
        self.stats['dim_patient_records'] = cursor.fetchone()[0]
        logger.info(f"   Dim patient rows: {self.stats['dim_patient_records']:,}")

    def populate_dim_procedure(self, cursor, logger):
        logger.info("\nPopulating dim_procedure...")

        # Use operational procedures.procedure_id (SERIAL INTEGER) as natural key
        cursor.execute("""
            INSERT INTO dim_procedure (
                procedure_id, external_procedure_id, procedure_code, procedure_name,
                modality, projection, body_part
            )
            SELECT DISTINCT
                p.procedure_id,
                NULL::VARCHAR(100),
                p.procedure_code,
                p.procedure_name,
                p.modality,
                p.view_position AS projection,
                p.body_part
            FROM procedures p
            WHERE p.procedure_id IS NOT NULL
            ON CONFLICT (procedure_id) DO NOTHING
        """)

        cursor.execute("SELECT COUNT(*) FROM dim_procedure")
        self.stats['dim_procedure_records'] = cursor.fetchone()[0]
        logger.info(f"   Dim procedure rows: {self.stats['dim_procedure_records']:,}")

    def populate_dim_diagnosis(self, cursor, logger):
        logger.info("\nPopulating dim_diagnosis...")

        # Map operational VARCHAR diagnosis_id to INTEGER by stripping non-digits
        cursor.execute("""
            INSERT INTO dim_diagnosis (
                diagnosis_id, diagnosis_code, diagnosis_name, category, severity
            )
            SELECT
                CAST(REGEXP_REPLACE(d.diagnosis_id, '\D', '', 'g') AS INTEGER) AS diagnosis_id,
                d.diagnosis_code,
                d.diagnosis_name,
                d.diagnosis_category AS category,
                d.severity
            FROM diagnoses d
            ON CONFLICT (diagnosis_id) DO NOTHING
        """)

        cursor.execute("SELECT COUNT(*) FROM dim_diagnosis")
        self.stats['dim_diagnosis_records'] = cursor.fetchone()[0]
        logger.info(f"   Dim diagnosis rows: {self.stats['dim_diagnosis_records']:,}")

    def populate_fact_encounters(self, cursor, logger):
        logger.info("\nPopulating fact_encounters...")

        # Insert using mapped integer encounter_id and patient_key
        cursor.execute("""
            INSERT INTO fact_encounters (
                encounter_id, patient_key, date_id, facility_id, encounter_type,
                procedure_count, diagnosis_count, report_count
            )
            SELECT
                CAST(REGEXP_REPLACE(e.encounter_id, '\D', '', 'g') AS INTEGER) AS encounter_id,
                dp.patient_key,
                TO_CHAR(e.encounter_date, 'YYYYMMDD')::INTEGER AS date_id,
                CAST(REGEXP_REPLACE(e.facility_id, '\D', '', 'g') AS INTEGER) AS facility_id,
                e.encounter_type,
                COALESCE(COUNT(DISTINCT p.procedure_id), 0) AS procedure_count,
                COALESCE(COUNT(DISTINCT ed.diagnosis_id), 0) AS diagnosis_count,
                COALESCE(COUNT(DISTINCT r.report_id), 0) AS report_count
            FROM encounters e
            JOIN dim_patient dp
              ON dp.patient_id = CAST(REGEXP_REPLACE(e.patient_id, '\D', '', 'g') AS INTEGER)
            LEFT JOIN procedures p
              ON e.encounter_id = p.encounter_id
            LEFT JOIN encounter_diagnoses ed
              ON e.encounter_id = ed.encounter_id
            LEFT JOIN reports r
              ON e.encounter_id = r.encounter_id
            GROUP BY e.encounter_id, dp.patient_key, e.encounter_date, e.facility_id, e.encounter_type
            ON CONFLICT (encounter_id) DO NOTHING
        """)

        cursor.execute("SELECT COUNT(*) FROM fact_encounters")
        self.stats['fact_encounters_records'] = cursor.fetchone()[0]
        logger.info(f"   Fact encounters rows: {self.stats['fact_encounters_records']:,}")

    def populate_bridge_procedures(self, cursor, logger):
        logger.info("\nPopulating bridge_encounter_procedures...")

        # Join on fact_encounters.encounter_key and dim_procedure.procedure_key
        cursor.execute("""
            INSERT INTO bridge_encounter_procedures (encounter_key, procedure_key)
            SELECT DISTINCT
                f.encounter_key,
                dp.procedure_key
            FROM fact_encounters f
            JOIN encounters e
              ON CAST(REGEXP_REPLACE(e.encounter_id, '\D', '', 'g') AS INTEGER) = f.encounter_id
            JOIN procedures p
              ON e.encounter_id = p.encounter_id
            JOIN dim_procedure dp
              ON dp.procedure_id = p.procedure_id
            ON CONFLICT (encounter_key, procedure_key) DO NOTHING
        """)

        cursor.execute("SELECT COUNT(*) FROM bridge_encounter_procedures")
        self.stats['bridge_procedures_records'] = cursor.fetchone()[0]
        logger.info(f"   Bridge procedures rows: {self.stats['bridge_procedures_records']:,}")

    def populate_bridge_diagnoses(self, cursor, logger):
        logger.info("\nPopulating bridge_encounter_diagnoses...")

        cursor.execute("""
            INSERT INTO bridge_encounter_diagnoses (encounter_key, diagnosis_key, diagnosis_type)
            SELECT DISTINCT
                f.encounter_key,
                dd.diagnosis_key,
                CASE WHEN ed.is_primary THEN 'Primary' ELSE 'Secondary' END AS diagnosis_type
            FROM fact_encounters f
            JOIN encounters e
              ON CAST(REGEXP_REPLACE(e.encounter_id, '\D', '', 'g') AS INTEGER) = f.encounter_id
            JOIN encounter_diagnoses ed
              ON e.encounter_id = ed.encounter_id
            JOIN dim_diagnosis dd
              ON dd.diagnosis_id = CAST(REGEXP_REPLACE(ed.diagnosis_id, '\D', '', 'g') AS INTEGER)
            ON CONFLICT (encounter_key, diagnosis_key) DO NOTHING
        """)

        cursor.execute("SELECT COUNT(*) FROM bridge_encounter_diagnoses")
        self.stats['bridge_diagnoses_records'] = cursor.fetchone()[0]
        logger.info(f"   Bridge diagnoses rows: {self.stats['bridge_diagnoses_records']:,}")

    def run(self, logger):
        conn = None
        try:
            conn = self.get_connection()
            conn.autocommit = False
            cursor = conn.cursor()

            logger.info("  Clearing existing warehouse data...")
            self.clear_warehouse(cursor)
            conn.commit()
            logger.info("    Warehouse cleared")

            self.populate_dim_time(cursor, logger); conn.commit()
            self.populate_dim_patient(cursor, logger); conn.commit()
            self.populate_dim_procedure(cursor, logger); conn.commit()
            self.populate_dim_diagnosis(cursor, logger); conn.commit()
            self.populate_fact_encounters(cursor, logger); conn.commit()
            self.populate_bridge_procedures(cursor, logger); conn.commit()
            self.populate_bridge_diagnoses(cursor, logger); conn.commit()

            logger.info("\n" + "=" * 60)
            logger.info("WAREHOUSE POPULATION SUMMARY")
            logger.info("=" * 60)
            for key, value in self.stats.items():
                logger.info(f"   {key.replace('_', ' ').title():.<40} {value:>10,}")
            logger.info("=" * 60)

            cursor.close()
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                conn.close()


def main():
    with PipelineLogger("Warehouse Population") as logger:
        logger.info("Starting Data Warehouse Population...")
        etl = WarehouseETL()
        etl.run(logger)
        logger.info("\nData warehouse populated successfully!")


if __name__ == "__main__":
    main()
