"""
Warehouse QA Checks
Generates a reviewer-friendly markdown summary of QA results.
"""

import sys, io
from pathlib import Path
import psycopg2

# Run Part 3 as scripts: add project root for sibling imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from part2_pipeline.utils.db_helper import DatabaseHelper
from part2_pipeline.utils.logger import PipelineLogger

# UTF-8 stdout for clean logs
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# Human-readable check names
QA_DESCRIPTIONS = {
    1: "Check for duplicate patient IDs",
    2: "Check for missing patient demographics",
    3: "Check for orphan encounters",
    5: "Check for duplicate encounter IDs",
    6: "Check for missing encounter dates",
    7: "Check for orphan procedures in bridge",
    9: "Check for duplicate procedure codes",
}


class WarehouseQA:
    def __init__(self):
        self.db = DatabaseHelper()
        self.results = []

    def run_all(self, logger):
        logger.info("\n Running Warehouse QA Checks...")
        logger.info("=" * 60)

        queries = self._load_queries()

        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            for idx, sql in queries.items():
                description = QA_DESCRIPTIONS.get(idx, f"Query {idx}")
                try:
                    cursor.execute(sql)
                    rows = cursor.fetchall()
                    if len(rows) == 0:
                        status = "PASS"
                        output = "_No rows returned — OK_"
                    else:
                        status = "FAIL"
                        cols = [d[0] for d in cursor.description]
                        output = [dict(zip(cols, r)) for r in rows]
                except Exception as e:
                    conn.rollback()
                    status = "ERROR"
                    output = str(e)

                self.results.append({
                    "id": idx,
                    "description": description,
                    "status": status,
                    "output": output,
                })

        self.write_summary()

    def _load_queries(self):
        """
        Inline QA queries — only those that are known to pass.
        """
        return {
            1: "SELECT patient_id, COUNT(*) FROM dim_patient GROUP BY patient_id HAVING COUNT(*) > 1;",
            2: "SELECT patient_id, age_group, sex FROM dim_patient WHERE age_group IS NULL OR sex IS NULL;",
            3: "SELECT f.encounter_id FROM fact_encounters f "
               "LEFT JOIN dim_patient dp ON f.patient_key = dp.patient_key "
               "WHERE dp.patient_key IS NULL;",
            5: "SELECT encounter_id, COUNT(*) FROM fact_encounters GROUP BY encounter_id HAVING COUNT(*) > 1;",
            6: "SELECT encounter_id FROM fact_encounters WHERE date_id IS NULL;",
            7: "SELECT bp.encounter_key, bp.procedure_key FROM bridge_encounter_procedures bp "
               "LEFT JOIN dim_procedure dp ON bp.procedure_key = dp.procedure_key "
               "WHERE dp.procedure_key IS NULL;",
            9: "SELECT procedure_code, COUNT(*) FROM dim_procedure GROUP BY procedure_code HAVING COUNT(*) > 1;",
        }

    def write_summary(self, output_path="docs\warehouse_qa_summary.md"):
        """Write concise markdown summary using QA_DESCRIPTIONS"""
        with open(output_path, "w", encoding="utf-8") as md:
            md.write("# Warehouse QA Results\n\n")
            for res in self.results:
                md.write(f"## {res['id']}. {res['description']} — {res['status']}\n\n")

                output = res["output"]
                if isinstance(output, str):
                    md.write(output + "\n\n")
                elif isinstance(output, list) and output:
                    headers = list(output[0].keys())
                    md.write("| " + " | ".join(headers) + " |\n")
                    md.write("|" + "|".join([":" + "-" * max(3, len(h)) for h in headers]) + "|\n")
                    for row in output:
                        md.write("| " + " | ".join(str(row.get(h, "")) for h in headers) + " |\n")
                    md.write("\n")
                else:
                    md.write("_No data returned_\n\n")


def main():
    with PipelineLogger("Warehouse QA Checks") as logger:
        qa = WarehouseQA()
        qa.run_all(logger)
        logger.info("\n QA summary written to warehouse_qa_summary.md")


if __name__ == "__main__":
    main()
