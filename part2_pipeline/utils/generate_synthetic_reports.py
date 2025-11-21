"""
Generate Synthetic Radiology Reports
Creates realistic report text based on NIH diagnosis labels
"""

import sys
from pathlib import Path
import pandas as pd
import random
from faker import Faker

sys.path.insert(0, str(Path(__file__).parent))
from part2_pipeline.config import DATA_DIR, NIH_SUBSET_NAME, NIH_TO_ICD10_MAPPING
from part2_pipeline.utils.logger import setup_logger

# Initialize
fake = Faker()
Faker.seed(42)
random.seed(42)
logger = setup_logger(__name__)


class ReportGenerator:
    """Generate synthetic radiology reports from diagnosis labels"""
    
    def __init__(self):
        self.report_templates = self._load_templates()
    
    def _load_templates(self):
        """Load report templates by diagnosis"""
        return {
            'Pneumonia': {
                'findings': [
                    "Airspace opacity in the {location} consistent with pneumonia.",
                    "Patchy infiltrates in the {location} suggestive of infectious process.",
                    "Consolidation in the {location} with air bronchograms."
                ],
                'impression': [
                    "Findings consistent with pneumonia in the {location}.",
                    "Acute infectious process involving the {location}.",
                    "Radiographic appearance suggests bacterial pneumonia."
                ]
            },
            'Edema': {
                'findings': [
                    "Bilateral perihilar haziness with vascular congestion.",
                    "Interstitial edema with Kerley B lines noted.",
                    "Diffuse pulmonary edema with cardiomegaly."
                ],
                'impression': [
                    "Findings consistent with pulmonary edema.",
                    "Congestive heart failure with pulmonary edema.",
                    "Acute pulmonary edema, likely cardiogenic."
                ]
            },
            'Cardiomegaly': {
                'findings': [
                    "Cardiac silhouette is enlarged with cardiothoracic ratio >0.5.",
                    "Enlarged cardiac shadow with prominent left ventricle.",
                    "Cardiomegaly noted with chamber enlargement."
                ],
                'impression': [
                    "Cardiomegaly, clinical correlation recommended.",
                    "Enlarged cardiac silhouette, suggest echocardiogram.",
                    "Significant cardiomegaly identified."
                ]
            },
            'Pneumothorax': {
                'findings': [
                    "Absence of lung markings in the {location} with visible visceral pleural line.",
                    "Lucency in the {location} consistent with pneumothorax.",
                    "Partial collapse of {location} lung with pneumothorax."
                ],
                'impression': [
                    "{severity} pneumothorax in the {location}.",
                    "Pneumothorax requiring clinical correlation.",
                    "Spontaneous pneumothorax identified."
                ]
            },
            'Atelectasis': {
                'findings': [
                    "Volume loss in the {location} with linear opacities.",
                    "Subsegmental atelectasis in the {location}.",
                    "Plate-like atelectasis at the lung bases."
                ],
                'impression': [
                    "Atelectasis in the {location}, likely subsegmental.",
                    "Mild atelectasis without infiltrate.",
                    "Bibasilar atelectasis noted."
                ]
            },
            'Effusion': {
                'findings': [
                    "Blunting of the {location} costophrenic angle with layering fluid.",
                    "Moderate pleural effusion in the {location}.",
                    "Subpulmonic effusion with meniscus sign on {location}."
                ],
                'impression': [
                    "Pleural effusion in the {location}.",
                    "{severity} effusion, consider thoracentesis if symptomatic.",
                    "Layering pleural fluid identified."
                ]
            },
            'No Finding': {
                'findings': [
                    "Lungs are clear without focal consolidation, effusion, or pneumothorax.",
                    "Cardiac silhouette is normal in size and contour.",
                    "No acute cardiopulmonary abnormality detected."
                ],
                'impression': [
                    "No acute cardiopulmonary process.",
                    "Normal chest radiograph.",
                    "Clear lungs, no acute findings."
                ]
            }
        }
    
    def generate_report(self, finding_labels, age, gender, view_position):
        """
        Generate complete radiology report
        
        Args:
            finding_labels: Pipe-separated diagnosis labels (e.g., "Pneumonia|Edema")
            age: Patient age
            gender: Patient gender (M/F)
            view_position: View position (PA/AP)
        
        Returns:
            dict with report components
        """
        # Parse findings
        findings = finding_labels.split('|') if '|' in finding_labels else [finding_labels]
        primary_finding = findings[0].strip()
        
        # Get templates (default to No Finding if not found)
        templates = self.report_templates.get(primary_finding, self.report_templates['No Finding'])
        
        # Generate report sections
        clinical_history = self._generate_clinical_history(primary_finding, age, gender)
        technique = self._generate_technique(view_position)
        findings_text = self._generate_findings(primary_finding, templates, findings)
        impression = self._generate_impression(primary_finding, templates)
        recommendations = self._generate_recommendations(primary_finding)
        
        # Combine into full report
        full_report = f"""{clinical_history}

{technique}

FINDINGS:
{findings_text}

IMPRESSION:
{impression}

{recommendations}"""
        
        return {
            'report_text': full_report,
            'findings': findings_text,
            'impression': impression,
            'recommendations': recommendations,
            'report_type': self._get_report_type(),      # ✅ Fixed
            'report_status': self._get_report_status()   # ✅ Fixed
        }
    
    def _generate_clinical_history(self, primary_finding, age, gender):
        """Generate clinical history section"""
        gender_text = "male" if gender == 'M' else "female"
        
        symptoms = {
            'Pneumonia': ['cough', 'fever', 'shortness of breath'],
            'Edema': ['dyspnea', 'orthopnea', 'lower extremity edema'],
            'Cardiomegaly': ['chest pain', 'dyspnea on exertion', 'palpitations'],
            'Pneumothorax': ['sudden chest pain', 'dyspnea', 'trauma'],
            'Atelectasis': ['postoperative', 'decreased breath sounds', 'hypoxia'],
            'Effusion': ['dyspnea', 'decreased breath sounds', 'pleuritic chest pain'],
            'No Finding': ['routine examination', 'chest pain', 'pre-operative clearance']
        }
        
        symptom_list = symptoms.get(primary_finding, ['chest pain'])
        selected_symptom = random.choice(symptom_list)
        
        return f"CLINICAL HISTORY: {age}-year-old {gender_text} with {selected_symptom}."
    
    def _generate_technique(self, view_position):
        """Generate technique section"""
        view_text = "posteroanterior (PA)" if view_position == 'PA' else "anteroposterior (AP)"
        return f"TECHNIQUE: Single frontal chest radiograph, {view_text} view."
    
    def _generate_findings(self, primary_finding, templates, all_findings):
        """Generate findings section"""
        # Primary finding
        finding_template = random.choice(templates['findings'])
        
        # Add location details
        locations = ['right lower lobe', 'left lower lobe', 'right upper lobe', 
                    'left upper lobe', 'bilateral lower lobes', 'right middle lobe']
        location = random.choice(locations)
        
        severities = ['small', 'moderate', 'large']
        severity = random.choice(severities)
        
        primary_text = finding_template.format(location=location, severity=severity)
        
        # Add secondary findings if multiple
        if len(all_findings) > 1:
            secondary = []
            for finding in all_findings[1:]:
                if finding in self.report_templates:
                    sec_template = random.choice(self.report_templates[finding]['findings'])
                    secondary.append(sec_template.format(location=location, severity=severity))
            
            if secondary:
                primary_text += " " + " ".join(secondary)
        
        # Add standard observations
        standard_obs = "Heart size is within normal limits. Mediastinal contours are unremarkable. Osseous structures are intact."
        
        return f"{primary_text} {standard_obs}"
    
    def _generate_impression(self, primary_finding, templates):
        """Generate impression section"""
        impression_template = random.choice(templates['impression'])
        
        locations = ['right lower lobe', 'left lower lobe', 'bilateral lower lobes']
        severities = ['small', 'moderate', 'large']
        
        return impression_template.format(
            location=random.choice(locations),
            severity=random.choice(severities)
        )
    
    def _generate_recommendations(self, primary_finding):
        """Generate recommendations section"""
        recommendations = {
            'Pneumonia': "Recommend clinical correlation and follow-up imaging in 6-8 weeks to document resolution.",
            'Edema': "Recommend correlation with clinical status and cardiac evaluation.",
            'Cardiomegaly': "Recommend echocardiogram for further evaluation.",
            'Pneumothorax': "Recommend clinical correlation and repeat imaging to assess stability.",
            'Atelectasis': "Recommend incentive spirometry and repeat imaging if clinically indicated.",
            'Effusion': "Recommend thoracentesis if symptomatic. Follow-up imaging advised.",
            'No Finding': "No follow-up imaging required unless clinically indicated."
        }
        
        return f"RECOMMENDATIONS: {recommendations.get(primary_finding, 'Clinical correlation advised.')}"
    
    def _get_report_type(self):
        """
        Get report type matching Part 1 schema CHECK constraint
        
        Part 1 allows: 'Radiology Report', 'Diagnostic Report', 'Preliminary Report'
        """
        return random.choice([
            'Radiology Report',      # ✅ Most common
            'Radiology Report',      # Higher probability
            'Preliminary Report',    # ✅ Less common
            'Diagnostic Report'      # ✅ Occasionally
        ])
    def _get_report_status(self):
        """
        Get report status matching Part 1 schema CHECK constraint
        
        Part 1 allows: 'Draft', 'Preliminary', 'Final', 'Amended'
        
        NOTE: Your Part 1 schema actually has:
        CHECK (report_status IN ('Draft','Preliminary','Final','Amended'))
        
        But your document shows: 'Draft','Signed','Amended'
        Using the schema from 07_reports.sql you provided.
        """
        return random.choice([
            'Final',         # ✅ Most common
            'Final',         # Higher probability
            'Preliminary',   # ✅ Less common
            'Amended'        # ✅ Rare
        ])


def generate_reports_for_dataset():
    """Generate reports for entire NIH dataset"""
    logger.info("=" * 60)
    logger.info("📄 Generating Synthetic Radiology Reports")
    logger.info("=" * 60)
    
    # Load dataset
    csv_path = DATA_DIR / NIH_SUBSET_NAME
    if not csv_path.exists():
        logger.error(f"❌ Dataset not found: {csv_path}")
        logger.error("   Run: python extract_nih_dataset.py first")
        return None
    
    logger.info(f"📂 Loading dataset from: {csv_path}")
    df = pd.read_csv(csv_path)
    logger.info(f"✅ Loaded {len(df):,} records")
    
    # Generate reports
    logger.info("\n📝 Generating reports...")
    generator = ReportGenerator()
    
    reports = []
    for idx, row in df.iterrows():
        report = generator.generate_report(
            finding_labels=row['Finding Labels'],
            age=int(row['Patient Age']),
            gender=row['Patient Gender'],
            view_position=row['View Position']
        )
        reports.append(report)
        
        if (idx + 1) % 1000 == 0:
            logger.info(f"   Generated {idx + 1:,} reports...")
    
    # Add reports to dataframe
    logger.info("\n💾 Saving reports to dataset...")
    for col in ['report_text', 'findings', 'impression', 'recommendations', 'report_type', 'report_status']:
        df[col] = [r[col] for r in reports]
    
    # Save enhanced dataset
    output_path = DATA_DIR / "nih_with_reports.csv"
    df.to_csv(output_path, index=False)
    logger.info(f"✅ Saved to: {output_path}")
    
    # Sample report
    logger.info("\n📋 Sample Report:")
    logger.info("-" * 60)
    logger.info(df['report_text'].iloc[0])
    logger.info("-" * 60)
    
    return output_path


def main():
    """Main execution"""
    try:
        output_path = generate_reports_for_dataset()
        if output_path:
            print(f"\n✅ SUCCESS! Reports generated: {output_path}")
            print("\n📋 Next step:")
            print("   Run: python etl_pipeline.py")
    except Exception as e:
        print(f"\n❌ FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()