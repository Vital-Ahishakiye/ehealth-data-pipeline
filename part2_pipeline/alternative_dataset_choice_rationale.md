# Part 2: Dataset Choice Rationale

## Why We're NOT Using PadChest Dataset

### 1. Dataset Availability Issue

**Assignment Requirement:**
> Use the PadChest dataset: https://huggingface.co/datasets/MedHK23/IMT-CXR

**Reality Check:**
When attempting to access the PadChest dataset on HuggingFace:

```python
from datasets import load_dataset
dataset = load_dataset("MedHK23/IMT-CXR")
```

**Result:** ❌ Dataset is not available for download in the assignment's specified location.

**Investigation Results:**
- The HuggingFace link `MedHK23/IMT-CXR` shows only **preview TSV files**
- Full dataset download is **not enabled**
- No complete parquet/CSV files available for bulk download
- The dataset page shows: "Preview only - full dataset not accessible"

### 2. Alternative Dataset Selection: NIH Chest X-ray Dataset

**Chosen Dataset:**
- **Source:** `alkzar90/NIH-Chest-X-ray-dataset` on HuggingFace
- **Size:** 112,120 frontal-view X-ray images
- **Status:** ✅ Publicly accessible, no credentials required

**Why This Dataset Works:**

| Requirement | PadChest (Intended) | NIH Dataset (Actual) | Status |
|-------------|-------------------|---------------------|---------|
| **Patient metadata** | ✅ Age, sex, study date | ✅ Patient ID, age, gender | ✅ Met |
| **Imaging info** | ✅ Projection, modality | ✅ View position, modality | ✅ Met |
| **Diagnostic labels** | ✅ Pneumonia, edema, etc. | ✅ 14 disease labels | ✅ Met |
| **Radiology reports** | ✅ Free text | ⚠️ Not included | ⚠️ Workaround |
| **Downloadable** | ❌ Preview only | ✅ Full access | ✅ Met |

### 3. Handling Missing Radiology Reports

**Challenge:** NIH dataset provides diagnostic labels but NOT full radiology report text.

**Solution:** Generate synthetic reports based on diagnosis labels.

**Implementation:**
```python
# Template-based report generation
def generate_report(diagnosis, age, gender, view_position):
    findings = map_diagnosis_to_findings(diagnosis)
    impression = map_diagnosis_to_impression(diagnosis)
    
    report = f"""
    CLINICAL HISTORY: {age}-year-old {gender} patient.
    
    TECHNIQUE: Chest radiograph, {view_position} view.
    
    FINDINGS: {findings}
    
    IMPRESSION: {impression}
    """
    return report
```

**Result:** 
- ✅ Realistic report text structure
- ✅ Matches diagnosis labels (consistency)
- ✅ Suitable for NLP/embeddings (Part 3 requirement)

### 4. Dataset Comparison

#### **NIH Chest X-ray Dataset - Details**

**Metadata Fields Available:**
```
- Image Index (unique ID)
- Finding Labels (14 categories)
  - Atelectasis
  - Cardiomegaly
  - Effusion
  - Infiltration
  - Mass
  - Nodule
  - Pneumonia
  - Pneumothorax
  - Consolidation
  - Edema
  - Emphysema
  - Fibrosis
  - Pleural_Thickening
  - Hernia
- Follow-up # (encounter tracking)
- Patient ID
- Patient Age
- Patient Gender
- View Position (AP/PA)
- OriginalImage[Width x Height]
- OriginalImagePixelSpacing[x y]
```

**Sample Record:**
```csv
Image Index,Finding Labels,Patient ID,Patient Age,Patient Gender,View Position
00000001_000.png,"Cardiomegaly|Emphysema",1,58,M,PA
00000002_000.png,"No Finding",2,45,F,AP
```

#### **PadChest Dataset - What We're Missing**

**Unavailable in Preview:**
- Full download capability
- Complete metadata CSV
- Original DICOM files
- Full radiology reports (text)

### 5. Impact on Assignment Deliverables

| Deliverable | Impact | Mitigation |
|------------|--------|-----------|
| **ETL Pipeline** | ✅ No impact | Use NIH dataset |
| **Incremental Loads** | ✅ No impact | Same logic applies |
| **Report Text for Embeddings** | ⚠️ No real reports | Generate synthetic reports |
| **Analytics Queries** | ✅ No impact | Same data structure |
| **Diagnosis-based clustering** | ✅ Enhanced | 14 standardized labels |

### 6. Advantages of NIH Dataset

**Pros:**
1. ✅ **Accessibility**: Immediate download, no credentials
2. ✅ **Size**: 112k+ images (exceeds 10k requirement)
3. ✅ **Standardized Labels**: 14 common chest pathologies
4. ✅ **Well-Documented**: Widely used in research
5. ✅ **HuggingFace Integration**: Easy to download via `datasets` library

**Cons:**
1. ⚠️ **No Report Text**: Need to generate synthetic reports
2. ⚠️ **Limited Demographics**: Only age/gender (no location)

### 7. Data Pipeline Design Decisions

**To maintain assignment requirements:**

1. **Map NIH labels to eHealth diagnoses:**
   ```python
   nih_to_icd10 = {
       "Pneumonia": "J18.9",
       "Edema": "J81.0",
       "Cardiomegaly": "I51.7",
       # ... etc
   }
   ```

2. **Generate realistic reports:**
   - Use templates with medical terminology
   - Match findings to diagnosis labels
   - Include standard report sections (History, Technique, Findings, Impression)

3. **Preserve data lineage:**
   - Store original NIH Image Index
   - Link to generated procedure IDs
   - Maintain audit trail

### 8. Stakeholder Communication

**For Assignment Reviewers:**

> "Due to the PadChest dataset being unavailable for download at the specified HuggingFace location (preview-only mode), we selected the NIH Chest X-ray dataset as an alternative. This dataset provides:
> 
> - ✅ All required metadata fields (patient demographics, imaging info, diagnostic labels)
> - ✅ 112k+ images (exceeds 10k requirement)
> - ✅ Full download access
> 
> To address the missing radiology report text, we implemented synthetic report generation based on diagnosis labels, which still fulfills the requirement for text embeddings and NLP analytics in Part 3."

### 9. Production Considerations

**In a real-world scenario:**

1. **Vendor Integration**: 
   - Connect directly to hospital PACS system
   - Real-time data feeds (HL7/FHIR)
   
2. **Data Quality**:
   - Real reports with physician-validated text
   - No synthetic generation needed
   
3. **Privacy**:
   - De-identification pipeline
   - HIPAA compliance

### 10. Conclusion

**Decision:** Use NIH Chest X-ray dataset with synthetic report generation.

**Rationale:**
- ✅ Achieves all assignment objectives
- ✅ Demonstrates ETL/pipeline skills
- ✅ Enables Part 3 analytics (embeddings, clustering)
- ✅ Production-ready pipeline design (adaptable to real data sources)

**Trade-off Accepted:**
- Synthetic reports vs. real reports
- Still demonstrates: template design, text generation, NLP readiness

---

## References

1. NIH Chest X-ray Dataset: https://huggingface.co/datasets/alkzar90/NIH-Chest-X-ray-dataset
2. Original NIH Paper: Wang et al. "ChestX-ray8: Hospital-scale Chest X-ray Database and Benchmarks on Weakly-Supervised Classification and Localization of Common Thorax Diseases" (CVPR 2017)
3. PadChest Dataset Reference: https://huggingface.co/datasets/MedHK23/IMT-CXR (preview only)