-- =============================================
-- Table: procedures
-- Description: Medical imaging procedures (DICOM standards)
-- =============================================

DROP TABLE IF EXISTS procedures CASCADE;

CREATE TABLE procedures (
    procedure_id SERIAL PRIMARY KEY,
    encounter_id VARCHAR(20) NOT NULL REFERENCES encounters(encounter_id) ON DELETE CASCADE,
    procedure_code VARCHAR(50),
    procedure_name VARCHAR(255) NOT NULL,
    procedure_category VARCHAR(100),
    body_part VARCHAR(100),
    laterality VARCHAR(50) CHECK (laterality IN ('Left', 'Right', 'Bilateral', 'N/A')),
    view_position VARCHAR(100),
    modality VARCHAR(50) CHECK (
        modality IN ('X-Ray','CT','MRI','Ultrasound','Fluoroscopy','Mammography')
    ),
    performing_radiologist VARCHAR(255),
    procedure_datetime TIMESTAMP NOT NULL,
    procedure_duration_minutes INTEGER,
    radiation_dose_mgy NUMERIC(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ux_procedures_enc_code UNIQUE (encounter_id, procedure_code)
);


-- Indexes
CREATE INDEX idx_procedures_encounter ON procedures(encounter_id);
CREATE INDEX idx_procedures_modality ON procedures(modality);
CREATE INDEX idx_procedures_body_part ON procedures(body_part);
CREATE INDEX idx_procedures_datetime ON procedures(procedure_datetime);
CREATE INDEX idx_procedures_laterality ON procedures(laterality);

-- Comments
COMMENT ON TABLE procedures IS 'Medical imaging procedures performed during encounters';
COMMENT ON COLUMN procedures.modality IS 'DICOM modality: DX=Digital X-Ray, CR=Computed Radiography, CT=CT Scan, MR=MRI';
COMMENT ON COLUMN procedures.laterality IS 'Left, Right, Bilateral, or N/A';
COMMENT ON COLUMN procedures.radiation_dose_mgy IS 'Radiation dose in milligrays (mGy)';