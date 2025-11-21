-- =============================================
-- Table: reports
-- Description: Radiology reports (structured + free-text)
-- =============================================

DROP TABLE IF EXISTS reports CASCADE;

CREATE TABLE reports (
    report_id SERIAL PRIMARY KEY,
    encounter_id VARCHAR(20) NOT NULL REFERENCES encounters(encounter_id) ON DELETE CASCADE,
    report_type VARCHAR(50) CHECK (
    report_type IN ('Radiology Report','Diagnostic Report','Preliminary Report')),
    report_status VARCHAR(50) CHECK (
    report_status IN ('Draft','Preliminary','Final','Amended')),
    report_text TEXT NOT NULL,                  -- Full report text
    findings TEXT,                              -- Structured findings section
    impression TEXT,                            -- Structured impression section
    recommendations TEXT,                       -- Clinical recommendations
    radiologist_name VARCHAR(255),
    dictated_datetime TIMESTAMP,                -- When report was dictated
    signed_datetime TIMESTAMP,                  -- When report was signed
    critical_finding BOOLEAN DEFAULT FALSE,     -- True if critical result
    critical_notification_datetime TIMESTAMP,   -- When critical result was communicated
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_reports_encounter ON reports(encounter_id);
CREATE INDEX idx_reports_type ON reports(report_type);
CREATE INDEX idx_reports_status ON reports(report_status);
CREATE INDEX idx_reports_critical ON reports(critical_finding);
CREATE INDEX idx_reports_dictated ON reports(dictated_datetime);
CREATE INDEX idx_reports_signed ON reports(signed_datetime);

-- Full-text search index
CREATE INDEX idx_reports_text_search ON reports USING gin(to_tsvector('english', report_text));

-- Comments
COMMENT ON TABLE reports IS 'Radiology reports with structured and unstructured content';
COMMENT ON COLUMN reports.report_type IS 'Preliminary, Final, Addendum, or Corrected';
COMMENT ON COLUMN reports.report_status IS 'Draft, Signed, or Amended';
COMMENT ON COLUMN reports.critical_finding IS 'True if report contains critical findings requiring immediate attention';