-- =============================================
-- Table: facilities
-- Description: Healthcare facilities in Rwanda
-- =============================================

DROP TABLE IF EXISTS facilities CASCADE;

CREATE TABLE facilities (
    facility_id VARCHAR(50) PRIMARY KEY,        -- e.g., 'FAC000001'
    facility_name VARCHAR(200) NOT NULL,
    facility_type VARCHAR(50) NOT NULL CHECK (facility_type IN ('Hospital', 'Clinic', 'Imaging Center', 'Urgent Care')),
    address_line1 VARCHAR(200),
    address_city VARCHAR(100),
    address_state VARCHAR(50) NOT NULL,         -- Province in Rwanda
    address_zipcode VARCHAR(10),
    phone VARCHAR(20),
    total_beds INTEGER,
    has_emergency BOOLEAN DEFAULT FALSE,
    has_icu BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_facilities_name ON facilities(facility_name);
CREATE INDEX idx_facilities_type ON facilities(facility_type);
CREATE INDEX idx_facilities_city ON facilities(address_city);
CREATE INDEX idx_facilities_state ON facilities(address_state);

-- Comments
COMMENT ON TABLE facilities IS 'Healthcare facilities in Rwanda (hospitals, clinics, health centers)';
COMMENT ON COLUMN facilities.facility_type IS 'Hospital, Clinic, Imaging Center, or Urgent Care';
COMMENT ON COLUMN facilities.address_state IS 'Province: Kigali, Eastern, Southern, Western, Northern';