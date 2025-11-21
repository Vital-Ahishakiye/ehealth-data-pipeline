DROP TABLE IF EXISTS dim_procedure CASCADE;

CREATE TABLE dim_procedure (
    procedure_key SERIAL PRIMARY KEY,             -- Surrogate key for warehouse
    procedure_id INTEGER NOT NULL UNIQUE,         -- Natural key from operational procedures
    external_procedure_id VARCHAR(100),
    procedure_code VARCHAR(50),
    procedure_name VARCHAR(255),
    modality VARCHAR(100),
    projection VARCHAR(100),
    body_part VARCHAR(100),
    effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE INDEX idx_dim_procedure_id ON dim_procedure(procedure_id);
CREATE INDEX idx_dim_procedure_modality ON dim_procedure(modality);
CREATE INDEX idx_dim_procedure_body_part ON dim_procedure(body_part);

COMMENT ON TABLE dim_procedure IS 'Procedure dimension for analytics';
