DROP TABLE IF EXISTS dim_time CASCADE;

CREATE TABLE dim_time (
    date_id INTEGER PRIMARY KEY,                  -- Surrogate key in YYYYMMDD format
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    month_name VARCHAR(20) NOT NULL,
    week INTEGER NOT NULL CHECK (week BETWEEN 1 AND 53),
    day_of_month INTEGER NOT NULL CHECK (day_of_month BETWEEN 1 AND 31),
    day_of_week INTEGER NOT NULL CHECK (day_of_week BETWEEN 1 AND 7), -- 1=Monday, 7=Sunday
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER
);

CREATE INDEX idx_dim_time_year ON dim_time(year);
CREATE INDEX idx_dim_time_month ON dim_time(year, month);
CREATE INDEX idx_dim_time_quarter ON dim_time(year, quarter);

COMMENT ON TABLE dim_time IS 'Time dimension for data warehouse analytics';
