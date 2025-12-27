-- warehouse.dimdate (Date Dimension - 3 years of data)
CREATE TABLE IF NOT EXISTS warehouse.dimdate (
    datekey SERIAL PRIMARY KEY,
    fulldate DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    monthname VARCHAR(20) NOT NULL,
    day INTEGER NOT NULL,
    weekday INTEGER NOT NULL,
    weekdayname VARCHAR(20) NOT NULL,
    isweekend BOOLEAN NOT NULL
);

-- Populate 3 years (2024-2026)
INSERT INTO warehouse.dimdate (fulldate, year, quarter, month, monthname, day, weekday, weekdayname, isweekend)
SELECT 
    d::date,
    EXTRACT(YEAR FROM d)::int,
    EXTRACT(QUARTER FROM d)::int,
    EXTRACT(MONTH FROM d)::int,
    TO_CHAR(d, 'Month'),
    EXTRACT(DAY FROM d)::int,
    EXTRACT(DOW FROM d)::int,
    TO_CHAR(d, 'Day'),
    (EXTRACT(DOW FROM d) IN (0,6))
FROM generate_series('2024-01-01'::date, '2026-12-31'::date, '1 day') AS d;

CREATE UNIQUE INDEX idx_dimdate_fulldate ON warehouse.dimdate(fulldate);
