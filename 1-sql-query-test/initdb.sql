-- Enable extensions if needed (optional)
-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create departments table
CREATE TABLE IF NOT EXISTS departments (
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

-- Create employees table
CREATE TABLE IF NOT EXISTS employees (
    id            SERIAL PRIMARY KEY,
    name          TEXT NOT NULL,
    department_id INTEGER NOT NULL,
    first_salary  INTEGER NOT NULL,
    hire_date     DATE NOT NULL,
    FOREIGN KEY (department_id) REFERENCES departments(id) ON DELETE CASCADE
);

-- Create salaries table
CREATE TABLE IF NOT EXISTS salaries (
    employee_id    INTEGER,
    salary         INTEGER NOT NULL,
    effective_date DATE NOT NULL,
    FOREIGN KEY (employee_id) REFERENCES employees(id) ON DELETE CASCADE
);

-- Copy data from CSV (requires correct file path)
COPY departments FROM '/docker-entrypoint-initdb.d/data/departments.csv' DELIMITER ',' CSV HEADER;
COPY employees FROM '/docker-entrypoint-initdb.d/data/employees.csv' DELIMITER ',' CSV HEADER;
COPY salaries FROM '/docker-entrypoint-initdb.d/data/salaries.csv' DELIMITER ',' CSV HEADER;
