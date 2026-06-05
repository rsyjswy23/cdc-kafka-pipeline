-- 1. Create employees table (run on source_db)
CREATE TABLE IF NOT EXISTS employees (
    emp_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT
);

-- 2. Create CDC table (run on source_db)
CREATE TABLE IF NOT EXISTS emp_cdc (
    cdc_id SERIAL PRIMARY KEY,
    emp_id INT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT,
    action VARCHAR(100),
    change_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Create function (run on source_db)
CREATE OR REPLACE FUNCTION log_employee_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'DELETE') THEN
        INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, salary, action)
        VALUES (OLD.emp_id, OLD.first_name, OLD.last_name, OLD.dob, OLD.city, OLD.salary, 'DELETE');
        RETURN OLD;
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, salary, action)
        VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, NEW.salary, 'UPDATE');
        RETURN NEW;
    ELSIF (TG_OP = 'INSERT') THEN
        INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, salary, action)
        VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, NEW.salary, 'INSERT');
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- 4. Create trigger (run on source_db)
CREATE TRIGGER employee_changes_trigger
AFTER INSERT OR UPDATE OR DELETE ON employees
FOR EACH ROW EXECUTE FUNCTION log_employee_changes();

-- 5. Create destination table (run on dest_db)
CREATE TABLE IF NOT EXISTS employees (
    emp_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT
);

-- 6. Test with sample data (run on source_db)
INSERT INTO employees (first_name, last_name, dob, city, salary) VALUES
('John', 'Doe', '1990-05-15', 'New York', 75000),
('Jane', 'Smith', '1985-08-20', 'Los Angeles', 85000);

-- 7. Verify CDC table (run on source_db)
SELECT * FROM emp_cdc;

-- 8. Check destination (run on dest_db)
SELECT * FROM employees;