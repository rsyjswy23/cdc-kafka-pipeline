CREATE TABLE IF NOT EXISTS employees (
    emp_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT
);

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

CREATE TRIGGER employee_changes_trigger
AFTER INSERT OR UPDATE OR DELETE ON employees
FOR EACH ROW EXECUTE FUNCTION log_employee_changes();


-- Insert multiple employees
INSERT INTO employees (first_name, last_name, dob, city, salary) VALUES
('John', 'Doe', '1990-05-15', 'New York', 75000),
('Jane', 'Smith', '1985-08-20', 'Los Angeles', 85000),
('Bob', 'Johnson', '1992-11-10', 'Chicago', 65000),
('Alice', 'Williams', '1988-03-25', 'Houston', 70000),
('Charlie', 'Brown', '1995-07-30', 'Boston', 72000);

-- Verify the data was inserted
SELECT * FROM employees;


-- See what CDC records were created
SELECT cdc_id, emp_id, first_name, action, change_time 
FROM emp_cdc 
ORDER BY cdc_id DESC;



-- Update John's salary
UPDATE employees SET salary = 80000 WHERE first_name = 'John';

-- Update Jane's city
UPDATE employees SET city = 'San Francisco' WHERE first_name = 'Jane';

-- Delete Bob
DELETE FROM employees WHERE first_name = 'Bob';

-- Check CDC again - should show UPDATE records
SELECT cdc_id, emp_id, first_name, action, change_time 
FROM emp_cdc 
ORDER BY cdc_id DESC;

-- Comprehensive view of all changes
SELECT 
    cdc_id,
    emp_id,
    first_name,
    last_name,
    action,
    salary,
    change_time
FROM emp_cdc 
ORDER BY cdc_id;

SELECT COUNT(*) as total_employees FROM employees;


-- Current employees in source
SELECT 'SOURCE' as database, emp_id, first_name, last_name, salary 
FROM employees

UNION ALL

-- Current employees in destination (run on port 5434)
SELECT 'DESTINATION' as database, emp_id, first_name, last_name, salary 
FROM employees;



-- Insert a new employee on source
INSERT INTO employees (first_name, last_name, dob, city, salary) 
VALUES ('Sync', 'Test', '1995-01-01', 'Test City', 99999);