-- Run on source_db (port 5433)
INSERT INTO employees (first_name, last_name, dob, city, salary) VALUES
('John', 'Doe', '1990-05-15', 'New York', 75000),
('Jane', 'Smith', '1985-08-20', 'Los Angeles', 85000);

UPDATE employees SET salary = 80000 WHERE first_name = 'John';
DELETE FROM employees WHERE first_name = 'Jane';

SELECT * FROM emp_cdc;