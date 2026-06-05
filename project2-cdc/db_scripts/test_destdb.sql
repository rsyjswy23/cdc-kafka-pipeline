CREATE TABLE IF NOT EXISTS employees (
    emp_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT
);

SELECT current_database();
SELECT table_name FROM information_schema.tables WHERE table_name = 'employees';


-- Check destination table
SELECT * FROM employees ORDER BY emp_id;

-- Count records
SELECT COUNT(*) as total_employees FROM employees;

-- See department stats (if you have city as department)
SELECT city, COUNT(*) as emp_count, SUM(salary) as total_salary
FROM employees
GROUP BY city
ORDER BY total_salary DESC;