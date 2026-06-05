-- Test 1: Insert with first_name as NULL (first_name has no default)
INSERT INTO employees (last_name, city, salary) 
VALUES ('NoFirstName', 'Boston', 50000);

-- Test 2: Insert with invalid salary type
-- salary should be integer, but we send string
INSERT INTO employees (first_name, last_name, city, salary) 
VALUES ('Wrong', 'Type', 'Chicago', 'NOT_A_NUMBER');

-- Test 3: Invalid Enum Value
-- This creates a CDC record, then we manually set invalid action
INSERT INTO employees (first_name, last_name, city, salary) 
VALUES ('Bad', 'Action', 'Seattle', 60000);

-- Test 4: Extra Field (Should FAIL)
INSERT INTO employees (first_name, last_name, city, salary, age) 
VALUES (NULL, 'NullFirstName', 'Denver', 70000, 30);