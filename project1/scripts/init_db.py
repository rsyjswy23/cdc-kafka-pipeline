import psycopg2

# Connection parameters
conn_params = {
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "user": "postgres",
    "password": "postgres"
}

def create_tables():
    """Create the required tables for the assignment"""
    
    # Table for individual employee records
    create_employee_table = """
    CREATE TABLE IF NOT EXISTS department_employee(
        id SERIAL PRIMARY KEY,
        department VARCHAR(200),
        department_division VARCHAR(200),
        position_title VARCHAR(200),
        hire_date DATE,
        salary DECIMAL
    );
    """
    
    # Table for department salary aggregates
    create_salary_table = """
    CREATE TABLE IF NOT EXISTS department_employee_salary (
        department VARCHAR(200) PRIMARY KEY,
        total_salary BIGINT
    );
    """
    
    # Clear existing data (optional - for clean runs)
    truncate_employee = "TRUNCATE TABLE department_employee RESTART IDENTITY CASCADE;"
    truncate_salary = "TRUNCATE TABLE department_employee_salary CASCADE;"
    
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        
        # Drop existing tables if you want a fresh start (optional)
        # cur.execute("DROP TABLE IF EXISTS department_employee CASCADE;")
        # cur.execute("DROP TABLE IF EXISTS department_employee_salary CASCADE;")
        
        # Create tables
        cur.execute(create_employee_table)
        cur.execute(create_salary_table)
        
        # Clear existing data
        cur.execute(truncate_employee)
        cur.execute(truncate_salary)
        
        conn.commit()
        print("✅ Tables created/truncated successfully!")
        print("   - department_employee")
        print("   - department_employee_salary")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    create_tables()