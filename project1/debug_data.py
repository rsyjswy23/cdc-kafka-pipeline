import pandas as pd

df = pd.read_csv('data/Employee_Salaries.csv')
print(f"Total records: {len(df)}")
print(f"\nDepartments in file:")
print(df['Department'].value_counts())
print(f"\nSalary range: ${df['Salary'].min():,.2f} - ${df['Salary'].max():,.2f}")
print(f"Total of all salaries: ${df['Salary'].sum():,.2f}")