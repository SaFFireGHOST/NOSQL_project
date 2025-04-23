import pandas as pd
import mysql.connector

# Connect to MySQL
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='admin',
    database='university_db'
)
cursor = conn.cursor()

# create database university_db in mysql terminal


cursor.execute("USE university_db")

# Create MySQL table
create_table_query = """
CREATE TABLE IF NOT EXISTS student_course_grades (
    student_id VARCHAR(50),
    course_id VARCHAR(50),
    roll_no VARCHAR(50),
    email_id VARCHAR(100),
    grade CHAR(2),
    PRIMARY KEY (student_id, course_id)
)
"""
cursor.execute(create_table_query)

# Read CSV file
csv_file = 'student_course_grades.csv'
data = pd.read_csv(csv_file)

# Insert data into MySQL
insert_query = """
INSERT INTO student_course_grades (student_id, course_id, roll_no, email_id, grade)
VALUES (%s, %s, %s, %s, %s)
"""
for _, row in data.iterrows():
    cursor.execute(insert_query, (
        row['student-ID'],
        row['course-id'],
        row['roll no'],
        row['email ID'],
        row['grade']
    ))

# Commit and close
conn.commit()
cursor.close()
conn.close()

print("Data loaded into MySQL successfully!")