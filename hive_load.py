import pandas as pd
from pyhive import hive
from subprocess import call

# Copy CSV to HDFS
hdfs_path = '/user/hive/data/student_course_grades.csv'
call(['hdfs', 'dfs', '-put', '-f', 'student_course_grades.csv', hdfs_path])

# Connect to Hive
conn = hive.connect(host='localhost', port=10000, database='default')
cursor = conn.cursor()

# Create Hive table
create_table_query = """
CREATE TABLE IF NOT EXISTS student_course_grades (
    student_id STRING,
    course_id STRING,
    roll_no STRING,
    email_id STRING,
    grade STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1')
"""
cursor.execute(create_table_query)

# Load data into Hive
load_data_query = f"LOAD DATA INPATH '{hdfs_path}' OVERWRITE INTO TABLE student_course_grades"
cursor.execute(load_data_query)

# Commit and close
conn.commit()
cursor.close()
conn.close()

print("Data loaded into Hive successfully!")