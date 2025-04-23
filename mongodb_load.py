import pandas as pd
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['university_db']
collection = db['student_course_grades']

# Read CSV file
csv_file = 'student_course_grades.csv'
data = pd.read_csv(csv_file)

# Convert to list of dictionaries
records = data.to_dict('records')

# Insert into MongoDB
collection.insert_many(records)

print("Data inserted into MongoDB successfully!")