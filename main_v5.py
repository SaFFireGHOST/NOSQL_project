import pandas as pd
from pymongo import MongoClient
import mysql.connector
from pyhive import hive
from datetime import datetime
import os
import re
from dateutil.parser import parse

# Log file paths
MONGO_LOG = 'mongo_operations.log'
MYSQL_LOG = 'mysql_operations.log'
HIVE_LOG = 'hive_operations.log'

def log_operation(log_file, operation, student_id, course_id, grade=None):
    """Log the GET or SET operation with timestamp to the specified log file."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if operation == 'GET':
        log_entry = f"{timestamp} - GET ({student_id}, {course_id})\n"
    else:  # SET
        log_entry = f"{timestamp} - SET (({student_id}, {course_id}), {grade})\n"
    with open(log_file, 'a') as f:
        f.write(log_entry)

def complete_log_operation(log_file, operation, student_id, course_id, timestamp, grade=None):
    """Log the GET or SET operation with provided timestamp to the specified log file."""
    timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
    if operation == 'GET':
        log_entry = f"{timestamp_str} - GET ({student_id}, {course_id})\n"
    else:  # SET
        log_entry = f"{timestamp_str} - SET (({student_id}, {course_id}), {grade})\n"
    with open(log_file, 'a') as f:
        f.write(log_entry)

def merge_logs(local_log_file, remote_log_file):
    """Parse local and remote log files and return a hashmap of latest SET updates from remote log only."""
    latest_updates = {}
    local_set_count = 0
    remote_set_count = 0
    
    # Regex for SET operations: YYYY-MM-DD HH:MM:SS - SET ((student_id, course_id), grade)
    set_pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - SET \(\(([^,]+), ([^)]+)\), ([^\)]+)\)'
    
    # Process local log file
    if os.path.exists(local_log_file):
        with open(local_log_file, 'r') as f:
            for line in f:
                match = re.match(set_pattern, line.strip())
                if match:
                    timestamp_str, student_id, course_id, grade = match.groups()
                    timestamp = parse(timestamp_str)
                    key = (student_id, course_id)
                    local_set_count += 1
                    
                    # Store with source 'local'
                    if key not in latest_updates or timestamp > latest_updates[key][0]:
                        latest_updates[key] = (timestamp, grade, 'local')
    else:
        print(f"Warning: Local log file {local_log_file} does not exist.")
    
    # Process remote log file
    if os.path.exists(remote_log_file):
        with open(remote_log_file, 'r') as f:
            for line in f:
                match = re.match(set_pattern, line.strip())
                if match:
                    timestamp_str, student_id, course_id, grade = match.groups()
                    timestamp = parse(timestamp_str)
                    key = (student_id, course_id)
                    remote_set_count += 1
                    
                    # Store with source 'remote' if newer or no existing entry
                    if key not in latest_updates or timestamp > latest_updates[key][0]:
                        latest_updates[key] = (timestamp, grade, 'remote')
    else:
        print(f"Warning: Remote log file {remote_log_file} does not exist.")
    
    # Convert to final hashmap: (student_id, course_id) -> (grade, timestamp), only for remote updates
    result = {key: (grade, timestamp) for key, (timestamp, grade, source) in latest_updates.items() if source == 'remote'}
    
    # Debug output
    print(f"Debug: Found {local_set_count} SET operations in local log ({local_log_file})")
    print(f"Debug: Found {remote_set_count} SET operations in remote log ({remote_log_file})")
    print(f"Debug: Merged {len(result)} remote updates")
    
    return result

def get_mongo(student_id, course_id):
    """Retrieve a row from MongoDB based on student-ID and course-id."""
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client['university_db']
        collection = db['student_course_grades']
        
        query = {'student-ID': student_id, 'course-id': course_id}
        result = collection.find_one(query)
        
        if result:
            # Remove MongoDB's _id field for cleaner output
            result.pop('_id', None)
            print("MongoDB Result:", result)
        else:
            print(f"No record found in MongoDB for student-ID: {student_id}, course-id: {course_id}")
        
        log_operation(MONGO_LOG, 'GET', student_id, course_id)
        return result
    
    except Exception as e:
        print(f"MongoDB Error: {e}")
        return None
    finally:
        client.close()

def set_mongo(student_id, course_id, new_grade):
    """Update the grade in MongoDB for the given student-ID and course-id."""
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client['university_db']
        collection = db['student_course_grades']
        
        query = {'student-ID': student_id, 'course-id': course_id}
        update = {'$set': {'grade': new_grade}}
        result = collection.update_one(query, update)
        
        if result.matched_count > 0:
            print(f"Grade updated to {new_grade} in MongoDB for student-ID: {student_id}, course-id: {course_id}")
        else:
            print(f"No record found in MongoDB for student-ID: {student_id}, course-id: {course_id}")
        
        log_operation(MONGO_LOG, 'SET', student_id, course_id, new_grade)
        return result.matched_count > 0
    
    except Exception as e:
        print(f"MongoDB Error: {e}")
        return False
    finally:
        client.close()

def merge_mongo(database_system):
    """Merge MongoDB with the state of another system based on operation logs."""
    client = None
    try:
        # Map database system to log file
        log_map = {
            'MongoDB': MONGO_LOG,
            'MySQL': MYSQL_LOG,
            'Hive': HIVE_LOG
        }
        
        if database_system not in log_map:
            print(f"Invalid database system: {database_system}. Choose MongoDB, MySQL, or Hive.")
            return False
        
        if database_system == 'MongoDB':
            print("Cannot merge MongoDB with itself.")
            return False
        
        local_log = MONGO_LOG
        remote_log = log_map[database_system]
        
        # Get merged updates
        updates = merge_logs(local_log, remote_log)
        
        if not updates:
            print("No updates to merge.")
            return False
        
        # Connect to MongoDB
        client = MongoClient('mongodb://localhost:27017/')
        db = client['university_db']
        collection = db['student_course_grades']
        
        # Apply updates
        updated_count = 0
        for (student_id, course_id), (grade, timestamp) in updates.items():
            query = {'student-ID': student_id, 'course-id': course_id}
            update = {'$set': {'grade': grade}}
            result = collection.update_one(query, update)
            if result.matched_count > 0:
                updated_count += 1
                complete_log_operation(MONGO_LOG, 'SET', student_id, course_id, timestamp, grade)
        
        print(f"Merged {updated_count} records into MongoDB from {database_system}.")
        return updated_count > 0
    
    except Exception as e:
        print(f"MongoDB Merge Error: {e}")
        return False
    finally:
        if client is not None:
            client.close()

def get_mysql(student_id, course_id):
    """Retrieve a row from MySQL based on student_id and course_id."""
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='admin',
            database='university_db'
        )
        cursor = conn.cursor(dictionary=True)
        
        query = """
        SELECT student_id, course_id, roll_no, email_id, grade
        FROM student_course_grades
        WHERE student_id = %s AND course_id = %s
        """
        cursor.execute(query, (student_id, course_id))
        result = cursor.fetchone()
        
        if result:
            print("MySQL Result:", result)
        else:
            print(f"No record found in MySQL for student_id: {student_id}, course_id: {course_id}")
        
        log_operation(MYSQL_LOG, 'GET', student_id, course_id)
        return result
    
    except Exception as e:
        print(f"MySQL Error: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

def set_mysql(student_id, course_id, new_grade):
    """Update the grade in MySQL for the given student_id and course_id."""
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='admin',
            database='university_db'
        )
        cursor = conn.cursor()
        
        query = """
        UPDATE student_course_grades
        SET grade = %s
        WHERE student_id = %s AND course_id = %s
        """
        cursor.execute(query, (new_grade, student_id, course_id))
        conn.commit()
        
        if cursor.rowcount > 0:
            print(f"Grade updated to {new_grade} in MySQL for student_id: {student_id}, course_id: {course_id}")
        else:
            print(f"No record found in MySQL for student_id: {student_id}, course_id: {course_id}")
        
        log_operation(MYSQL_LOG, 'SET', student_id, course_id, new_grade)
        return cursor.rowcount > 0
    
    except Exception as e:
        print(f"MySQL Error: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

def merge_mysql(database_system):
    """Merge MySQL with the state of another system based on operation logs."""
    conn = None
    cursor = None
    try:
        # Map database system to log file
        log_map = {
            'MongoDB': MONGO_LOG,
            'MySQL': MYSQL_LOG,
            'Hive': HIVE_LOG
        }
        
        if database_system not in log_map:
            print(f"Invalid database system: {database_system}. Choose MongoDB, MySQL, or Hive.")
            return False
        
        if database_system == 'MySQL':
            print("Cannot merge MySQL with itself.")
            return False
        
        local_log = MYSQL_LOG
        remote_log = log_map[database_system]
        
        # Get merged updates
        updates = merge_logs(local_log, remote_log)
        
        if not updates:
            print("No updates to merge.")
            return False
        
        # Connect to MySQL
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='admin',
            database='university_db'
        )
        cursor = conn.cursor()
        
        # Apply updates
        updated_count = 0
        query = """
        UPDATE student_course_grades
        SET grade = %s
        WHERE student_id = %s AND course_id = %s
        """
        for (student_id, course_id), (grade, timestamp) in updates.items():
            cursor.execute(query, (grade, student_id, course_id))
            if cursor.rowcount > 0:
                updated_count += 1
                complete_log_operation(MYSQL_LOG, 'SET', student_id, course_id, timestamp, grade)
        
        conn.commit()
        print(f"Merged {updated_count} records into MySQL from {database_system}.")
        return updated_count > 0
    
    except Exception as e:
        print(f"MySQL Merge Error: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

def get_hive(student_id, course_id):
    """Retrieve a row from Hive based on student_id and course_id."""
    try:
        conn = hive.connect(host='localhost', port=10000, database='default')
        cursor = conn.cursor()
        
        query = """
        SELECT student_id, course_id, roll_no, email_id, grade
        FROM student_course_grades
        WHERE student_id = '%s' AND course_id = '%s'
        """
        cursor.execute(query % (student_id, course_id))
        result = cursor.fetchone()
        
        if result:
            # Convert tuple to dict for consistent output
            result_dict = {
                'student_id': result[0],
                'course_id': result[1],
                'roll_no': result[2],
                'email_id': result[3],
                'grade': result[4]
            }
            print("Hive Result:", result_dict)
        else:
            print(f"No record found in Hive for student_id: {student_id}, course_id: {course_id}")
        
        log_operation(HIVE_LOG, 'GET', student_id, course_id)
        return result_dict
    
    except Exception as e:
        print(f"Hive Error: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def set_hive(student_id, course_id, new_grade):
    """Update the grade in Hive for the given student_id and course_id."""
    try:
        conn = hive.connect(host='localhost', port=10000, database='default')
        cursor = conn.cursor()
        
        query = """
        INSERT OVERWRITE TABLE student_course_grades
        SELECT student_id, course_id, roll_no, email_id, 
               CASE WHEN student_id = '%s' AND course_id = '%s' THEN '%s' ELSE grade END
        FROM student_course_grades
        """
        cursor.execute(query % (student_id, course_id, new_grade))
        conn.commit()
        
        # Verify update
        cursor.execute("""
        SELECT COUNT(*) 
        FROM student_course_grades 
        WHERE student_id = '%s' AND course_id = '%s' AND grade = '%s'
        """ % (student_id, course_id, new_grade))
        updated = cursor.fetchone()[0]
        
        if updated > 0:
            print(f"Grade updated to {new_grade} in Hive for student_id: {student_id}, course_id: {course_id}")
        else:
            print(f"No record found in Hive for student_id: {student_id}, course_id: {course_id}")
        
        log_operation(HIVE_LOG, 'SET', student_id, course_id, new_grade)
        return updated > 0
    
    except Exception as e:
        print(f"Hive Error: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def merge_hive(database_system):
    """Merge Hive with the state of another system based on operation logs."""
    conn = None
    cursor = None
    try:
        # Map database system to log file
        log_map = {
            'MongoDB': MONGO_LOG,
            'MySQL': MYSQL_LOG,
            'Hive': HIVE_LOG
        }
        
        if database_system not in log_map:
            print(f"Invalid database system: {database_system}. Choose MongoDB, MySQL, or Hive.")
            return False
        
        if database_system == 'Hive':
            print("Cannot merge Hive with itself.")
            return False
        
        local_log = HIVE_LOG
        remote_log = log_map[database_system]
        
        # Get merged updates
        updates = merge_logs(local_log, remote_log)
        
        if not updates:
            print("No updates to merge.")
            return False
        
        # Connect to Hive
        conn = hive.connect(host='localhost', port=10000, database='default')
        cursor = conn.cursor()
        
        # Apply updates
        updated_count = 0
        for (student_id, course_id), (grade, timestamp) in updates.items():
            query = """
            INSERT OVERWRITE TABLE student_course_grades
            SELECT student_id, course_id, roll_no, email_id, 
                   CASE WHEN student_id = '%s' AND course_id = '%s' THEN '%s' ELSE grade END
            FROM student_course_grades
            """
            cursor.execute(query % (student_id, course_id, grade))
            conn.commit()
            
            # Verify update
            cursor.execute("""
            SELECT COUNT(*) 
            FROM student_course_grades 
            WHERE student_id = '%s' AND course_id = '%s' AND grade = '%s'
            """ % (student_id, course_id, grade))
            if cursor.fetchone()[0] > 0:
                updated_count += 1
                complete_log_operation(HIVE_LOG, 'SET', student_id, course_id, timestamp, grade)
        
        print(f"Merged {updated_count} records into Hive from {database_system}.")
        return updated_count > 0
    
    except Exception as e:
        print(f"Hive Merge Error: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def main():
    """Main function to handle user input and call the appropriate get, set, or merge function."""
    while True:
        print("\nSelect an operation:")
        print("1. GET (Retrieve record)")
        print("2. SET (Update grade)")
        print("3. MERGE (Merge with another system)")
        print("4. Exit")
        
        op_choice = input("Enter operation (1-4): ").strip()
        
        if op_choice == '4':
            print("Exiting program.")
            break
        
        if op_choice not in ['1', '2', '3']:
            print("Invalid operation. Please select 1, 2, 3, or 4.")
            continue
        
        print("\nSelect a database to query:")
        print("1. MongoDB")
        print("2. MySQL")
        print("3. Hive")
        
        choice = input("Enter choice (1-3): ").strip()
        
        if choice not in ['1', '2', '3']:
            print("Invalid choice. Please select 1, 2, or 3.")
            continue
        
        if op_choice in ['1', '2']:
            student_id = input("Enter student-ID (e.g., SID1033): ").strip()
            course_id = input("Enter course-id (e.g., CSE016): ").strip()
            
            if not student_id or not course_id:
                print("Error: student-ID and course-id cannot be empty.")
                continue
            
            if op_choice == '2':
                new_grade = input("Enter new grade (e.g., A): ").strip()
                if not new_grade:
                    print("Error: New grade cannot be empty.")
                    continue
        
        if op_choice == '3':
            print("\nSelect a database to merge with:")
            print("1. MongoDB")
            print("2. MySQL")
            print("3. Hive")
            merge_choice = input("Enter choice (1-3): ").strip()
            
            if merge_choice not in ['1', '2', '3']:
                print("Invalid merge choice. Please select 1, 2, or 3.")
                continue
            
            # Map choice to database system
            db_map = {'1': 'MongoDB', '2': 'MySQL', '3': 'Hive'}
            merge_system = db_map[merge_choice]
            
            if choice == '1':  # MongoDB
                merge_mongo(merge_system)
            elif choice == '2':  # MySQL
                merge_mysql(merge_system)
            elif choice == '3':  # Hive
                merge_hive(merge_system)
        
        else:
            if choice == '1':  # MongoDB
                if op_choice == '1':
                    get_mongo(student_id, course_id)
                else:
                    set_mongo(student_id, course_id, new_grade)
            elif choice == '2':  # MySQL
                if op_choice == '1':
                    get_mysql(student_id, course_id)
                else:
                    set_mysql(student_id, course_id, new_grade)
            elif choice == '3':  # Hive
                if op_choice == '1':
                    get_hive(student_id, course_id)
                else:
                    set_hive(student_id, course_id, new_grade)

if __name__ == "__main__":
    main()