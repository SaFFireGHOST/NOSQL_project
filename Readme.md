# Data Synchronization Across Heterogeneous Systems

This project consists of Python scripts to load student course grades from a CSV file into MongoDB, MySQL, and Apache Hive, and to query, update, and synchronize grades across these databases. The scripts include `hive_load.py`, `mongo_load.py`, `mysql_load.py` for data loading, and `main_v4.py` for querying, updating, and merging data using operation logs.

---

## Table of Contents

1. [Scripts and Features](#scripts-and-features)
2. [CSV File Format](#csv-file-format)
3. [Installation and Setup](#installation-and-setup)
   - [Prerequisites](#prerequisites)
   - [Step 1: Install Dependencies](#step-1-install-dependencies)
   - [Step 2: Configure Databases](#step-2-configure-databases)
   - [Step 3: Prepare the CSV File](#step-3-prepare-the-csv-file)
   - [Step 4: Prepare Log Files](#step-4-prepare-log-files)
   - [Step 5: Run the Scripts](#step-5-run-the-scripts)
4. [Usage Examples](#usage-examples)
5. [Troubleshooting](#troubleshooting)
6. [Notes](#notes)

---

## Scripts and Features

### 1. `hive_load.py`

**Purpose:** Loads student course grades from `student_course_grades.csv` into an Apache Hive table.

**Features:**

- Copies the CSV file to HDFS: `hdfs dfs -put student_course_grades.csv /user/hive/data/`.
- Creates a Hive table `student_course_grades` with columns: `student_id`, `course_id`, `roll_no`, `email_id`, `grade`.
- Loads the CSV data into the Hive table, skipping the header row.

**Output:** Prints `"Data loaded into Hive successfully!"` on completion.

**Dependencies:** `pandas`, `pyhive`, `thrift`

---

### 2. `mongo_load.py`

**Purpose:** Imports student course grades from `student_course_grades.csv` into a MongoDB collection.

**Features:**

- Connects to MongoDB at `localhost:27017`.
- Reads the CSV with pandas and converts it to a list of dictionaries for bulk insertion.
- Inserts data into the `university_db.student_course_grades` collection.

**Output:** Prints `"Data inserted into MongoDB successfully!"` on completion.

**Dependencies:** `pandas`, `pymongo`

---

### 3. `mysql_load.py`

**Purpose:** Loads student course grades from `student_course_grades.csv` into a MySQL table.

**Features:**

- Connects to MySQL at `localhost` with user `root` and password `admin`.
- Creates a `student_course_grades` table in the `university_db` database with columns: `student_id`, `course_id`, `roll_no`, `email_id`, `grade`.
- Inserts CSV data row-by-row using parameterized queries.

**Output:** Prints `"Data loaded into MySQL successfully!"` on completion.

**Dependencies:** `pandas`, `mysql-connector-python`

---

### 4. `main_v4.py`

**Purpose:** Queries, updates, and synchronizes student course grades across MongoDB, MySQL, and Hive.

**Supported Databases:**

- **MongoDB:** `university_db.student_course_grades` collection with keys `student-ID`, `course-id`, `roll no`, `email ID`, `grade`.
- **MySQL:** `university_db.student_course_grades` table with columns `student_id`, `course_id`, `roll_no`, `email_id`, `grade`.
- **Hive:** `default.student_course_grades` table with the same schema as MySQL.

**Operations:**

- **GET:** Retrieve a record by `student_id` and `course_id`.
- **SET:** Update the `grade` for a given `student_id` and `course_id`.
- **MERGE:** Synchronize local database state with a remote database’s state using operation logs.

**Operation Logs:**

- Files: `mongo_operations.log`, `mysql_operations.log`, `hive_operations.log`.
- Format:
  - `GET: YYYY-MM-DD HH:MM:SS - GET (student_id, course_id)`
  - `SET: YYYY-MM-DD HH:MM:SS - SET ((student_id, course_id), grade)`
- `MERGE` reads SET operations from the remote log and applies them locally.

**Merge Logic:**

- `merge_logs` extracts latest remote SETs, ignores local log updates.
- `merge_mongo`, `merge_mysql`, `merge_hive` apply updates.

**Error Handling:** Manages missing logs, invalid inputs, and connection errors.

**Debugging:** Outputs count of SET operations found and merged.

**Dependencies:** `pandas`, `pymongo`, `mysql-connector-python`, `pyhive`, `thrift`, `python-dateutil`

---

## CSV File Format

**File:** `student_course_grades.csv`

**Columns:**

| Column (MongoDB) | Column (MySQL/Hive) | Description                             |
| ---------------- | ------------------- | --------------------------------------- |
| `student-ID`     | `student_id`        | Student identifier (e.g., `SID1033`)    |
| `course-id`      | `course_id`         | Course identifier (e.g., `CSE016`)      |
| `roll no`        | `roll_no`           | Roll number (e.g., `CRPC2ZW9`)          |
| `email ID`       | `email_id`          | Email (e.g., `crpc2zw9@university.edu`) |
| `grade`          | `grade`             | Course grade (e.g., `A`, `B`)           |

> **Note:** MongoDB uses hyphenated names; MySQL/Hive use underscores.

---

## Installation and Setup

### Prerequisites

- **OS:** Ubuntu (Linux) or compatible.
- **MongoDB:** Running on `localhost:27017`.
- **MySQL:** Running on `localhost` (user `root`, password `admin`).
- **Apache Hive:** HiveServer2 on `localhost:10000`.
- **HDFS:** Required for `hive_load.py`.
- **Python:** Version 3.6 or higher.

### Step 1: Install Dependencies

```bash
pip install pandas pymongo mysql-connector-python pyhive thrift python-dateutil
```

### Step 2: Configure Databases

**MongoDB:**

```bash
mongod --dbpath /path/to/db
```

*(**`mongo_load.py`** creates **`university_db`** and the collection automatically.)*

**MySQL:**

```bash
sudo systemctl start mysql
# In MySQL terminal:
CREATE DATABASE university_db;
```

*Ensure **`root`** user has password **`admin`**.*

**Hive & HDFS:**

```bash
start-dfs.sh
hive --service hiveserver2 &
hdfs dfs -mkdir -p /user/hive/data/
```

*(**`hive_load.py`** creates the table in the default database.)*

### Step 3: Prepare the CSV File

- Place `student_course_grades.csv` in the project directory (e.g., `/home/koushik/Desktop/NOSQL_project/`).
- Ensure headers match: `student-ID,course-id,roll no,email ID,grade`.

**Example:**

```csv
student-ID,course-id,roll no,email ID,grade
SID1033,CSE016,CRPC2ZW9,crpc2zw9@university.edu,A
SID1310,CSE020,XYZ12345,xyz12345@university.edu,B
```

### Step 4: Prepare Log Files

```bash
touch /home/koushik/Desktop/NOSQL_project/*.log
chmod 644 /home/koushik/Desktop/NOSQL_project/*.log
```

*Creates **`mongo_operations.log`**, **`mysql_operations.log`**, **`hive_operations.log`**.*

### Step 5: Run the Scripts

**Load Data:**

```bash
python hive_load.py
python mongo_load.py
python mysql_load.py
```

**Expected Outputs:**

- `hive_load.py`: Data loaded into Hive successfully!
- `mongo_load.py`: Data inserted into MongoDB successfully!
- `mysql_load.py`: Data loaded into MySQL successfully!

**Query, Update, and Merge:**

```bash
python main_v4.py
```

Follow prompts for GET, SET, or MERGE.

---

## Usage Examples

**Load into Hive:**

```bash
python hive_load.py
# Verify in Hive:
SELECT * FROM student_course_grades;
```

**Load into MongoDB:**

```bash
python mongo_load.py
# Verify:
use university_db
db.student_course_grades.find().pretty()
```

**Load into MySQL:**

```bash
python mysql_load.py
# Verify:
SELECT * FROM university_db.student_course_grades;
```

**GET a Record:**

1. Run `main_v4.py` → choose GET → MongoDB
2. Enter `student-ID: SID1033`, `course-id: CSE016`

*Output:*

```
MongoDB Result: { student-ID: 'SID1033', course-id: 'CSE016', roll no: 'CRPC2ZW9', email ID: 'crpc2zw9@university.edu', grade: 'A' }
```

*Log:*

```
2025-04-21 12:34:56 - GET (SID1033, CSE016)
```

**SET a Grade:**

1. Run `main_v4.py` → choose SET → Hive
2. Enter `SID1033`, `CSE016`, new grade `B`

*Output:*

```
Grade updated to B in Hive for student_id: SID1033, course_id: CSE016
```

*Log:*

```
2025-04-21 12:34:56 - SET ((SID1033, CSE016), B)
```

**MERGE Databases:**

1. Run `main_v4.py` → MERGE → from Hive → into MongoDB
2. Applies SET records from `hive_operations.log` to MongoDB

*Output:*

```
Merged 2 records into MongoDB from Hive.
Debug: Found 0 SET operations in local log
Debug: Found 2 SET operations in remote log
```

---

## Troubleshooting


- **Connection errors:** Ensure MongoDB, MySQL, Hive services are running and ports/credentials are correct.
- **Log format issues:** Ensure logs match the regex: `YYYY-MM-DD HH:MM:SS - (GET|SET) ...`.
- **CSV issues:** Confirm correct headers and file path.

---

## Notes

- **Performance:** Hive loads and MERGE using `INSERT OVERWRITE` can be slow for large datasets. Consider using partitioning or ORC/Parquet formats in production.
- **Log Format:** Must exactly match `YYYY-MM-DD HH:MM:SS - SET ((student_id, course_id), grade)`. Provide sample logs if parsing errors occur.


