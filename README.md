# Database Usage
## 1. Package Requirements
```python
# Install psycopg library for PostgreSQL database interaction
pip install psycopg
```
## 2. Import 
```python
import sys 
import os 

db_module_path = "/home/shared/database" 
if db_module_path not in sys.path:
    sys.path.append(db_module_path)

from patient_db import PatientDatabase
```
## 3. Config & Initialization
```python
db_connection_params = { 
    "dbname": "patient_records", 
    "user": "your_db_user_name", 
    "password": "your_password", 
    "host": "localhost", 
    "port": "5432" 
} 
# Default to store last 10 updates
db = PatientDatabase(db_connection_params, history_limit=10)
```
## 4. Usage 
```python
#Update a Patient's DHP File
patient_name = db.push_dhp(dhp_data)

#Update a Patient's Plan Status 
db.push_plan_status(patient_name, plan_data)

#Get most recent DHP
dhp_data = db.get_dhp(patient_name)

#Get most recent plan status
plan_data = db.get_plan_status(patient_name)

#Rolling Back a Patient's DHP Update
#Defualt is one step, cannot exceed the max number of updates stored 
db.rollback_dhp(patient_name, steps=1) 

#Rolling Back a Patient's Plan Update
#Defualt is one step, cannot exceed the max number of updates stored 
db.rollback_plan(patient_name, steps=1) 

#Exporting DHP and Plan Status to JSON 
db.export_dhp_to_json(patient_name, "output/Patient_Profile.json") 
db.export_plan_status_to_json(patient_name, "output/Patient_Plan.json")
```
## 5. Closing DB 
```python
db.close()
```