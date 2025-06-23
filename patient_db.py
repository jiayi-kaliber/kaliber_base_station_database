import psycopg
from psycopg.rows import dict_row
import json
from datetime import datetime
import os
import time

class PatientDatabase:
    """
    A class to manage a PostgreSQL database for storing and updating
    patient DHP and plan snapshot information.
    """

    def __init__(self, db_params, history_limit=10):
        """
        Initializes the database connection and sets the history limit.

        Args:
            db_params (dict): A dictionary of connection parameters for PostgreSQL.
            history_limit (int, optional): The maximum number of plan snapshots
                                           to store per patient. Defaults to 10.
        """
        self.conninfo = " ".join([f"{k}={v}" for k, v in db_params.items()])
        self.conn = None
        self.history_limit = history_limit
        try:
            self.conn = psycopg.connect(self.conninfo, row_factory=dict_row)
        except psycopg.OperationalError as e:
            print(f"Could not connect to the PostgreSQL database: {e}")
            raise

    def _execute_query(self, query, params=None, fetch=None):
        """Helper function to execute queries."""
        with self.conn.cursor() as cursor:
            cursor.execute(query, params or ())
            if fetch == 'one':
                return cursor.fetchone()
            if fetch == 'all':
                return cursor.fetchall()
            self.conn.commit()

    def create_tables(self):
        self._execute_query("""
            CREATE TABLE IF NOT EXISTS patients (
                patient_id SERIAL PRIMARY KEY,
                patient_name TEXT UNIQUE NOT NULL,
                procedure TEXT,
                last_updated TEXT,
                soft_data TEXT,
                current_plan JSONB
            );
        """)
        self._execute_query("""
            CREATE TABLE IF NOT EXISTS plan_history (
                history_id SERIAL PRIMARY KEY,
                patient_name TEXT NOT NULL REFERENCES patients(patient_name) ON DELETE CASCADE,
                plan_snapshot JSONB NOT NULL
            );
        """)
        self._execute_query("""
            CREATE TABLE IF NOT EXISTS dhp_history (
                history_id SERIAL PRIMARY KEY,
                patient_name TEXT NOT NULL REFERENCES patients(patient_name) ON DELETE CASCADE,
                procedure TEXT,
                last_updated TEXT,
                soft_data TEXT
            );
        """)
        print("Tables for DHP and plan snapshot history created or already exist.")

    def push_dhp(self, dhp_data):
        data = dhp_data
        hard_data = data.get("hard", {})
        patient_name = hard_data.get("Patient Alias")

        if not patient_name:
            raise ValueError("DHP data must contain a 'Patient Alias'.")

        params = {
            'alias': patient_name,
            'proc': hard_data.get("Patient's Procedure Performed or Non-Surgical Pathology"),
            'updated': hard_data.get("Time of most recent update"),
            'soft': data.get("soft", "")
        }

        # Insert or update the main patients table
        update_query = """
            INSERT INTO patients (patient_name, procedure, last_updated, soft_data, current_plan)
            VALUES (%(alias)s, %(proc)s, %(updated)s, %(soft)s, '{}'::jsonb)
            ON CONFLICT (patient_name) DO UPDATE SET
                procedure=EXCLUDED.procedure,
                last_updated=EXCLUDED.last_updated,
                soft_data=EXCLUDED.soft_data;
        """
        self._execute_query(update_query, params)
        print(f"Updated DHP for patient '{patient_name}'.")

        # Insert new DHP data into history
        history_query = """
            INSERT INTO dhp_history (patient_name, procedure, last_updated, soft_data)
            VALUES (%s, %s, %s, %s);
        """
        self._execute_query(history_query, (params['alias'], params['proc'], params['updated'], params['soft']))
        print(f"Created new DHP history snapshot for '{patient_name}'.")

        # Trim DHP history
        trim_query = """
            DELETE FROM dhp_history
            WHERE history_id IN (
                SELECT history_id FROM (
                    SELECT history_id,
                           ROW_NUMBER() OVER (ORDER BY history_id DESC) as rn
                    FROM dhp_history
                    WHERE patient_name = %s
                ) as sub
                WHERE rn > %s
            )
        """
        self._execute_query(trim_query, (patient_name, self.history_limit))

        return patient_name

    def push_plan_status(self, patient_name, plan_data):
        plan_snapshot = plan_data

        history_query = """
            INSERT INTO plan_history (patient_name, plan_snapshot)
            VALUES (%s, %s);
        """
        self._execute_query(history_query, (patient_name, json.dumps(plan_snapshot)))
        print(f"Created new plan history snapshot for '{patient_name}'.")

        update_patient_query = """
            UPDATE patients
            SET current_plan = %s
            WHERE patient_name = %s;
        """
        self._execute_query(update_patient_query, (json.dumps(plan_snapshot), patient_name))
        
        trim_query = """
            DELETE FROM plan_history
            WHERE history_id IN (
                SELECT history_id FROM (
                    SELECT history_id,
                           ROW_NUMBER() OVER (ORDER BY history_id DESC) as rn
                    FROM plan_history
                    WHERE patient_name = %s
                ) as sub
                WHERE rn > %s
            )
        """
        self._execute_query(trim_query, (patient_name, self.history_limit))

        print(f"Processed and updated PlanStatus for patient '{patient_name}'.")

    def rollback_dhp(self, patient_name, steps=1):
        if steps <= 0:
            print("Rollback steps must be a positive number.")
            return

        history_query = """
            SELECT history_id, procedure, last_updated, soft_data
            FROM dhp_history
            WHERE patient_name = %s
            ORDER BY history_id DESC
            LIMIT %s;
        """
        history_records = self._execute_query(history_query, (patient_name, steps + 1), fetch='all')

        if len(history_records) <= steps:
            print(f"Cannot roll back DHP by {steps} step(s): Patient '{patient_name}' only has {len(history_records) -1} previous version(s).")
            return

        target_record = history_records[steps]
        records_to_delete = history_records[:steps]
        ids_to_delete = [rec['history_id'] for rec in records_to_delete]

        update_patient_query = """
            UPDATE patients
            SET procedure = %s, last_updated = %s, soft_data = %s
            WHERE patient_name = %s;
        """
        self._execute_query(update_patient_query, (
            target_record['procedure'],
            target_record['last_updated'],
            target_record['soft_data'],
            patient_name
        ))

        delete_query = "DELETE FROM dhp_history WHERE history_id = ANY(%s::int[]);"
        self._execute_query(delete_query, (ids_to_delete,))

        print(f"Successfully rolled back DHP for patient '{patient_name}' by {steps} step(s).")

    def rollback_plan(self, patient_name, steps=1):
        if steps <= 0:
            print("Rollback steps must be a positive number.")
            return

        history_query = """
            SELECT history_id, plan_snapshot
            FROM plan_history
            WHERE patient_name = %s
            ORDER BY history_id DESC
            LIMIT %s;
        """
        history_records = self._execute_query(history_query, (patient_name, steps + 1), fetch='all')

        if len(history_records) <= steps:
            print(f"Cannot roll back plan by {steps} step(s): Patient '{patient_name}' only has {len(history_records) -1} previous version(s).")
            return

        target_record = history_records[steps]
        records_to_delete = history_records[:steps]
        ids_to_delete = [rec['history_id'] for rec in records_to_delete]

        update_patient_query = """
            UPDATE patients
            SET current_plan = %s
            WHERE patient_name = %s;
        """
        self._execute_query(update_patient_query, (
            json.dumps(target_record['plan_snapshot']),
            patient_name
        ))

        delete_query = "DELETE FROM plan_history WHERE history_id = ANY(%s::int[]);"
        self._execute_query(delete_query, (ids_to_delete,))

        print(f"Successfully rolled back plan for patient '{patient_name}' by {steps} step(s).")

    def get_dhp(self, patient_name):
        dhp_data = self._execute_query(
            "SELECT patient_name, procedure, last_updated, soft_data FROM patients WHERE patient_name = %s",
            (patient_name,),
            fetch='one'
        )

        if not dhp_data:
            return None

        output_data = {
            "hard": {
                "Patient Alias": dhp_data['patient_name'],
                "Patient's Procedure Performed or Non-Surgical Pathology": dhp_data['procedure'],
                "Time of most recent update": dhp_data['last_updated']
            },
            "soft": dhp_data['soft_data']
        }
        return output_data

    def get_plan_status(self, patient_name):
        patient_data = self._execute_query(
            "SELECT current_plan FROM patients WHERE patient_name = %s",
            (patient_name,),
            fetch='one'
        )

        if not patient_data or not patient_data['current_plan']:
            return None
        
        return patient_data['current_plan']

    def export_dhp_to_json(self, patient_name, file_path):
        dhp_content = self.get_dhp(patient_name)

        if not dhp_content:
            print(f"Could not export DHP: Patient '{patient_name}' not found.")
            return

        with open(file_path, 'w') as f:
            json.dump(dhp_content, f, indent=2)
        print(f"Successfully exported DHP for '{patient_name}' to '{file_path}'.")

    def export_plan_status_to_json(self, patient_name, file_path):
        plan_status_output = self.get_plan_status(patient_name)

        if not plan_status_output:
            print(f"Could not export plan status: Patient '{patient_name}' not found or has no plan.")
            return

        with open(file_path, 'w') as f:
            json.dump(plan_status_output, f, indent=2)
        print(f"Successfully exported PlanStatus for '{patient_name}' to '{file_path}'.")

    def close(self):
        """Closes the database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()