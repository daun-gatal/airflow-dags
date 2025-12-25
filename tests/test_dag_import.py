import os
import pytest
from airflow.models import DagBag

def test_no_import_errors(airflow_db):
    """
    Verify that all DAGs in the dags/ directory can be imported without errors.
    """
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dag_folder = os.path.join(project_root, "dags")
    
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    
    # Print errors for easier debugging in CI logs
    if dagbag.import_errors:
        print("\nFATAL: DAG import errors found:")
        for filename, stacktrace in dagbag.import_errors.items():
            print(f"File: {filename}\nError: {stacktrace}\n{'-'*50}")
            
    assert len(dagbag.import_errors) == 0, f"Found {len(dagbag.import_errors)} DAG import errors"
