import os
import pytest
import json
from airflow.models import Connection, Variable
from airflow.utils.session import create_session

@pytest.fixture(scope="session")
def airflow_db():
    """
    Initialize Airflow DB and load variables/connections from etc/
    """
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.environ["AIRFLOW_HOME"] = project_root
    
    # Initialize DB (SQLite by default for tests if not configured otherwise)
    os.system("airflow db migrate")

    # Load Variables
    var_path = os.path.join(project_root, "etc", "variables", "variables.json")
    if os.path.exists(var_path):
        with open(var_path, 'r') as f:
            vars_dict = json.load(f)
            with create_session() as session:
                for key, val in vars_dict.items():
                    # If value is string, save as is. If dict/list, json serialize it.
                    val_to_save = val if isinstance(val, str) else json.dumps(val)
                    Variable.set(key, val_to_save, session=session)

    # Load Connections
    conn_path = os.path.join(project_root, "etc", "connections", "connections.json")
    if os.path.exists(conn_path):
        with open(conn_path, 'r') as f:
            conns_dict = json.load(f)
            with create_session() as session:
                for conn_id, conn_data in conns_dict.items():
                    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
                    if existing_conn:
                        session.delete(existing_conn)
                    
                    new_conn = Connection(
                        conn_id=conn_id,
                        conn_type=conn_data.get("conn_type"),
                        host=conn_data.get("host"),
                        schema=conn_data.get("schema"),
                        login=conn_data.get("login"),
                        password=conn_data.get("password"),
                        port=conn_data.get("port"),
                        extra=conn_data.get("extra")
                    )
                    session.add(new_conn)
                session.commit()
    
    yield
