#===========================================================
# Database Related Functions
#===========================================================

from libsql_client import create_client_sync
from contextlib import contextmanager
from dotenv import load_dotenv
from os import getenv, path, makedirs
from app.helpers.logging import log_db_request, log_db_result, log_sync_result


# Load Turso environment variables from the .env file
load_dotenv()
TURSO_URL = getenv("TURSO_URL")
TURSO_KEY = getenv("TURSO_KEY")

# Define the local database path
LOCAL_DB_PATH = path.join(path.dirname(path.dirname(__file__)), "db", "data.sqlite")


#-----------------------------------------------------------
# Wrapper function to handle syncing with Turso
#-----------------------------------------------------------
def sync_db(client, operation="sync"):
    try:
        client.sync()
        log_sync_result(operation, True)

    except Exception as e:
        log_sync_result(operation, False, str(e))


#-----------------------------------------------------------
# Connect to the local synced DB and return the connection
#-----------------------------------------------------------
@contextmanager
def connect_db():
    from flask import current_app as app
    client = None

    try:
        # Ensure the db directory exists
        db_dir = path.dirname(LOCAL_DB_PATH)
        makedirs(db_dir, exist_ok=True)

        # Create a synced client that uses local DB with remote sync
        client = create_client_sync(
            url="file:" + LOCAL_DB_PATH,
            auth_token=TURSO_KEY,
            sync_url=TURSO_URL
        )

        # Perform initial sync to ensure local DB is up to date
        sync_db(client, "Initial database sync")

        # Clear any past queries
        app.dbSQL = None
        app.dbParams = None

        # Wrap the execute method to add logging and auto-sync
        original_execute = client.execute

        def logged_execute(sql, *params, **kwargs):
            # Store for later error handling
            app.dbSQL = sql
            app.dbParams = params[0] if params else None

            # Log and run the query
            log_db_request(app, sql, params)
            result = original_execute(sql, *params, **kwargs)
            log_db_result(app, sql, result)

            # Sync after write operations to push changes to remote
            sql_upper = sql.upper().strip()
            if any(keyword in sql_upper for keyword in ['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER']):
                sync_db(client, f"Sync after {sql_upper.split()[0]} operation")

            return result

        # Update the execute function
        client.execute = logged_execute

        # And return the client connection
        yield client

    finally:
        if client is not None:
            client.close()


