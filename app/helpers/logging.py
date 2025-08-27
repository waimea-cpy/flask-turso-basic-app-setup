#===========================================================
# Logging Middleware
#===========================================================

from flask import request, session
from dotenv import load_dotenv
from os import getenv
from colorama import Fore, init
from datetime import datetime
import logging

# Colorama config
init(autoreset=True)

# Logging colours
REQUEST_COL = Fore.CYAN
ROUTE_COL   = Fore.YELLOW
SESSION_COL = Fore.MAGENTA
DB_COL      = Fore.BLUE
OK_COL      = Fore.GREEN
WARN_COL    = Fore.YELLOW
ERROR_COL   = Fore.RED
RESET_COL   = Fore.RESET

# Divider for requests
DIVIDER = "─" * 80

# Logging sections
REQUEST_TEXT   = "Request: "
MATCH_TEXT     = "Matches: "
HANDLER_TEXT   = "Handler: "
PARAMS_TEXT    = " Params: "
ARGS_TEXT      = "   Args: "
FORM_TEXT      = "  Forms: "
FILE_TEXT      = "  Files: "
SESSION_TEXT   = "Session: "
DB_SQL_TEXT    = " DB SQL: "
DB_PARAMS_TEXT = " Params: "
DB_DATA_TEXT   = "DB Data: "
DB_ROWS_TEXT   = "   Rows: "
DB_NEW_ID_TEXT = " New ID: "
STATUS_TEXT    = " Status: "

# Logging indentation for values
SPACING = " " * len(REQUEST_TEXT)

# Logging headings
REQUEST_HEADING   = f"{REQUEST_TEXT  }{REQUEST_COL}"
MATCH_HEADING     = f"{MATCH_TEXT    }{ROUTE_COL  }"
HANDLER_HEADING   = f"{HANDLER_TEXT  }{ROUTE_COL  }"
PARAMS_HEADING    = f"{PARAMS_TEXT   }{ROUTE_COL  }"
ARGS_HEADING      = f"{ARGS_TEXT     }{ROUTE_COL  }"
FORM_HEADING      = f"{FORM_TEXT     }{ROUTE_COL  }"
FILE_HEADING      = f"{FILE_TEXT     }{ROUTE_COL  }"
SESSION_HEADING   = f"{SESSION_TEXT  }{SESSION_COL}"
DB_SQL_HEADING    = f"{DB_SQL_TEXT   }{DB_COL     }"
DB_PARAMS_HEADING = f"{DB_PARAMS_TEXT}{DB_COL     }"
DB_DATA_HEADING   = f"{DB_DATA_TEXT  }{DB_COL     }"
DB_ROWS_HEADING   = f"{DB_ROWS_TEXT  }{DB_COL     }"
DB_NEW_ID_HEADING = f"{DB_NEW_ID_TEXT}{DB_COL     }"

# Load Flask environment variables from the .env file
load_dotenv()
HOST = getenv("FLASK_RUN_HOST", "localhost")
PORT = getenv("FLASK_RUN_PORT", 5000)

# Disable built-in logging
logging.getLogger('werkzeug').setLevel(logging.CRITICAL)


#-----------------------------------------------------------
# Return a coloured status message
#-----------------------------------------------------------
def _col_status(response):
    if response.status_code < 300:
        return f"{OK_COL}{response.status}"
    if response.status_code < 400:
        return f"{WARN_COL}{response.status}"
    return f"{ERROR_COL}{response.status}"


#-----------------------------------------------------------
# Highlight grouping / punctuation chars as normal colour
#-----------------------------------------------------------
def _highlight(text, normal_col):
    high_chars = ["{", "}", "[", "]", "(", ")", ":", ",", '"', "'", "=", "?", ".", "@"]
    reset_marker = "^^^"
    normal_marker = "|||"

    for char in high_chars:
        text = text.replace(char, f"{reset_marker}{char}{normal_marker}")

    text = text.replace(reset_marker, RESET_COL)
    text = text.replace(normal_marker, normal_col)

    return text


#-----------------------------------------------------------
# Provide logging handlers to the Flask app
#-----------------------------------------------------------
def init_logging(app):
    # Announce the app...
    print(f"\n🚀 Flask server is running at {OK_COL}http://{HOST}{WARN_COL}:{PORT}\n")


    #--------------------------------------------------
    # Pre-request logging
    #--------------------------------------------------
    @app.before_request
    def log_request():
        # Don't log at start for static files
        if app.debug and not '/static/' in request.path:
            now = datetime.now().strftime("%H:%M:%S")
            print(f"{now}{DIVIDER}\n{REQUEST_HEADING}{request.method} {request.path}")      # The URL

            if request.url_rule:
                print(MATCH_HEADING   + _highlight(f"{request.method.lower()}(\"{request.url_rule}\")", ROUTE_COL))   # Matched routing rule
            if request.endpoint:
                print(HANDLER_HEADING + _highlight(f"{request.endpoint}()", ROUTE_COL))     # Matched route function name
            if request.view_args:
                print(PARAMS_HEADING  + _highlight(f"{request.view_args}", ROUTE_COL))      # URL params, if any
            if request.args:
                print(ARGS_HEADING    + _highlight(f"{dict(request.args)}", ROUTE_COL))     # Any GET args
            if request.form:
                print(FORM_HEADING    + _highlight(f"{dict(request.form)}", ROUTE_COL))     # Any form data
            if request.files:
                print(FILE_HEADING    + _highlight(f"{dict(request.files)}", ROUTE_COL))    # Any files uploaded
            if session:
                print(SESSION_HEADING + _highlight(f"{dict(session)}", SESSION_COL))        # Any session values


    #--------------------------------------------------
    # Post-request logging
    #--------------------------------------------------
    @app.after_request
    def log_response(response):
        if app.debug:
            # Was this a matched route?
            if not '/static/' in request.path:
                # Yes, so complete it
                print(f"{STATUS_TEXT}{_col_status(response)}{RESET_COL}\n")
            else:
                # Nope, a static file, so show the full request/response
                now = datetime.now().strftime("%H:%M:%S")
                print(f"{now}{DIVIDER}\n{REQUEST_HEADING}{request.method} {request.path} {_col_status(response)}{RESET_COL}\n")

        return response


#-----------------------------------------------------------
# Converts the row data from a DB result set into a well
# formatted string, not including large BLOB data, instead
# adding a summary of the data
#-----------------------------------------------------------
def _format_result_rows(result):
    columns = result.columns
    records = []
    for row in result.rows:
        data = {}
        for col, val in zip(columns, row):
            if not val:
                data[col] = None
            elif isinstance(val, (bytes, bytearray)):
                data[col] = f"<BLOB {len(val)} bytes>"
            elif isinstance(val, str) and len(val) > 30:
                data[col] = f"{val[:30]}…"
            else:
                data[col] = val
        records.append(f"{dict(data)}")

    return f"\n{SPACING}".join(records) if len(records) > 0 else "None"


#-----------------------------------------------------------
# Formats an SQL query, adjusting any indents on a multiline
# string to match the logging indentation
#-----------------------------------------------------------
def _format_query(sql):
    sql = sql.strip("\n")                       # Clean outer newlines
    indent = len(sql) - len(sql.lstrip(" "))    # Count leading spaces of first line
    sql = sql.strip()                           # Clean outer whitespace
    sql_rows = sql.split(f"\n{' '*indent}")     # Break lines, retaining other spacing
    sql = f"\n{SPACING}".join(sql_rows)         # Rejoin with correct indent for logs
    return sql


#-----------------------------------------------------------
# Log a given SQL request - Call prior to running the SQL
#-----------------------------------------------------------
def log_db_request(app, sql, params):
    if app.debug:
        print(DB_SQL_HEADING    + _highlight(_format_query(sql), DB_COL))
        print(DB_PARAMS_HEADING + _highlight(f"{params[0] if params else 'None'}", DB_COL))


#-----------------------------------------------------------
# Log result of an SQL request - Call after running the SQL
#-----------------------------------------------------------
def log_db_result(app, sql, result):
    if app.debug:
        sqlUp = sql.upper()

        # Check the type of query
        if 'SELECT' in sqlUp:
            print(DB_DATA_HEADING   + _highlight(_format_result_rows(result), DB_COL))

        elif 'UPDATE' in sqlUp or 'DELETE' in sqlUp:
            print(DB_ROWS_HEADING   + _highlight(f"{getattr(result, 'rows_affected', result)} affected", DB_COL))

        elif 'INSERT' in sqlUp:
            print(DB_NEW_ID_HEADING + _highlight(f"{getattr(result, 'last_insert_rowid', result)}", DB_COL))

