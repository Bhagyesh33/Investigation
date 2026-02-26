# -*- coding: utf-8 -*-
import streamlit as st
import snowflake.connector
import pandas as pd
from datetime import datetime, timedelta
import re
import logging
import traceback
from decimal import Decimal
import numpy as np

try:
    from matplotlib import pyplot as plt
    import matplotlib.pyplot as plt
    import seaborn as sns
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

st.set_page_config(
    page_title="DeploySure Suite",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    .main-header {
        text-align: center;
        padding: 30px;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 10px;
        margin-bottom: 30px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    .score-box {
        text-align: center;
        padding: 20px;
        border-radius: 10px;
        font-size: 24px;
        font-weight: bold;
        margin: 20px 0;
    }
    
    .passed-score {
        background-color: #d4edda;
        border: 2px solid #28a745;
        color: #155724;
    }
    
    .warning-score {
        background-color: #fff3cd;
        border: 2px solid #ffc107;
        color: #856404;
    }
    
    .failed-score {
        background-color: #f8d7da;
        border: 2px solid #dc3545;
        color: #721c24;
    }
    
    .stButton button {
        width: 100%;
        border-radius: 8px;
        font-weight: 600;
    }

    .kpi-card {
        background: white;
        border-radius: 8px;
        padding: 16px;
        border-left: 4px solid #667eea;
        box-shadow: 0 2px 4px rgba(0,0,0,0.08);
        margin-bottom: 8px;
    }
</style>
""", unsafe_allow_html=True)

# ========== SNOWFLAKE FUNCTIONS ==========
def get_snowflake_connection(user, password, account):
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            authenticator='snowflake'
        )
        logging.info("Successfully connected to Snowflake.")
        return conn, "✅ Successfully connected!"
    except Exception as e:
        logging.error(f"Connection failed: {str(e)}")
        return None, f"❌ Connection failed: {str(e)}"

def get_databases(conn):
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        return [row[1] for row in cursor.fetchall()]
    except Exception as e:
        logging.error(f"Error getting databases: {str(e)}")
        return []

def get_schemas(conn, database):
    if not conn or not database:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {database}")
        return [row[1] for row in cursor.fetchall()]
    except Exception as e:
        logging.error(f"Error getting schemas: {str(e)}")
        return []

def get_tables(conn, database, schema):
    if not conn or not database or not schema:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute(f"SHOW TABLES IN SCHEMA {database}.{schema}")
        tables = [row[1] for row in cursor.fetchall()]
        return [t for t in tables if t.upper() not in ('TEST_CASES', 'ORDER_KPIS')]
    except Exception as e:
        logging.error(f"Error getting tables: {str(e)}")
        return []

def get_columns_for_table(conn, database, schema, table):
    if not conn or not database or not schema or not table:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COLUMN_NAME
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
        """)
        return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        logging.error(f"Error getting columns: {str(e)}")
        return []

def _get_column_details_for_dq(conn, database, schema, table):
    if not conn or not database or not schema or not table:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
        """)
        return [{'name': row[0], 'type': row[1].upper()} for row in cursor.fetchall()]
    except Exception as e:
        logging.error(f"Error getting column details: {str(e)}")
        return []

def _categorize_columns_by_type(column_details_list):
    numeric_cols, date_cols, string_cols, all_cols = [], [], [], []
    for col in column_details_list:
        col_name, col_type = col['name'], col['type']
        all_cols.append(col_name)
        if any(t in col_type for t in ["NUMBER", "INT", "FLOAT", "DOUBLE"]):
            numeric_cols.append(col_name)
        elif any(t in col_type for t in ["DATE", "TIMESTAMP"]):
            date_cols.append(col_name)
        elif any(t in col_type for t in ["VARCHAR", "TEXT", "STRING"]):
            string_cols.append(col_name)
    return all_cols, numeric_cols, date_cols, string_cols

def clone_schema(conn, source_db, source_schema, target_schema):
    if not conn:
        return False, "❌ Not connected to Snowflake.", pd.DataFrame()
    if not source_db or not source_schema or not target_schema:
        return False, "⚠️ Please provide all required fields.", pd.DataFrame()
    
    cursor = conn.cursor()
    try:
        cursor.execute(f"SHOW SCHEMAS LIKE '{source_schema}' IN DATABASE {source_db}")
        if not cursor.fetchall():
            return False, f"❌ Source schema doesn't exist", pd.DataFrame()
        
        clone_sql = f"CREATE OR REPLACE SCHEMA {source_db}.{target_schema} CLONE {source_db}.{source_schema}"
        cursor.execute(clone_sql)
        
        cursor.execute(f"SHOW TABLES IN SCHEMA {source_db}.{source_schema}")
        source_tables = [row[1] for row in cursor.fetchall()]
        
        cursor.execute(f"SHOW TABLES IN SCHEMA {source_db}.{target_schema}")
        clone_tables = [row[1] for row in cursor.fetchall()]
        
        df = pd.DataFrame({
            'Database': [source_db],
            'Source Schema': [source_schema],
            'Clone Schema': [target_schema],
            'Source Tables': [len(source_tables)],
            'Cloned Tables': [len(clone_tables)],
            'Status': ['✅ Success' if len(source_tables) == len(clone_tables) else '⚠️ Partial']
        })
        
        return True, f"✅ Successfully Mirrored Schema", df
    except Exception as e:
        logging.error(f"Clone failed: {str(e)}")
        return False, f"❌ Clone failed: {str(e)}", pd.DataFrame()

def compare_table_differences(conn, db_name, source_schema, clone_schema):
    if not conn:
        return pd.DataFrame()
    
    cursor = conn.cursor()
    query = f"""
    WITH source_tables AS (
        SELECT table_name FROM {db_name}.information_schema.tables
        WHERE table_schema = '{source_schema}'
    ),
    clone_tables AS (
        SELECT table_name FROM {db_name}.information_schema.tables
        WHERE table_schema = '{clone_schema}'
    )
    SELECT
        COALESCE(s.table_name, c.table_name) AS table_name,
        CASE
            WHEN s.table_name IS NULL THEN 'Missing in source'
            WHEN c.table_name IS NULL THEN 'Missing in clone'
            ELSE 'Present in both'
        END AS difference
    FROM source_tables s
    FULL OUTER JOIN clone_tables c ON s.table_name = c.table_name
    WHERE s.table_name IS NULL OR c.table_name IS NULL
    ORDER BY difference, table_name
    """
    
    try:
        cursor.execute(query)
        return pd.DataFrame(cursor.fetchall(), columns=['Table', 'Difference'])
    except Exception as e:
        logging.error(f"Error comparing tables: {str(e)}")
        return pd.DataFrame()

def compare_column_differences(conn, db_name, source_schema, clone_schema):
    if not conn:
        return pd.DataFrame(), pd.DataFrame()
    
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT s.table_name
            FROM {db_name}.information_schema.tables s
            JOIN {db_name}.information_schema.tables c ON s.table_name = c.table_name
            WHERE s.table_schema = '{source_schema}' AND c.table_schema = '{clone_schema}'
        """)
        common_tables = [row[0] for row in cursor.fetchall()]
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame()
    
    column_diff_data, datatype_diff_data = [], []
    
    for table in common_tables:
        try:
            cursor.execute(f"DESCRIBE TABLE {db_name}.{source_schema}.{table}")
            source_cols = {row[0]: row[1] for row in cursor.fetchall()}
            
            cursor.execute(f"DESCRIBE TABLE {db_name}.{clone_schema}.{table}")
            clone_cols = {row[0]: row[1] for row in cursor.fetchall()}
            
            all_columns = set(source_cols.keys()).union(set(clone_cols.keys()))
            
            for col in all_columns:
                if col not in source_cols:
                    column_diff_data.append({
                        'Table': table, 'Column': col, 'Difference': 'Missing in source',
                        'Source Type': None, 'Clone Type': clone_cols.get(col)
                    })
                elif col not in clone_cols:
                    column_diff_data.append({
                        'Table': table, 'Column': col, 'Difference': 'Missing in clone',
                        'Source Type': source_cols.get(col), 'Clone Type': None
                    })
                elif source_cols[col] != clone_cols[col]:
                    datatype_diff_data.append({
                        'Table': table, 'Column': col,
                        'Source Type': source_cols[col], 'Clone Type': clone_cols[col],
                        'Difference': 'Type Changed'
                    })
        except:
            continue
    
    return pd.DataFrame(column_diff_data), pd.DataFrame(datatype_diff_data)

def get_test_case_tables(conn, database, schema):
    if not conn or not database or not schema:
        return ["All"]
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COUNT(*) FROM {database}.information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = 'TEST_CASES'
        """)
        if cursor.fetchone()[0] == 0:
            return ["All"]
        
        cursor.execute(f"""
            SELECT DISTINCT TABLE_NAME FROM {database}.{schema}.TEST_CASES
            WHERE TABLE_NAME IS NOT NULL ORDER BY TABLE_NAME
        """)
        return ["All"] + [row[0] for row in cursor.fetchall()]
    except:
        return ["All"]

def get_test_cases(conn, database, schema, table):
    if not conn or not database or not schema:
        return []
    try:
        cursor = conn.cursor()
        if table == "All":
            query = f"""
                SELECT TEST_CASE_ID, TEST_ABBREVIATION, TABLE_NAME,
                       TEST_DESCRIPTION, SQL_CODE, EXPECTED_RESULT
                FROM {database}.{schema}.TEST_CASES ORDER BY TEST_CASE_ID
            """
        else:
            query = f"""
                SELECT TEST_CASE_ID, TEST_ABBREVIATION, TABLE_NAME,
                       TEST_DESCRIPTION, SQL_CODE, EXPECTED_RESULT
                FROM {database}.{schema}.TEST_CASES
                WHERE TABLE_NAME = '{table}' ORDER BY TEST_CASE_ID
            """
        cursor.execute(query)
        return cursor.fetchall()
    except:
        return []

def validate_test_cases(conn, database, schema, test_cases):
    if not conn or not test_cases:
        return pd.DataFrame(), "❌ No connection or test cases"
    
    cursor = conn.cursor()
    results = []
    
    for case in test_cases:
        test_id, abbrev, table_name, desc, sql, expected = case
        expected = str(expected).strip()
        
        try:
            qualified_sql = re.sub(
                rf'\b{re.escape(table_name)}\b',
                f'{database}.{schema}.{table_name}',
                sql, flags=re.IGNORECASE
            )
            cursor.execute(qualified_sql)
            result = cursor.fetchone()
            actual = str(result[0]) if result else "0"
            status = "✅ PASS" if actual == expected else "❌ FAIL"
            
            results.append({
                'Test Case': abbrev, 'Category': table_name,
                'Expected': expected, 'Actual': actual, 'Status': status
            })
        except Exception as e:
            results.append({
                'Test Case': abbrev, 'Category': table_name,
                'Expected': expected, 'Actual': f"ERROR: {str(e)[:50]}",
                'Status': "❌ ERROR"
            })
    
    return pd.DataFrame(results), "✅ Validation completed"

def validate_kpis(conn, database, source_schema, target_schema):
    if not conn:
        return pd.DataFrame(), "❌ Not connected"
    
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT KPI_ID, KPI_NAME, KPI_VALUE FROM {database}.{source_schema}.ORDER_KPIS")
        kpis = cursor.fetchall()
        
        if not kpis:
            return pd.DataFrame(), "⚠️ No KPIs found"
        
        results = []
        for kpi_id, kpi_name, kpi_sql in kpis:
            try:
                source_query = re.sub(r'\bORDER_DATA\b', f'{database}.{source_schema}.ORDER_DATA', kpi_sql, flags=re.IGNORECASE)
                cursor.execute(source_query)
                source_val = cursor.fetchone()[0]
            except:
                source_val = "ERROR"
            
            try:
                clone_query = re.sub(r'\bORDER_DATA\b', f'{database}.{target_schema}.ORDER_DATA', kpi_sql, flags=re.IGNORECASE)
                cursor.execute(clone_query)
                clone_val = cursor.fetchone()[0]
            except:
                clone_val = "ERROR"
            
            if isinstance(source_val, (int, float)) and isinstance(clone_val, (int, float)):
                diff = float(source_val) - float(clone_val)
                status = '✅ Match' if diff == 0 else '⚠️ Mismatch'
            else:
                diff = "N/A"
                status = '✅ Match' if str(source_val) == str(clone_val) else '⚠️ Mismatch'
            
            results.append({
                'KPI': kpi_name, 'Source': source_val,
                'Clone': clone_val, 'Difference': diff, 'Status': status
            })
        
        return pd.DataFrame(results), "✅ KPI validation completed"
    except Exception as e:
        return pd.DataFrame(), f"❌ Failed: {str(e)}"

class DataQualityValidator:
    def __init__(self, conn):
        self.conn = conn
    
    def _execute_query(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        return pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    
    def _run_row_count_check(self, database, schema, table, min_rows):
        query = f"SELECT COUNT(*) FROM {database}.{schema}.{table}"
        count = self._execute_query(query).iloc[0, 0]
        status = "✅ Pass" if count >= min_rows else "❌ Fail"
        return {
            "Check": "Row Count", "Column": "N/A",
            "Expected": f">= {min_rows}", "Actual": count,
            "Status": status, "Details": f"Rows: {count}"
        }
    
    def _run_duplicate_check(self, database, schema, table):
        columns = _get_column_details_for_dq(self.conn, database, schema, table)
        if not columns:
            return {
                "Check": "Duplicates", "Column": "All",
                "Expected": "0", "Actual": "N/A",
                "Status": "⚠️ N/A", "Details": "No columns"
            }
        
        cols_str = ", ".join([f'"{col["name"]}"' for col in columns])
        query = f"""
        SELECT COUNT(*) FROM (
            SELECT {cols_str} FROM {database}.{schema}.{table}
            GROUP BY {cols_str} HAVING COUNT(*) > 1
        )
        """
        dup_count = self._execute_query(query).iloc[0, 0]
        status = "✅ Pass" if dup_count == 0 else "❌ Fail"
        return {
            "Check": "Duplicates", "Column": "All",
            "Expected": "0", "Actual": dup_count,
            "Status": status, "Details": f"Duplicates: {dup_count}"
        }
    
    def run_checks(self, database, schema, table, check_row_count, min_rows, check_duplicates):
        results = []
        total = passed = failed = 0
        
        if check_row_count:
            res = self._run_row_count_check(database, schema, table, min_rows)
            results.append(res)
            total += 1
            if res["Status"] == "✅ Pass":
                passed += 1
            else:
                failed += 1
        
        if check_duplicates:
            res = self._run_duplicate_check(database, schema, table)
            results.append(res)
            total += 1
            if res["Status"] == "✅ Pass":
                passed += 1
            else:
                failed += 1
        
        score = (passed / total * 100) if total > 0 else 0
        
        summary = pd.DataFrame([
            {"Metric": "Table", "Value": f"{database}.{schema}.{table}"},
            {"Metric": "Total Checks", "Value": total},
            {"Metric": "Passed", "Value": passed},
            {"Metric": "Failed", "Value": failed},
            {"Metric": "Score", "Value": f"{score:.1f}%"}
        ])
        
        details = pd.DataFrame(results)
        
        return summary, details, score


# ========== PERFORMANCE MONITORING FUNCTIONS (FROM GRADIO APP) ==========

def _build_where_clause(start_date, end_date, database=None, schema=None, warehouse=None, user=None, query_type=None):
    where_clauses = [f"START_TIME BETWEEN '{start_date}' AND '{end_date}'"]
    if database and database != "All":
        where_clauses.append(f"DATABASE_NAME = '{database}'")
    if schema and schema != "All":
        where_clauses.append(f"SCHEMA_NAME = '{schema}'")
    if warehouse and warehouse != "All":
        where_clauses.append(f"WAREHOUSE_NAME = '{warehouse}'")
    if user and user != "All":
        where_clauses.append(f"USER_NAME = '{user}'")
    if query_type and query_type != "All":
        where_clauses.append(f"QUERY_TYPE = '{query_type}'")
    return "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

def _execute_perf_query(conn, query):
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        for col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col], errors='ignore')
            except:
                pass
        return df
    except Exception as e:
        logging.error(f"Query error: {str(e)}")
        return pd.DataFrame()

def fetch_longest_running_queries(conn, start_date, end_date, database, schema, warehouse, user, query_type):
    where_clause = _build_where_clause(start_date, end_date, database, schema, warehouse, user, query_type)
    query = f"""
    SELECT QUERY_ID as "Query ID", ROUND(EXECUTION_TIME/1000, 2) as "Exec Time (s)", 
           USER_NAME as "User", START_TIME as "Start Time", WAREHOUSE_NAME as "Warehouse",
           LEFT(QUERY_TEXT, 100) as "Query Preview"
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    {where_clause}
    ORDER BY "Exec Time (s)" DESC LIMIT 10
    """
    return _execute_perf_query(conn, query)

def fetch_expensive_queries(conn, start_date, end_date, database, schema, warehouse, user, query_type):
    where_clause = _build_where_clause(start_date, end_date, database, schema, warehouse, user, query_type)
    query = f"""
    SELECT qh.QUERY_ID AS "Query ID", LEFT(qh.QUERY_TEXT, 100) AS "Query Preview",
           qh.USER_NAME AS "User", qh.WAREHOUSE_NAME AS "Warehouse",
           SUM(wmh.CREDITS_USED) AS "Credits Consumed", qh.START_TIME AS "Start Time"
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
    JOIN SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY wmh
        ON qh.WAREHOUSE_ID = wmh.WAREHOUSE_ID
        AND qh.START_TIME BETWEEN wmh.START_TIME AND wmh.END_TIME
    {where_clause}
    GROUP BY qh.QUERY_ID, qh.QUERY_TEXT, qh.USER_NAME, qh.WAREHOUSE_NAME, qh.START_TIME
    ORDER BY "Credits Consumed" DESC LIMIT 10
    """
    return _execute_perf_query(conn, query)

def fetch_top_frequent_queries(conn, start_date, end_date, database, schema, warehouse, user, query_type):
    where_clause = _build_where_clause(start_date, end_date, database, schema, warehouse, user, query_type)
    query = f"""
    SELECT LEFT(QUERY_TEXT, 100) as "Query Preview", COUNT(*) as "Execution Count", 
           USER_NAME as "User", AVG(ROUND(EXECUTION_TIME/1000, 2)) as "Avg Exec Time (s)"
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    {where_clause}
    GROUP BY QUERY_TEXT, USER_NAME
    ORDER BY "Execution Count" DESC LIMIT 10
    """
    return _execute_perf_query(conn, query)

def fetch_failed_queries(conn, start_date, end_date, database, schema, warehouse, user):
    where_clause = _build_where_clause(start_date, end_date, database, schema, warehouse, user, None)
    where_clause_failed = where_clause.replace("WHERE ", "WHERE EXECUTION_STATUS != 'SUCCESS' AND ", 1) if where_clause else "WHERE EXECUTION_STATUS != 'SUCCESS'"
    query = f"""
    SELECT QUERY_ID AS "Query ID", LEFT(QUERY_TEXT, 100) AS "Query Preview",
           USER_NAME AS "User", LEFT(ERROR_MESSAGE, 150) AS "Error", START_TIME AS "Start Time"
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    {where_clause_failed}
    ORDER BY START_TIME DESC LIMIT 50
    """
    return _execute_perf_query(conn, query)

def fetch_top_active_users(conn, start_date, end_date, database, schema, query_type):
    where_clause = _build_where_clause(start_date, end_date, database, schema, None, None, query_type)
    query = f"""
    SELECT USER_NAME as "User", COUNT(*) as "Query Count", 
           COUNT(DISTINCT SESSION_ID) as "Sessions",
           ROUND(SUM(EXECUTION_TIME/1000), 2) as "Total Exec Time (s)"
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    {where_clause}
    GROUP BY USER_NAME ORDER BY "Query Count" DESC LIMIT 10
    """
    return _execute_perf_query(conn, query)

def fetch_active_users_over_time(conn, start_date, end_date, database, schema, warehouse, user, query_type):
    where_clause = _build_where_clause(start_date, end_date, database, schema, warehouse, user, query_type)
    query = f"""
    SELECT TO_DATE(START_TIME) AS "Date", COUNT(DISTINCT USER_NAME) AS "Active Users"
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    {where_clause}
    GROUP BY 1 ORDER BY 1
    """
    return _execute_perf_query(conn, query)

def fetch_warehouse_credits(conn, start_date, end_date, warehouse):
    where_clause = f"WHERE START_TIME BETWEEN '{start_date}' AND '{end_date}'"
    if warehouse and warehouse != "All":
        where_clause += f" AND WAREHOUSE_NAME = '{warehouse}'"
    query = f"""
    SELECT WAREHOUSE_NAME as "Warehouse", TO_DATE(START_TIME) as "Date",
           SUM(CREDITS_USED) as "Credits Used",
           SUM(CREDITS_USED_COMPUTE) as "Compute Credits",
           SUM(CREDITS_USED_CLOUD_SERVICES) as "Cloud Services Credits"
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    {where_clause}
    GROUP BY WAREHOUSE_NAME, TO_DATE(START_TIME)
    ORDER BY "Date" ASC, "Credits Used" DESC
    """
    return _execute_perf_query(conn, query)

def fetch_credit_usage_over_time(conn, start_date, end_date, warehouse):
    where_clause = f"WHERE START_TIME BETWEEN '{start_date}' AND '{end_date}'"
    if warehouse and warehouse != "All":
        where_clause += f" AND WAREHOUSE_NAME = '{warehouse}'"
    date_diff = (datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days
    if date_diff <= 31:
        date_expr = "TO_DATE(START_TIME)"
        date_col = "Date"
    else:
        date_expr = "TO_CHAR(START_TIME, 'YYYY-MM')"
        date_col = "Month"
    query = f"""
    SELECT {date_expr} AS "{date_col}", SUM(CREDITS_USED) AS "Credits Used"
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    {where_clause}
    GROUP BY {date_expr} ORDER BY {date_expr}
    """
    return _execute_perf_query(conn, query), date_col

def fetch_cost_heatmap_data(conn, start_date, end_date, warehouse):
    where_clause = f"WHERE START_TIME BETWEEN '{start_date}' AND '{end_date}'"
    if warehouse and warehouse != "All":
        where_clause += f" AND WAREHOUSE_NAME = '{warehouse}'"
    query = f"""
    SELECT TO_CHAR(START_TIME, 'DY') AS "DayOfWeek", 
           EXTRACT(HOUR FROM START_TIME) AS "HourOfDay", 
           SUM(CREDITS_USED) AS "Credits"
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    {where_clause}
    GROUP BY 1, 2
    """
    return _execute_perf_query(conn, query)

def fetch_warehouse_utilization(conn, start_date, end_date, warehouse):
    where_clause = f"WHERE START_TIME BETWEEN '{start_date}' AND '{end_date}'"
    if warehouse and warehouse != "All":
        where_clause += f" AND WAREHOUSE_NAME = '{warehouse}'"
    query = f"""
    SELECT WAREHOUSE_NAME as "Warehouse", TO_DATE(START_TIME) as "Date",
           AVG(AVG_RUNNING) as "Avg Running",
           AVG(AVG_QUEUED_LOAD) as "Avg Queued",
           AVG(AVG_BLOCKED) as "Avg Blocked"
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
    {where_clause}
    GROUP BY 1, 2 ORDER BY 1, 2
    """
    return _execute_perf_query(conn, query)

def fetch_daily_storage_usage(conn, start_date, end_date):
    query = f"""
    SELECT USAGE_DATE as "Date",
           AVERAGE_DATABASE_BYTES/POWER(1024,3) as "Avg DB Storage (GB)",
           AVERAGE_FAILSAFE_BYTES/POWER(1024,3) as "Avg Failsafe (GB)"
    FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY
    WHERE USAGE_DATE BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY "Date" ASC
    """
    return _execute_perf_query(conn, query)

def fetch_table_storage_metrics(conn, database, schema):
    if not database or database == "All" or not schema or schema == "All":
        return pd.DataFrame()
    query = f"""
    SELECT TABLE_NAME as "Table",
           ACTIVE_BYTES/POWER(1024,2) as "Active Size (MB)",
           TIME_TRAVEL_BYTES/POWER(1024,2) as "Time Travel (MB)"
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
    WHERE TABLE_CATALOG = '{database}' AND TABLE_SCHEMA = '{schema}'
    ORDER BY "Active Size (MB)" DESC
    """
    return _execute_perf_query(conn, query)

def get_all_users(conn):
    if not conn:
        return ["All"]
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT USER_NAME FROM SNOWFLAKE.ACCOUNT_USAGE.USERS WHERE DELETED_ON IS NULL ORDER BY USER_NAME")
        return ["All"] + [row[0] for row in cursor.fetchall()]
    except:
        return ["All"]

def get_all_warehouses(conn):
    if not conn:
        return ["All"]
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW WAREHOUSES")
        return ["All"] + [row[0] for row in cursor.fetchall()]
    except:
        return ["All"]

def get_date_range(time_range_str):
    end = datetime.now()
    if time_range_str == "Last 24 hours":
        start = end - timedelta(days=1)
    elif time_range_str == "Last 7 days":
        start = end - timedelta(days=7)
    elif time_range_str == "Last 30 days":
        start = end - timedelta(days=30)
    else:
        start = end - timedelta(days=7)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


# ========== SESSION STATE ==========
if 'conn' not in st.session_state:
    st.session_state.conn = None
if 'is_logged_in' not in st.session_state:
    st.session_state.is_logged_in = False
if 'username' not in st.session_state:
    st.session_state.username = ""

# ========== LOGIN PAGE ==========
def show_login_page():
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("""
        <div class="main-header">
            <h1>🔧 DeploySure Suite</h1>
            <p>Snowflake Data Validation & Quality Management</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        st.subheader("🔐 Sign in to Snowflake")
        
        with st.form("login_form"):
            username = st.text_input("Username", placeholder="your_username")
            password = st.text_input("Password", type="password", placeholder="••••••••")
            account = st.text_input("Account", placeholder="account.region")
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            login_button = st.form_submit_button("🔓 Connect", use_container_width=True, type="primary")
            
            if login_button:
                if username and password and account:
                    with st.spinner("🔄 Connecting..."):
                        conn, msg = get_snowflake_connection(username, password, account)
                        
                        if conn:
                            st.session_state.conn = conn
                            st.session_state.is_logged_in = True
                            st.session_state.username = username
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)
                else:
                    st.warning("⚠️ Please fill in all fields")
        
        st.markdown("<br>", unsafe_allow_html=True)
        st.info("💡 **Tip:** Ensure you have proper Snowflake credentials and network access")

# ========== MAIN APP ==========
def show_main_app():
    st.markdown(f"""
    <div class="main-header">
        <h1>🔧 DeploySure Suite</h1>
        <p>Welcome, {st.session_state.username}!</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.success(f"✅ **{st.session_state.username}**")
        
        if st.button("🔓 Disconnect", use_container_width=True):
            if st.session_state.conn:
                st.session_state.conn.close()
            st.session_state.conn = None
            st.session_state.is_logged_in = False
            st.rerun()
        
        st.markdown("---")
        try:
            dbs = get_databases(st.session_state.conn)
            st.metric("Databases", len(dbs))
        except:
            pass
    
    # Main tabs - now 3 tabs
    tab1, tab2, tab3 = st.tabs(["⎘ MirrorSchema", "🔍 DriftWatch", "📊 Performance Monitoring"])
    
    # ===== MIRROR SCHEMA =====
    with tab1:
        st.header("Mirror Schema")
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.subheader("📋 Configuration")
            
            databases = get_databases(st.session_state.conn)
            if not databases:
                st.warning("No databases found")
                return
            
            source_db = st.selectbox("Source Database", databases)
            
            if source_db:
                schemas = get_schemas(st.session_state.conn, source_db)
                if schemas:
                    source_schema = st.selectbox("Source Schema", schemas)
                    target_schema = st.text_input("Target Schema", value=f"{source_schema}_CLONE")
                    
                    if st.button("🚀 Execute MirrorSchema", type="primary", use_container_width=True):
                        if target_schema:
                            with st.spinner("Mirroring..."):
                                success, msg, df = clone_schema(
                                    st.session_state.conn, source_db, source_schema, target_schema
                                )
                                
                                if success:
                                    st.success(msg)
                                    if not df.empty:
                                        st.dataframe(df, use_container_width=True)
                                else:
                                    st.error(msg)
                else:
                    st.warning("No schemas found")
        
        with col2:
            st.subheader("ℹ️ Information")
            st.info("""
            **Mirror Schema** creates an exact copy of your source schema.
            
            **Includes:**
            - All tables and data
            - Table structures
            - Constraints
            """)
    
    # ===== DRIFTWATCH =====
    with tab2:
        st.header("DriftWatch")
        
        validation_type = st.selectbox(
            "Validation Type",
            ["Schema Validation", "KPI Validation", "Test Case Validation", "Data Quality Validation"]
        )
        
        st.markdown("---")
        
        # === SCHEMA VALIDATION ===
        if validation_type == "Schema Validation":
            col1, col2 = st.columns([1, 2])
            
            with col1:
                st.subheader("📋 Configuration")
                databases = get_databases(st.session_state.conn)
                val_db = st.selectbox("Database", databases, key="schema_db")
                
                if val_db:
                    schemas = get_schemas(st.session_state.conn, val_db)
                    if len(schemas) >= 2:
                        val_source = st.selectbox("Source Schema", schemas, key="schema_source")
                        val_target = st.selectbox("Target Schema", schemas, index=1, key="schema_target")
                        
                        if st.button("Execute DriftWatch", type="primary", use_container_width=True):
                            with st.spinner("Validating..."):
                                table_diff = compare_table_differences(st.session_state.conn, val_db, val_source, val_target)
                                col_diff, type_diff = compare_column_differences(st.session_state.conn, val_db, val_source, val_target)
                                
                                st.session_state.table_diff = table_diff
                                st.session_state.col_diff = col_diff
                                st.session_state.type_diff = type_diff
                                st.success("✅ Validation completed!")
                    else:
                        st.warning("Need at least 2 schemas")
            
            with col2:
                st.subheader("📊 Results")
                
                sub_tab1, sub_tab2, sub_tab3 = st.tabs(["Tables", "Columns", "Data Types"])
                
                with sub_tab1:
                    if 'table_diff' in st.session_state and not st.session_state.table_diff.empty:
                        st.dataframe(st.session_state.table_diff, use_container_width=True)
                        csv = st.session_state.table_diff.to_csv(index=False)
                        st.download_button("📥 Download", csv, f"table_diff_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                    else:
                        st.info("No differences found")
                
                with sub_tab2:
                    if 'col_diff' in st.session_state and not st.session_state.col_diff.empty:
                        st.dataframe(st.session_state.col_diff, use_container_width=True)
                        csv = st.session_state.col_diff.to_csv(index=False)
                        st.download_button("📥 Download", csv, f"col_diff_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                    else:
                        st.info("No differences found")
                
                with sub_tab3:
                    if 'type_diff' in st.session_state and not st.session_state.type_diff.empty:
                        st.dataframe(st.session_state.type_diff, use_container_width=True)
                        csv = st.session_state.type_diff.to_csv(index=False)
                        st.download_button("📥 Download", csv, f"type_diff_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                    else:
                        st.info("No differences found")
        
        # === KPI VALIDATION ===
        elif validation_type == "KPI Validation":
            col1, col2 = st.columns([1, 2])
            
            with col1:
                st.subheader("📋 Configuration")
                databases = get_databases(st.session_state.conn)
                kpi_db = st.selectbox("Database", databases, key="kpi_db")
                
                if kpi_db:
                    schemas = get_schemas(st.session_state.conn, kpi_db)
                    if len(schemas) >= 2:
                        kpi_source = st.selectbox("Source Schema", schemas, key="kpi_source")
                        kpi_target = st.selectbox("Target Schema", schemas, index=1, key="kpi_target")
                        
                        if st.button("Execute DriftWatch", type="primary", use_container_width=True):
                            with st.spinner("Validating KPIs..."):
                                df, msg = validate_kpis(st.session_state.conn, kpi_db, kpi_source, kpi_target)
                                st.session_state.kpi_results = df
                                
                                if not df.empty:
                                    st.success(msg)
                                else:
                                    st.warning(msg)
                    else:
                        st.warning("Need at least 2 schemas")
            
            with col2:
                st.subheader("📊 Results")
                if 'kpi_results' in st.session_state and not st.session_state.kpi_results.empty:
                    st.dataframe(st.session_state.kpi_results, use_container_width=True)
                    csv = st.session_state.kpi_results.to_csv(index=False)
                    st.download_button("📥 Download", csv, f"kpi_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                else:
                    st.info("Run validation to see results")
        
        # === TEST CASE VALIDATION ===
        elif validation_type == "Test Case Validation":
            col1, col2 = st.columns([1, 2])
            
            with col1:
                st.subheader("📋 Configuration")
                databases = get_databases(st.session_state.conn)
                tc_db = st.selectbox("Database", databases, key="tc_db")
                
                if tc_db:
                    schemas = get_schemas(st.session_state.conn, tc_db)
                    tc_schema = st.selectbox("Schema", schemas, key="tc_schema")
                    
                    if tc_schema:
                        tables = get_test_case_tables(st.session_state.conn, tc_db, tc_schema)
                        tc_table = st.selectbox("Category", tables, key="tc_table")
                        
                        test_cases = get_test_cases(st.session_state.conn, tc_db, tc_schema, tc_table)
                        
                        if test_cases:
                            st.subheader("Select Test Cases")
                            test_names = [f"{case[1]}" for case in test_cases]
                            
                            select_all = st.checkbox("Select All", value=True, key="tc_select_all")
                            
                            if select_all:
                                selected = st.multiselect(
                                    "Test Cases",
                                    test_names,
                                    default=test_names,
                                    key="tc_selected"
                                )
                            else:
                                selected = st.multiselect(
                                    "Test Cases",
                                    test_names,
                                    key="tc_selected_manual"
                                )
                            
                            if st.button("Execute DriftWatch", type="primary", use_container_width=True):
                                if selected:
                                    with st.spinner("Running tests..."):
                                        selected_cases = [case for case in test_cases if case[1] in selected]
                                        df, msg = validate_test_cases(
                                            st.session_state.conn, tc_db, tc_schema, selected_cases
                                        )
                                        st.session_state.test_results = df
                                        
                                        if not df.empty:
                                            st.success(msg)
                                        else:
                                            st.warning(msg)
                                else:
                                    st.warning("Select at least one test case")
                        else:
                            st.warning("No test cases found")
            
            with col2:
                st.subheader("📊 Results")
                if 'test_results' in st.session_state and not st.session_state.test_results.empty:
                    st.dataframe(st.session_state.test_results, use_container_width=True)
                    
                    pass_count = len(st.session_state.test_results[st.session_state.test_results['Status'].str.contains('PASS')])
                    fail_count = len(st.session_state.test_results[st.session_state.test_results['Status'].str.contains('FAIL')])
                    error_count = len(st.session_state.test_results[st.session_state.test_results['Status'].str.contains('ERROR')])
                    
                    col_a, col_b, col_c = st.columns(3)
                    col_a.metric("✅ Passed", pass_count)
                    col_b.metric("❌ Failed", fail_count)
                    col_c.metric("⚠️ Errors", error_count)
                    
                    csv = st.session_state.test_results.to_csv(index=False)
                    st.download_button(
                        "📥 Download",
                        csv,
                        f"test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    )
                else:
                    st.info("Run validation to see results")
        
        # === DATA QUALITY VALIDATION ===
        elif validation_type == "Data Quality Validation":
            col1, col2 = st.columns([1, 2])
            
            with col1:
                st.subheader("📋 Configuration")
                databases = get_databases(st.session_state.conn)
                dq_db = st.selectbox("Database", databases, key="dq_db")
                
                if dq_db:
                    schemas = get_schemas(st.session_state.conn, dq_db)
                    dq_schema = st.selectbox("Schema", schemas, key="dq_schema")
                    
                    if dq_schema:
                        tables = get_tables(st.session_state.conn, dq_db, dq_schema)
                        dq_table = st.selectbox("Table", tables, key="dq_table")
                        
                        if dq_table:
                            st.subheader("Quality Checks")
                            
                            dq_row_count = st.checkbox("Row Count Check", value=True, key="dq_row")
                            if dq_row_count:
                                dq_min_rows = st.number_input("Minimum Rows", value=1, min_value=0, key="dq_min")
                            else:
                                dq_min_rows = 1
                            
                            dq_duplicates = st.checkbox("Duplicate Rows Check", value=True, key="dq_dup")
                            
                            st.markdown("<br>", unsafe_allow_html=True)
                            
                            if st.button("Run Quality Checks", type="primary", use_container_width=True):
                                with st.spinner("Running checks..."):
                                    validator = DataQualityValidator(st.session_state.conn)
                                    summary, details, score = validator.run_checks(
                                        dq_db, dq_schema, dq_table,
                                        dq_row_count, dq_min_rows, dq_duplicates
                                    )
                                    
                                    st.session_state.dq_summary = summary
                                    st.session_state.dq_details = details
                                    st.session_state.dq_score = score
                                    st.success("✅ Quality checks completed!")
            
            with col2:
                st.subheader("📊 Results")
                
                if 'dq_score' in st.session_state:
                    score = st.session_state.dq_score
                    
                    if score >= 80:
                        score_class = "passed-score"
                    elif score >= 50:
                        score_class = "warning-score"
                    else:
                        score_class = "failed-score"
                    
                    st.markdown(
                        f'<div class="score-box {score_class}">Quality Score: {score:.0f}/100</div>',
                        unsafe_allow_html=True
                    )
                
                sub_tab1, sub_tab2 = st.tabs(["Summary", "Details"])
                
                with sub_tab1:
                    if 'dq_summary' in st.session_state and not st.session_state.dq_summary.empty:
                        st.dataframe(st.session_state.dq_summary, use_container_width=True)
                    else:
                        st.info("Run checks to see summary")
                
                with sub_tab2:
                    if 'dq_details' in st.session_state and not st.session_state.dq_details.empty:
                        st.dataframe(st.session_state.dq_details, use_container_width=True)
                        
                        csv = st.session_state.dq_details.to_csv(index=False)
                        st.download_button(
                            "📥 Download Report",
                            csv,
                            f"dq_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                        )
                    else:
                        st.info("Run checks to see details")

    # ===== PERFORMANCE MONITORING (NEW TAB) =====
    with tab3:
        st.header("📊 Performance Monitoring & Cost Analysis")
        st.markdown("Monitor query performance, warehouse costs, storage, user activity, and more.")

        # ---- Shared Filters ----
        with st.expander("🔧 Global Filters", expanded=True):
            fcol1, fcol2, fcol3, fcol4 = st.columns(4)
            with fcol1:
                time_range_opt = st.selectbox(
                    "Time Range",
                    ["Last 24 hours", "Last 7 days", "Last 30 days", "Custom"],
                    key="perf_time_range"
                )
            with fcol2:
                if time_range_opt == "Custom":
                    perf_start = st.text_input("Start Date (YYYY-MM-DD)", value=(datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"), key="perf_start")
                else:
                    perf_start, perf_end = get_date_range(time_range_opt)
                    st.text_input("Start Date", value=perf_start, disabled=True, key="perf_start_display")
            with fcol3:
                if time_range_opt == "Custom":
                    perf_end = st.text_input("End Date (YYYY-MM-DD)", value=datetime.now().strftime("%Y-%m-%d"), key="perf_end")
                else:
                    st.text_input("End Date", value=perf_end, disabled=True, key="perf_end_display")
            with fcol4:
                perf_warehouses = get_all_warehouses(st.session_state.conn)
                perf_warehouse = st.selectbox("Warehouse", perf_warehouses, key="perf_warehouse")

            fcol5, fcol6, fcol7, fcol8 = st.columns(4)
            with fcol5:
                perf_dbs = ["All"] + get_databases(st.session_state.conn)
                perf_db = st.selectbox("Database", perf_dbs, key="perf_db")
            with fcol6:
                if perf_db and perf_db != "All":
                    perf_schemas_list = ["All"] + get_schemas(st.session_state.conn, perf_db)
                else:
                    perf_schemas_list = ["All"]
                perf_schema = st.selectbox("Schema", perf_schemas_list, key="perf_schema")
            with fcol7:
                perf_users = get_all_users(st.session_state.conn)
                perf_user = st.selectbox("User", perf_users, key="perf_user")
            with fcol8:
                perf_query_type = st.selectbox(
                    "Query Type",
                    ["All", "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE"],
                    key="perf_query_type"
                )

        # ---- Sub-tabs for Performance Monitoring ----
        perf_tab1, perf_tab2, perf_tab3, perf_tab4, perf_tab5 = st.tabs([
            "👤 User Adoption",
            "⚡ Query Performance",
            "💰 Compute Cost",
            "🗄️ Storage",
            "🏭 Warehouse Activity"
        ])

        # ---- USER ADOPTION ----
        with perf_tab1:
            st.subheader("👤 User Adoption")
            if st.button("🔄 Load User Adoption Data", key="load_ua", type="primary"):
                with st.spinner("Loading user adoption data..."):
                    try:
                        ua_top_users = fetch_top_active_users(
                            st.session_state.conn, perf_start, perf_end,
                            perf_db, perf_schema, perf_query_type
                        )
                        ua_over_time = fetch_active_users_over_time(
                            st.session_state.conn, perf_start, perf_end,
                            perf_db, perf_schema, perf_warehouse, perf_user, perf_query_type
                        )
                        st.session_state.ua_top_users = ua_top_users
                        st.session_state.ua_over_time = ua_over_time
                        st.success("✅ Data loaded!")
                    except Exception as e:
                        st.error(f"❌ Error: {str(e)}")

            ua_col1, ua_col2 = st.columns(2)

            with ua_col1:
                st.markdown("#### Top 10 Active Users")
                if 'ua_top_users' in st.session_state and not st.session_state.ua_top_users.empty:
                    st.dataframe(st.session_state.ua_top_users, use_container_width=True)
                    csv = st.session_state.ua_top_users.to_csv(index=False)
                    st.download_button("📥 Download", csv, f"top_users_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_ua_users")
                    if MATPLOTLIB_AVAILABLE:
                        fig, ax = plt.subplots(figsize=(8, 4))
                        df_plot = st.session_state.ua_top_users.copy()
                        if "Query Count" in df_plot.columns and "User" in df_plot.columns:
                            df_plot = df_plot.sort_values("Query Count", ascending=True).tail(10)
                            ax.barh(df_plot["User"], pd.to_numeric(df_plot["Query Count"], errors='coerce').fillna(0), color='steelblue')
                            ax.set_xlabel("Query Count")
                            ax.set_title("Top Users by Query Count")
                            plt.tight_layout()
                            st.pyplot(fig)
                            plt.close()
                else:
                    st.info("Click 'Load User Adoption Data' to see results.")

            with ua_col2:
                st.markdown("#### Active Users Over Time")
                if 'ua_over_time' in st.session_state and not st.session_state.ua_over_time.empty:
                    st.dataframe(st.session_state.ua_over_time, use_container_width=True)
                    csv = st.session_state.ua_over_time.to_csv(index=False)
                    st.download_button("📥 Download", csv, f"users_over_time_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_ua_time")
                    if MATPLOTLIB_AVAILABLE:
                        fig, ax = plt.subplots(figsize=(8, 4))
                        df_plot = st.session_state.ua_over_time.copy()
                        if "Date" in df_plot.columns and "Active Users" in df_plot.columns:
                            df_plot["Date"] = pd.to_datetime(df_plot["Date"])
                            df_plot["Active Users"] = pd.to_numeric(df_plot["Active Users"], errors='coerce').fillna(0)
                            df_plot = df_plot.sort_values("Date")
                            ax.plot(df_plot["Date"], df_plot["Active Users"], marker='o', color='purple')
                            ax.set_xlabel("Date")
                            ax.set_ylabel("Active Users")
                            ax.set_title("Active Users Over Time")
                            ax.grid(True)
                            plt.xticks(rotation=45)
                            plt.tight_layout()
                            st.pyplot(fig)
                            plt.close()
                else:
                    st.info("Click 'Load User Adoption Data' to see results.")

        # ---- QUERY PERFORMANCE ----
        with perf_tab2:
            st.subheader("⚡ Query Performance")
            if st.button("🔄 Load Query Performance Data", key="load_qp", type="primary"):
                with st.spinner("Loading query performance data..."):
                    try:
                        qp_longest = fetch_longest_running_queries(
                            st.session_state.conn, perf_start, perf_end,
                            perf_db, perf_schema, perf_warehouse, perf_user, perf_query_type
                        )
                        qp_expensive = fetch_expensive_queries(
                            st.session_state.conn, perf_start, perf_end,
                            perf_db, perf_schema, perf_warehouse, perf_user, perf_query_type
                        )
                        qp_frequent = fetch_top_frequent_queries(
                            st.session_state.conn, perf_start, perf_end,
                            perf_db, perf_schema, perf_warehouse, perf_user, perf_query_type
                        )
                        qp_failed = fetch_failed_queries(
                            st.session_state.conn, perf_start, perf_end,
                            perf_db, perf_schema, perf_warehouse, perf_user
                        )
                        st.session_state.qp_longest = qp_longest
                        st.session_state.qp_expensive = qp_expensive
                        st.session_state.qp_frequent = qp_frequent
                        st.session_state.qp_failed = qp_failed
                        st.success("✅ Data loaded!")
                    except Exception as e:
                        st.error(f"❌ Error: {str(e)}")

            qp_sub1, qp_sub2, qp_sub3, qp_sub4 = st.tabs([
                "Longest Running", "Most Expensive", "Most Frequent", "Failed Queries"
            ])

            with qp_sub1:
                if 'qp_longest' in st.session_state and not st.session_state.qp_longest.empty:
                    st.dataframe(st.session_state.qp_longest, use_container_width=True)
                    csv = st.session_state.qp_longest.to_csv(index=False)
                    st.download_button("📥 Download", csv, f"longest_queries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_qp_long")
                    if MATPLOTLIB_AVAILABLE:
                        df_plot = st.session_state.qp_longest.copy()
                        if "Query ID" in df_plot.columns and "Exec Time (s)" in df_plot.columns:
                            fig, ax = plt.subplots(figsize=(10, 5))
                            df_plot = df_plot.sort_values("Exec Time (s)", ascending=True).tail(10)
                            ax.barh(df_plot["Query ID"].astype(str), pd.to_numeric(df_plot["Exec Time (s)"], errors='coerce').fillna(0), color='tomato')
                            ax.set_xlabel("Execution Time (s)")
                            ax.set_title("Top 10 Longest Running Queries")
                            plt.tight_layout()
                            st.pyplot(fig)
                            plt.close()
                else:
                    st.info("Click 'Load Query Performance Data' to see results.")

            with qp_sub2:
                if 'qp_expensive' in st.session_state and not st.session_state.qp_expensive.empty:
                    st.dataframe(st.session_state.qp_expensive, use_container_width=True)
                    csv = st.session_state.qp_expensive.to_csv(index=False)
                    st.download_button("📥 Download", csv, f"expensive_queries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_qp_exp")
                    if MATPLOTLIB_AVAILABLE:
                        df_plot = st.session_state.qp_expensive.copy()
                        if "Query ID" in df_plot.columns and "Credits Consumed" in df_plot.columns:
                            fig, ax = plt.subplots(figsize=(10, 5))
                            df_plot = df_plot.sort_values("Credits Consumed", ascending=True).tail(10)
                            ax.barh(df_plot["Query ID"].astype(str), pd.to_numeric(df_plot["Credits Consumed"], errors='coerce').fillna(0), color='salmon')
                            ax.set_xlabel("Credits Consumed")
                            ax.set_title("Top 10 Expensive Queries")
                            plt.tight_layout()
                            st.pyplot(fig)
                            plt.close()
                else:
                    st.info("Click 'Load Query Performance Data' to see results.")

            with qp_sub3:
                if 'qp_frequent' in st.session_state and not st.session_state.qp_frequent.empty:
                    st.dataframe(st.session_state.qp_frequent, use_container_width=True)
                    csv = st.session_state.qp_frequent.to_csv(index=False)
                    st.download_button("📥 Download", csv, f"frequent_queries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_qp_freq")
                else:
                    st.info("Click 'Load Query Performance Data' to see results.")

            with qp_sub4:
                if 'qp_failed' in st.session_state and not st.session_state.qp_failed.empty:
                    st.dataframe(st.session_state.qp_failed, use_container_width=True)
                    csv = st.session_state.qp_failed.to_csv(index=False)
                    st.download_button("📥 Download", csv, f"failed_queries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_qp_fail")
                    st.metric("Total Failed Queries", len(st.session_state.qp_failed))
                else:
                    st.info("Click 'Load Query Performance Data' to see results.")

        # ---- COMPUTE COST ----
        with perf_tab3:
            st.subheader("💰 Compute Cost Analysis")
            if st.button("🔄 Load Cost Data", key="load_cc", type="primary"):
                with st.spinner("Loading cost data..."):
                    try:
                        cc_credits = fetch_warehouse_credits(
                            st.session_state.conn, perf_start, perf_end, perf_warehouse
                        )
                        cc_over_time, cc_date_col = fetch_credit_usage_over_time(
                            st.session_state.conn, perf_start, perf_end, perf_warehouse
                        )
                        cc_heatmap = fetch_cost_heatmap_data(
                            st.session_state.conn, perf_start, perf_end, perf_warehouse
                        )
                        st.session_state.cc_credits = cc_credits
                        st.session_state.cc_over_time = cc_over_time
                        st.session_state.cc_date_col = cc_date_col
                        st.session_state.cc_heatmap = cc_heatmap
                        st.success("✅ Data loaded!")
                    except Exception as e:
                        st.error(f"❌ Error: {str(e)}")

            if 'cc_credits' in st.session_state and not st.session_state.cc_credits.empty:
                # KPI Summary
                total_credits = pd.to_numeric(st.session_state.cc_credits.get("Credits Used", pd.Series()), errors='coerce').sum()
                st.markdown(f"""
                <div class="kpi-card">
                    <strong>Total Credits Used:</strong> {total_credits:,.2f} &nbsp;&nbsp;
                    <strong>Estimated Cost (@ $3/credit):</strong> ${total_credits * 3:,.2f}
                </div>
                """, unsafe_allow_html=True)

            cc_sub1, cc_sub2, cc_sub3 = st.tabs(["Credit Usage Over Time", "Warehouse Breakdown", "Cost Heatmap"])

            with cc_sub1:
                if 'cc_over_time' in st.session_state and not st.session_state.cc_over_time.empty:
                    st.dataframe(st.session_state.cc_over_time, use_container_width=True)
                    csv = st.session_state.cc_over_time.to_csv(index=False)
                    st.download_button("📥 Download", csv, f"credit_usage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_cc_time")
                    if MATPLOTLIB_AVAILABLE:
                        df_plot = st.session_state.cc_over_time.copy()
                        date_col = st.session_state.cc_date_col
                        if date_col in df_plot.columns and "Credits Used" in df_plot.columns:
                            fig, ax = plt.subplots(figsize=(10, 4))
                            credits_vals = pd.to_numeric(df_plot["Credits Used"], errors='coerce').fillna(0)
                            ax.fill_between(range(len(df_plot)), credits_vals, color='lightgreen', alpha=0.7)
                            ax.plot(range(len(df_plot)), credits_vals, color='darkgreen', marker='o')
                            ax.set_xticks(range(len(df_plot)))
                            ax.set_xticklabels(df_plot[date_col].astype(str), rotation=45, ha='right')
                            ax.set_ylabel("Credits Used")
                            ax.set_title(f"Credit Usage Over Time ({date_col})")
                            ax.grid(True, alpha=0.3)
                            plt.tight_layout()
                            st.pyplot(fig)
                            plt.close()
                else:
                    st.info("Click 'Load Cost Data' to see results.")

            with cc_sub2:
                if 'cc_credits' in st.session_state and not st.session_state.cc_credits.empty:
                    st.dataframe(st.session_state.cc_credits, use_container_width=True)
                    csv = st.session_state.cc_credits.to_csv(index=False)
                    st.download_button("📥 Download", csv, f"warehouse_credits_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_cc_wh")
                    if MATPLOTLIB_AVAILABLE:
                        df_plot = st.session_state.cc_credits.copy()
                        if "Warehouse" in df_plot.columns and "Compute Credits" in df_plot.columns:
                            fig, ax = plt.subplots(figsize=(10, 5))
                            wh_grp = df_plot.groupby("Warehouse")[["Compute Credits", "Cloud Services Credits"]].sum()
                            wh_grp = wh_grp.apply(pd.to_numeric, errors='coerce').fillna(0)
                            wh_grp.plot(kind='bar', stacked=True, ax=ax, cmap='coolwarm')
                            ax.set_xlabel("Warehouse")
                            ax.set_ylabel("Credits")
                            ax.set_title("Cost Breakdown by Warehouse")
                            plt.xticks(rotation=45, ha='right')
                            plt.tight_layout()
                            st.pyplot(fig)
                            plt.close()
                else:
                    st.info("Click 'Load Cost Data' to see results.")

            with cc_sub3:
                if 'cc_heatmap' in st.session_state and not st.session_state.cc_heatmap.empty and MATPLOTLIB_AVAILABLE:
                    df_heat = st.session_state.cc_heatmap.copy()
                    if "DayOfWeek" in df_heat.columns and "HourOfDay" in df_heat.columns and "Credits" in df_heat.columns:
                        df_heat["Credits"] = pd.to_numeric(df_heat["Credits"], errors='coerce').fillna(0)
                        day_order = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
                        pivot = df_heat.pivot_table(index='HourOfDay', columns='DayOfWeek', values='Credits', fill_value=0)
                        pivot = pivot.reindex(columns=day_order, fill_value=0)
                        fig, ax = plt.subplots(figsize=(12, 7))
                        sns.heatmap(pivot, cmap="YlGnBu", annot=True, fmt=".1f", linewidths=.5, ax=ax)
                        ax.set_title("Credits by Day of Week & Hour")
                        plt.tight_layout()
                        st.pyplot(fig)
                        plt.close()
                    csv = st.session_state.cc_heatmap.to_csv(index=False)
                    st.download_button("📥 Download Heatmap Data", csv, f"cost_heatmap_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_cc_heat")
                else:
                    st.info("Click 'Load Cost Data' to see results.")

        # ---- STORAGE ----
        with perf_tab4:
            st.subheader("🗄️ Storage Analysis")
            if st.button("🔄 Load Storage Data", key="load_st", type="primary"):
                with st.spinner("Loading storage data..."):
                    try:
                        st_daily = fetch_daily_storage_usage(
                            st.session_state.conn, perf_start, perf_end
                        )
                        st_tables = fetch_table_storage_metrics(
                            st.session_state.conn, 
                            perf_db if perf_db != "All" else None,
                            perf_schema if perf_schema != "All" else None
                        )
                        st.session_state.st_daily = st_daily
                        st.session_state.st_tables = st_tables
                        st.success("✅ Data loaded!")
                    except Exception as e:
                        st.error(f"❌ Error: {str(e)}")

            st_col1, st_col2 = st.columns(2)

            with st_col1:
                st.markdown("#### Daily Storage Usage")
                if 'st_daily' in st.session_state and not st.session_state.st_daily.empty:
                    st.dataframe(st.session_state.st_daily, use_container_width=True)
                    csv = st.session_state.st_daily.to_csv(index=False)
                    st.download_button("📥 Download", csv, f"daily_storage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_st_daily")
                    if MATPLOTLIB_AVAILABLE:
                        df_plot = st.session_state.st_daily.copy()
                        if "Date" in df_plot.columns and "Avg DB Storage (GB)" in df_plot.columns:
                            fig, ax = plt.subplots(figsize=(8, 4))
                            df_plot["Date"] = pd.to_datetime(df_plot["Date"])
                            df_plot["Avg DB Storage (GB)"] = pd.to_numeric(df_plot["Avg DB Storage (GB)"], errors='coerce').fillna(0)
                            ax.plot(df_plot["Date"], df_plot["Avg DB Storage (GB)"], marker='o', color='teal')
                            ax.set_xlabel("Date")
                            ax.set_ylabel("Storage (GB)")
                            ax.set_title("Avg Daily DB Storage (GB)")
                            ax.grid(True)
                            plt.xticks(rotation=45)
                            plt.tight_layout()
                            st.pyplot(fig)
                            plt.close()
                else:
                    st.info("Click 'Load Storage Data' to see results.")

            with st_col2:
                st.markdown("#### Table Storage Details")
                if 'st_tables' in st.session_state and not st.session_state.st_tables.empty:
                    st.dataframe(st.session_state.st_tables, use_container_width=True)
                    csv = st.session_state.st_tables.to_csv(index=False)
                    st.download_button("📥 Download", csv, f"table_storage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_st_tables")
                else:
                    st.info("Select a specific Database and Schema (not 'All') and click 'Load Storage Data' to see table details.")

        # ---- WAREHOUSE ACTIVITY ----
        with perf_tab5:
            st.subheader("🏭 Warehouse Activity")
            if st.button("🔄 Load Warehouse Activity Data", key="load_wa", type="primary"):
                with st.spinner("Loading warehouse activity data..."):
                    try:
                        wa_util = fetch_warehouse_utilization(
                            st.session_state.conn, perf_start, perf_end, perf_warehouse
                        )
                        st.session_state.wa_util = wa_util
                        st.success("✅ Data loaded!")
                    except Exception as e:
                        st.error(f"❌ Error: {str(e)}")

            if 'wa_util' in st.session_state and not st.session_state.wa_util.empty:
                st.dataframe(st.session_state.wa_util, use_container_width=True)
                csv = st.session_state.wa_util.to_csv(index=False)
                st.download_button("📥 Download", csv, f"warehouse_activity_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_wa")

                if MATPLOTLIB_AVAILABLE:
                    df_plot = st.session_state.wa_util.copy()
                    wa_col1, wa_col2 = st.columns(2)
                    with wa_col1:
                        if "Date" in df_plot.columns and "Avg Running" in df_plot.columns:
                            fig, ax = plt.subplots(figsize=(8, 4))
                            df_plot["Date"] = pd.to_datetime(df_plot["Date"])
                            daily_avg = df_plot.groupby("Date")["Avg Running"].mean().reset_index()
                            daily_avg["Avg Running"] = pd.to_numeric(daily_avg["Avg Running"], errors='coerce').fillna(0)
                            ax.plot(daily_avg["Date"], daily_avg["Avg Running"], marker='o', color='royalblue')
                            ax.set_xlabel("Date")
                            ax.set_ylabel("Avg Running Queries")
                            ax.set_title("Daily Avg Running Queries")
                            ax.grid(True)
                            plt.xticks(rotation=45)
                            plt.tight_layout()
                            st.pyplot(fig)
                            plt.close()
                    with wa_col2:
                        if "Warehouse" in df_plot.columns and "Avg Running" in df_plot.columns:
                            fig, ax = plt.subplots(figsize=(8, 4))
                            wh_avg = df_plot.groupby("Warehouse")[["Avg Running", "Avg Queued", "Avg Blocked"]].mean()
                            wh_avg = wh_avg.apply(pd.to_numeric, errors='coerce').fillna(0)
                            wh_avg.plot(kind='bar', ax=ax, cmap='Set2')
                            ax.set_xlabel("Warehouse")
                            ax.set_ylabel("Average Count")
                            ax.set_title("Avg Load by Warehouse")
                            plt.xticks(rotation=45, ha='right')
                            plt.tight_layout()
                            st.pyplot(fig)
                            plt.close()
            else:
                st.info("Click 'Load Warehouse Activity Data' to see results.")


# ========== MAIN EXECUTION ==========
if st.session_state.is_logged_in:
    show_main_app()
else:
    show_login_page()

st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 10px;'>
    <p>DeploySure Suite v2.0 | Powered by Streamlit & Snowflake</p>
</div>
""", unsafe_allow_html=True)