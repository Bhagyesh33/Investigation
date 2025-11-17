# -*- coding: utf-8 -*-
import streamlit as st
import snowflake.connector
import pandas as pd
from datetime import datetime, timedelta
import re
import os
import numpy as np
from matplotlib import pyplot as plt
import matplotlib.pyplot as plt
from typing import List, Dict, Tuple
import time
import logging
import traceback
import base64

# Configure logging for better debugging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Page configuration
st.set_page_config(
    page_title="DeploySure Suite",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        text-align: center;
        padding: 20px;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 10px;
        margin-bottom: 30px;
    }
    .status-box {
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
        border-left: 4px solid;
    }
    .status-success {
        background-color: #d4edda;
        border-left-color: #28a745;
        color: #155724;
    }
    .status-error {
        background-color: #f8d7da;
        border-left-color: #dc3545;
        color: #721c24;
    }
    .status-warning {
        background-color: #fff3cd;
        border-left-color: #ffc107;
        color: #856404;
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
    }
</style>
""", unsafe_allow_html=True)

# ========== SNOWFLAKE FUNCTIONS ==========
def get_snowflake_connection(user, password, account):
    """Establish connection to Snowflake"""
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
        traceback.print_exc()
        return None, f"❌ Connection failed: {str(e)}"

def disconnect_snowflake(conn):
    """Close Snowflake connection"""
    if conn:
        conn.close()
        logging.info("Disconnected from Snowflake.")
    return None, "🔌 Disconnected successfully"

def get_databases(conn):
    """Get list of databases"""
    if not conn:
        logging.warning("No connection to get databases.")
        return []
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        dbs = [row[1] for row in cursor.fetchall()]
        logging.info(f"Found databases: {dbs}")
        return dbs
    except Exception as e:
        logging.error(f"Error getting databases: {str(e)}")
        traceback.print_exc()
        return []

def get_schemas(conn, database):
    """Get schemas for specific database"""
    if not conn or not database:
        logging.warning("Missing connection or database to get schemas.")
        return []
    try:
        cursor = conn.cursor()
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {database}")
        schemas = [row[1] for row in cursor.fetchall()]
        logging.info(f"Found schemas for {database}: {schemas}")
        return schemas
    except Exception as e:
        logging.error(f"Error getting schemas for {database}: {str(e)}")
        traceback.print_exc()
        return []

def get_tables(conn, database, schema):
    """Get tables for specific schema"""
    if not conn or not database or not schema:
        logging.warning("Missing connection, database, or schema to get tables.")
        return []
    try:
        cursor = conn.cursor()
        cursor.execute(f"SHOW TABLES IN SCHEMA {database}.{schema}")
        tables = [row[1] for row in cursor.fetchall()]
        filtered_tables = [t for t in tables if t.upper() not in ('TEST_CASES', 'ORDER_KPIS')]
        logging.info(f"Found tables for {database}.{schema}: {filtered_tables}")
        return filtered_tables
    except Exception as e:
        logging.error(f"Error getting tables for {database}.{schema}: {str(e)}")
        traceback.print_exc()
        return []

def get_columns_for_table(conn, database, schema, table):
    """Utility function to get columns for a given table for UI dropdowns"""
    if not conn or not database or not schema or not table:
        logging.warning("Missing connection, database, schema, or table to get columns.")
        return []
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COLUMN_NAME
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}'
            AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
        """)
        columns = [row[0] for row in cursor.fetchall()]
        logging.info(f"Found columns for {database}.{schema}.{table}: {columns}")
        return columns
    except Exception as e:
        logging.error(f"Error getting columns for {database}.{schema}.{table}: {str(e)}")
        traceback.print_exc()
        return []

def _get_column_details_for_dq(conn, database, schema, table):
    """Get column names and types for a given table"""
    if not conn or not database or not schema or not table:
        logging.warning("Missing connection, database, schema, or table to get column details for DQ.")
        return []
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}'
            AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
        """)
        columns_details = [{'name': row[0], 'type': row[1].upper()} for row in cursor.fetchall()]
        logging.info(f"Found column details for {database}.{schema}.{table} (DQ): {columns_details}")
        return columns_details
    except Exception as e:
        logging.error(f"Error getting column details for {database}.{schema}.{table} (DQ): {str(e)}")
        traceback.print_exc()
        return []

def _categorize_columns_by_type(column_details_list: List[Dict]) -> Tuple[List[str], List[str], List[str], List[str]]:
    """Categorizes columns into numeric, date, string, and all columns based on their data types."""
    numeric_cols = []
    date_cols = []
    string_cols = []
    all_cols = []

    for col in column_details_list:
        col_name = col['name']
        col_type = col['type']
        all_cols.append(col_name)

        if "NUMBER" in col_type or "INT" in col_type or "FLOAT" in col_type or "DOUBLE" in col_type:
            numeric_cols.append(col_name)
        elif "DATE" in col_type or "TIMESTAMP" in col_type:
            date_cols.append(col_name)
        elif "VARCHAR" in col_type or "TEXT" in col_type or "STRING" in col_type:
            string_cols.append(col_name)
    return all_cols, numeric_cols, date_cols, string_cols

def clone_schema(conn, source_db, source_schema, target_schema):
    """Clone schema with improved error handling and reporting"""
    if not conn:
        return False, "❌ Not connected to Snowflake.", pd.DataFrame()
    if not source_db or not source_schema or not target_schema:
        return False, "⚠️ Please provide Source Database, Source Schema, and a Target Schema name.", pd.DataFrame()

    cursor = conn.cursor()
    try:
        cursor.execute(f"SHOW SCHEMAS LIKE '{source_schema}' IN DATABASE {source_db}")
        if not cursor.fetchall():
            logging.error(f"Source schema {source_db}.{source_schema} doesn't exist.")
            return False, f"❌ Source schema {source_db}.{source_schema} doesn't exist", pd.DataFrame()

        clone_sql = f"CREATE OR REPLACE SCHEMA {source_db}.{target_schema} CLONE {source_db}.{source_schema}"
        logging.info(f"Executing clone SQL: {clone_sql}")
        cursor.execute(clone_sql)

        cursor.execute(f"SHOW SCHEMAS LIKE '{target_schema}' IN DATABASE {source_db}")
        if not cursor.fetchall():
            logging.error(f"Clone failed - target schema {source_db}.{target_schema} not created.")
            return False, f"❌ Clone failed - target schema not created", pd.DataFrame()

        cursor.execute(f"SHOW TABLES IN SCHEMA {source_db}.{source_schema}")
        source_tables = [row[1] for row in cursor.fetchall()]

        cursor.execute(f"SHOW TABLES IN SCHEMA {source_db}.{target_schema}")
        clone_tables = [row[1] for row in cursor.fetchall()]

        df_tables = pd.DataFrame({
            'Database': source_db,
            'Source Schema': source_schema,
            'Clone Schema': target_schema,
            'Source Tables': len(source_tables),
            'Cloned Tables': len(clone_tables),
            'Status': '✅ Success' if len(source_tables) == len(clone_tables) else '⚠️ Partial Success'
        }, index=[0])

        logging.info(f"Successfully mirrored schema {source_db}.{source_schema} to {source_db}.{target_schema}")
        return True, f"✅ Successfully Mirrored Schema {source_db}.{source_schema} to {source_db}.{target_schema}", df_tables
    except Exception as e:
        logging.error(f"Clone failed: {str(e)}")
        traceback.print_exc()
        return False, f"❌ Clone failed: {str(e)}", pd.DataFrame()

def compare_table_differences(conn, db_name, source_schema, clone_schema):
    """Compare tables between schemas"""
    if not conn:
        return pd.DataFrame()
    cursor = conn.cursor()

    query = f"""
    WITH source_tables AS (
        SELECT table_name
        FROM {db_name}.information_schema.tables
        WHERE table_schema = '{source_schema}'
    ),
    clone_tables AS (
        SELECT table_name
        FROM {db_name}.information_schema.tables
        WHERE table_schema = '{clone_schema}'
    )
    SELECT
        COALESCE(s.table_name, c.table_name) AS table_name,
        CASE
            WHEN s.table_name IS NULL THEN 'Missing in source - Table Dropped'
            WHEN c.table_name IS NULL THEN 'Missing in clone - Table Added'
            ELSE 'Present in both'
        END AS difference
    FROM source_tables s
    FULL OUTER JOIN clone_tables c ON s.table_name = c.table_name
    WHERE s.table_name IS NULL OR c.table_name IS NULL
    ORDER BY difference, table_name;
    """
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=['Table', 'Difference'])
        logging.info(f"Table differences found: {len(df)} rows.")
        return df
    except Exception as e:
        logging.error(f"Error comparing table differences: {str(e)}")
        traceback.print_exc()
        return pd.DataFrame()

def compare_column_differences(conn, db_name, source_schema, clone_schema):
    """Compare columns and data types between schemas"""
    if not conn:
        return pd.DataFrame(), pd.DataFrame()
    cursor = conn.cursor()

    common_tables_query = f"""
    SELECT s.table_name
    FROM {db_name}.information_schema.tables s
    JOIN {db_name}.information_schema.tables c
        ON s.table_name = c.table_name
    WHERE s.table_schema = '{source_schema}'
    AND c.table_schema = '{clone_schema}';
    """
    try:
        cursor.execute(common_tables_query)
        common_tables = [row[0] for row in cursor.fetchall()]
    except Exception as e:
        logging.error(f"Error getting common tables for column comparison: {str(e)}")
        traceback.print_exc()
        return pd.DataFrame(), pd.DataFrame()

    column_diff_data = []
    datatype_diff_data = []

    for table in common_tables:
        try:
            cursor.execute(f"DESCRIBE TABLE {db_name}.{source_schema}.{table}")
            source_desc = cursor.fetchall()
            source_cols = {row[0]: row[1] for row in source_desc}

            cursor.execute(f"DESCRIBE TABLE {db_name}.{clone_schema}.{table}")
            clone_desc = cursor.fetchall()
            clone_cols = {row[0]: row[1] for row in clone_desc}

            all_columns = set(source_cols.keys()).union(set(clone_cols.keys()))

            for col in all_columns:
                source_exists = col in source_cols
                clone_exists = col in clone_cols

                if not source_exists:
                    column_diff_data.append({
                        'Table': table,
                        'Column': col,
                        'Difference': 'Missing in source - Column Dropped',
                        'Source Data Type': None,
                        'Clone Data Type': clone_cols.get(col)
                    })
                elif not clone_exists:
                    column_diff_data.append({
                        'Table': table,
                        'Column': col,
                        'Difference': 'Missing in clone - Column Added',
                        'Source Data Type': source_cols.get(col),
                        'Clone Data Type': None
                    })
                else:
                    if source_cols[col] != clone_cols[col]:
                        datatype_diff_data.append({
                            'Table': table,
                            'Column': col,
                            'Source Data Type': source_cols[col],
                            'Clone Data Type': clone_cols[col],
                            'Difference': 'Data Type Changed'
                        })
        except Exception as e:
            logging.error(f"Error processing table {table} for column differences: {str(e)}")
            traceback.print_exc()
            continue

    column_diff_df = pd.DataFrame(column_diff_data)
    if not column_diff_df.empty:
        column_diff_df = column_diff_df[['Table', 'Column', 'Difference', 'Source Data Type', 'Clone Data Type']]

    datatype_diff_df = pd.DataFrame(datatype_diff_data)
    if not datatype_diff_df.empty:
        datatype_diff_df = datatype_diff_df[['Table', 'Column', 'Source Data Type', 'Clone Data Type', 'Difference']]

    logging.info(f"Column differences found: {len(column_diff_df)} rows. Datatype differences found: {len(datatype_diff_df)} rows.")
    return column_diff_df, datatype_diff_df

def get_test_case_tables(conn, database, schema):
    """Get distinct tables from test cases table with error handling"""
    if not conn or not database or not schema:
        return ["All"]
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM {database}.information_schema.tables
            WHERE table_schema = '{schema}'
            AND table_name = 'TEST_CASES'
        """)
        if cursor.fetchone()[0] == 0:
            logging.warning(f"TEST_CASES table not found in {database}.{schema}")
            return ["All"]

        cursor.execute(f"""
            SELECT DISTINCT TABLE_NAME
            FROM {database}.{schema}.TEST_CASES
            WHERE TABLE_NAME IS NOT NULL
            ORDER BY TABLE_NAME
        """)
        tables = [row[0] for row in cursor.fetchall()]
        logging.info(f"Found test case tables: {tables}")
        return ["All"] + tables
    except Exception as e:
        logging.error(f"Error getting test case tables: {str(e)}")
        traceback.print_exc()
        return ["All"]

def get_test_cases(conn, database, schema, table):
    """Get test cases for specific table with error handling"""
    if not conn or not database or not schema:
        return []
    try:
        cursor = conn.cursor()

        cursor.execute(f"""
            SELECT COUNT(*)
            FROM {database}.information_schema.tables
            WHERE table_schema = '{schema}'
            AND table_name = 'TEST_CASES'
        """)
        if cursor.fetchone()[0] == 0:
            logging.warning(f"TEST_CASES table not found in {database}.{schema}")
            return []

        if table == "All":
            query = f"""
                SELECT
                    TEST_CASE_ID,
                    TEST_ABBREVIATION,
                    TABLE_NAME,
                    TEST_DESCRIPTION,
                    SQL_CODE,
                    EXPECTED_RESULT
                FROM {database}.{schema}.TEST_CASES
                ORDER BY TEST_CASE_ID
            """
        else:
            query = f"""
                SELECT
                    TEST_CASE_ID,
                    TEST_ABBREVIATION,
                    TABLE_NAME,
                    TEST_DESCRIPTION,
                    SQL_CODE,
                    EXPECTED_RESULT
                FROM {database}.{schema}.TEST_CASES
                WHERE TABLE_NAME = '{table}'
                ORDER BY TEST_CASE_ID
            """
        logging.info(f"Fetching test cases with query: {query}")
        cursor.execute(query)
        cases = cursor.fetchall()
        logging.info(f"Found {len(cases)} test cases for {database}.{schema}.{table}")
        return cases
    except Exception as e:
        logging.error(f"Error getting test cases: {str(e)}")
        traceback.print_exc()
        return []

def validate_test_cases(conn, database, schema, test_cases):
    """Executes selected test cases and returns results."""
    if not conn:
        return pd.DataFrame(), "❌ Not connected to Snowflake."
    if not test_cases:
        return pd.DataFrame(), "⚠️ No test cases selected"

    cursor = conn.cursor()
    results = []

    for case in test_cases:
        test_id, abbrev, table_name, desc, sql, expected = case
        expected = str(expected).strip()
        logging.info(f"Validating test case: {abbrev} for table {table_name}")

        try:
            qualified_sql = re.sub(
                rf'\b{re.escape(table_name)}\b',
                f'{database}.{schema}.{table_name}',
                sql,
                flags=re.IGNORECASE
            )
            logging.info(f"Executing test case SQL: {qualified_sql}")
            cursor.execute(qualified_sql)
            result = cursor.fetchone()
            actual_result = str(result[0]) if result else "0"

            status = "✅ PASS" if actual_result == expected else "❌ FAIL"
            results.append({
                'TEST CASE': abbrev,
                'CATEGORY': table_name,
                'EXPECTED RESULT': expected,
                'ACTUAL RESULT': actual_result,
                'STATUS': status
            })
            logging.info(f"Test case {abbrev} status: {status}")

        except Exception as e:
            error_msg = str(e).split('\n')[0]
            results.append({
                'TEST CASE': abbrev,
                'CATEGORY': table_name,
                'EXPECTED RESULT': expected,
                'ACTUAL RESULT': f"QUERY ERROR: {error_msg}",
                'STATUS': "❌ EXECUTION ERROR"
            })
            logging.error(f"Error executing test case {abbrev}: {e}")
            traceback.print_exc()

    df = pd.DataFrame(results)
    return df, "✅ Validation completed"

# ===== DATA QUALITY VALIDATION CLASS =====
class DataQualityValidator:
    def __init__(self, conn):
        self.conn = conn

    def _execute_query(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        return pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

    def _get_table_columns(self, database, schema, table):
        return _get_column_details_for_dq(self.conn, database, schema, table)

    def _get_column_details(self, database, schema, table):
        query = f"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM {database}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'"
        df = self._execute_query(query)
        return {row['COLUMN_NAME'].upper(): row.to_dict() for _, row in df.iterrows()}

    def _run_row_count_check(self, database, schema, table, min_rows):
        query = f"SELECT COUNT(*) FROM {database}.{schema}.{table}"
        count = self._execute_query(query).iloc[0, 0]
        status = "✅ Pass" if count >= min_rows else "❌ Fail"
        details = f"Actual rows: {count}, Minimum expected: {min_rows}"
        return {"Check": "Row Count Check", "Column Name": "N/A", "Expected": f">= {min_rows}", "Actual": count, "Status": status, "Details": details}

    def _run_duplicate_rows_check(self, database, schema, table):
        columns_details = self._get_table_columns(database, schema, table)
        if not columns_details:
            return {"Check": "Duplicate Rows Check", "Column Name": "All Columns", "Expected": "0", "Actual": "N/A", "Status": "⚠️ N/A", "Details": "No columns found to check for duplicates."}

        cols_str = ", ".join([f'"{col["name"]}"' for col in columns_details])
        query = f"""
        SELECT COUNT(*)
        FROM (
            SELECT {cols_str}
            FROM {database}.{schema}.{table}
            GROUP BY {cols_str}
            HAVING COUNT(*) > 1
        )
        """
        duplicate_count = self._execute_query(query).iloc[0, 0]
        status = "✅ Pass" if duplicate_count == 0 else "❌ Fail"
        details = f"Number of duplicate rows: {duplicate_count}"
        return {"Check": "Duplicate Rows Check", "Column Name": "All Columns", "Expected": "0", "Actual": duplicate_count, "Status": status, "Details": details}

    def run_dq_checks(self, conn, database, schema, table, **kwargs):
        self.conn = conn
        detailed_results = []
        status_message = "✅ Data quality checks completed."
        overall_score = 100

        total_checks = 0
        passed_checks = 0
        failed_checks = 0
        error_checks = 0

        if not (conn and database and schema and table):
            return pd.DataFrame(), pd.DataFrame(), "❌ Please select database, schema, and table to run DQ checks.", "Quality Score: N/A", None

        try:
            def process_check_result(res_or_list, penalty):
                nonlocal total_checks, passed_checks, failed_checks, error_checks, overall_score
                if isinstance(res_or_list, list):
                    for res in res_or_list:
                        if res.get("Status") != "⚠️ Skip":
                            total_checks += 1
                            detailed_results.append(res)
                            if res["Status"] == "✅ Pass":
                                passed_checks += 1
                            elif res["Status"] == "❌ Fail":
                                failed_checks += 1
                                overall_score -= penalty
                            elif res["Status"] == "❌ Error":
                                error_checks += 1
                                overall_score -= penalty
                elif res_or_list is not None and res_or_list.get("Status") != "⚠️ Skip":
                    total_checks += 1
                    detailed_results.append(res_or_list)
                    if res_or_list["Status"] == "✅ Pass":
                        passed_checks += 1
                    elif res_or_list["Status"] == "❌ Fail":
                        failed_checks += 1
                        overall_score -= penalty
                    elif res_or_list["Status"] == "❌ Error":
                        error_checks += 1
                        overall_score -= penalty

            if kwargs.get('dq_check_row_count'):
                res = self._run_row_count_check(database, schema, table, kwargs.get('dq_min_rows', 1))
                process_check_result(res, 10)

            if kwargs.get('dq_check_duplicate_rows'):
                res = self._run_duplicate_rows_check(database, schema, table)
                process_check_result(res, 10)

            overall_score = max(0, overall_score)

            summary_data = [
                {"Metric": "Table", "Value": f"{database}.{schema}.{table}"},
                {"Metric": "Total Checks", "Value": total_checks},
                {"Metric": "Passed Checks", "Value": passed_checks},
                {"Metric": "Failed Checks", "Value": failed_checks},
                {"Metric": "Error Checks", "Value": error_checks},
                {"Metric": "Quality Score", "Value": f"{overall_score:.2f}%"}
            ]
            summary_df = pd.DataFrame(summary_data)
            detailed_df = pd.DataFrame(detailed_results, columns=["Check", "Column Name", "Expected", "Actual", "Status", "Details"])

            score_html = f"{overall_score:.0f}/100"
            
            fig, ax = plt.subplots(figsize=(8, 5))
            if total_checks > 0:
                status_counts_data = {
                    "Passed": passed_checks,
                    "Failed": failed_checks,
                    "Error": error_checks,
                }
                status_counts_series = pd.Series(status_counts_data)
                status_counts_series = status_counts_series[status_counts_series > 0]

                if not status_counts_series.empty:
                    colors = {'Passed': 'green', 'Failed': 'red', 'Error': 'gray'}
                    plot_colors = [colors[status] for status in status_counts_series.index if status in colors]
                    status_counts_series.plot(kind='bar', ax=ax, color=plot_colors)
                    ax.set_title('Data Quality Check Status Summary')
                    ax.set_ylabel('Number of Checks')
                    ax.set_xlabel('Status')
                    plt.xticks(rotation=45, ha='right')
                else:
                    ax.text(0.5, 0.5, "No checks performed or no results.", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes)
                    ax.set_title('Data Quality Check Status Summary')
                    ax.set_xticks([])
                    ax.set_yticks([])
            else:
                ax.text(0.5, 0.5, "No checks performed or no results.", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes)
                ax.set_title('Data Quality Check Status Summary')
                ax.set_xticks([])
                ax.set_yticks([])

            plt.tight_layout()

            return summary_df, detailed_df, status_message, score_html, fig

        except Exception as e:
            logging.error(f"An unhandled error occurred during DQ checks: {str(e)}")
            traceback.print_exc()
            return pd.DataFrame(), pd.DataFrame(), f"❌ An unexpected error occurred: {str(e)}", "Quality Score: N/A", None

# ===== SESSION STATE INITIALIZATION =====
if 'conn' not in st.session_state:
    st.session_state.conn = None
if 'is_logged_in' not in st.session_state:
    st.session_state.is_logged_in = False
if 'validation_type' not in st.session_state:
    st.session_state.validation_type = "Schema Validation"

# ===== HEADER =====
st.markdown("""
<div class="main-header">
    <h1>🔧 DeploySure Suite</h1>
    <p>Snowflake Data Validation & Quality Management</p>
</div>
""", unsafe_allow_html=True)

# ===== SIDEBAR - LOGIN =====
with st.sidebar:
    st.header("🔐 Snowflake Connection")
    
    if not st.session_state.is_logged_in:
        with st.form("login_form"):
            user = st.text_input("Username", placeholder="your_username")
            password = st.text_input("Password", type="password", placeholder="••••••••")
            account = st.text_input("Account", placeholder="account.region")
            
            col1, col2 = st.columns(2)
            with col1:
                login_btn = st.form_submit_button("Connect", use_container_width=True)
            
            if login_btn:
                if user and password and account:
                    with st.spinner("Connecting to Snowflake..."):
                        conn, msg = get_snowflake_connection(user, password, account)
                        st.session_state.conn = conn
                        st.session_state.is_logged_in = (conn is not None)
                        
                        if st.session_state.is_logged_in:
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)
                else:
                    st.warning("Please fill in all fields")
    else:
        st.success(f"✅ Connected to Snowflake")
        if st.button("Disconnect", use_container_width=True):
            st.session_state.conn.close()
            st.session_state.conn = None
            st.session_state.is_logged_in = False
            st.rerun()

# ===== MAIN APPLICATION =====
if st.session_state.is_logged_in:
    # Create tabs
    tab1, tab2 = st.tabs(["⎘ MirrorSchema", "🔍 DriftWatch"])
    
    # ===== TAB 1: MIRROR SCHEMA =====
    with tab1:
        st.header("Mirror Schema")
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.subheader("Source Selection")
            
            databases = get_databases(st.session_state.conn)
            source_db = st.selectbox("Source Database", databases, key="mirror_source_db")
            
            if source_db:
                schemas = get_schemas(st.session_state.conn, source_db)
                source_schema = st.selectbox("Source Schema", schemas, key="mirror_source_schema")
                
                if source_schema:
                    target_schema = st.text_input(
                        "MirrorSchema Name", 
                        value=f"{source_schema}_CLONE",
                        key="mirror_target_schema"
                    )
                    
                    if st.button("Execute MirrorSchema", type="primary", use_container_width=True):
                        with st.spinner("Mirroring schema..."):
                            success, msg, df = clone_schema(
                                st.session_state.conn, 
                                source_db, 
                                source_schema, 
                                target_schema
                            )
                            
                            if success:
                                st.success(msg)
                                if not df.empty:
                                    st.dataframe(df, use_container_width=True)
                            else:
                                st.error(msg)
        
        with col2:
            st.subheader("Status")
            st.info("Select source database and schema, then provide a target schema name to mirror.")
    
    # ===== TAB 2: DRIFTWATCH =====
    with tab2:
        st.header("DriftWatch")
        
        # Validation Type Selection
        validation_type = st.selectbox(
            "Validation Type",
            ["Schema Validation", "KPI Validation", "Test Case Validation", "Data Quality Validation"],
            key="validation_type_selector"
        )
        
        st.session_state.validation_type = validation_type
        
        # ===== SCHEMA VALIDATION =====
        if validation_type == "Schema Validation":
            col1, col2 = st.columns([1, 2])
            
            with col1:
                st.subheader("Configuration")
                
                databases = get_databases(st.session_state.conn)
                val_db = st.selectbox("Database", databases, key="schema_val_db")
                
                if val_db:
                    schemas = get_schemas(st.session_state.conn, val_db)
                    val_source_schema = st.selectbox("Source Schema", schemas, key="schema_val_source")
                    val_target_schema = st.selectbox("Target Schema", schemas, key="schema_val_target", index=1 if len(schemas) > 1 else 0)
                    
                    if st.button("Execute DriftWatch", type="primary", use_container_width=True):
                        with st.spinner("Running validation..."):
                            table_diff = compare_table_differences(st.session_state.conn, val_db, val_source_schema, val_target_schema)
                            column_diff, datatype_diff = compare_column_differences(st.session_state.conn, val_db, val_source_schema, val_target_schema)
                            
                            st.session_state.table_diff = table_diff
                            st.session_state.column_diff = column_diff
                            st.session_state.datatype_diff = datatype_diff
            
            with col2:
                st.subheader("ChangeLens / Schema Validation Report")
                
                tab_tables, tab_columns, tab_datatypes = st.tabs(["Table Differences", "Column Differences", "Data Type Differences"])
                
                with tab_tables:
                    if 'table_diff' in st.session_state and not st.session_state.table_diff.empty:
                        st.dataframe(st.session_state.table_diff, use_container_width=True)
                        
                        csv = st.session_state.table_diff.to_csv(index=False)
                        st.download_button(
                            "📥 Download Table Differences",
                            csv,
                            f"Table_Differences_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            "text/csv",
                            use_container_width=True
                        )
                    else:
                        st.info("No table differences found or validation not run yet.")
                
                with tab_columns:
                    if 'column_diff' in st.session_state and not st.session_state.column_diff.empty:
                        st.dataframe(st.session_state.column_diff, use_container_width=True)
                        
                        csv = st.session_state.column_diff.to_csv(index=False)
                        st.download_button(
                            "📥 Download Column Differences",
                            csv,
                            f"Column_Differences_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            "text/csv",
                            use_container_width=True
                        )
                    else:
                        st.info("No column differences found or validation not run yet.")
                
                with tab_datatypes:
                    if 'datatype_diff' in st.session_state and not st.session_state.datatype_diff.empty:
                        st.dataframe(st.session_state.datatype_diff, use_container_width=True)
                        
                        csv = st.session_state.datatype_diff.to_csv(index=False)
                        st.download_button(
                            "📥 Download Data Type Differences",
                            csv,
                            f"Datatype_Differences_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            "text/csv",
                            use_container_width=True
                        )
                    else:
                        st.info("No data type differences found or validation not run yet.")
        
        # ===== KPI VALIDATION =====
        elif validation_type == "KPI Validation":
            col1, col2 = st.columns([1, 2])
            
            with col1:
                st.subheader("Configuration")
                
                databases = get_databases(st.session_state.conn)
                kpi_db = st.selectbox("Database", databases, key="kpi_db")
                
                if kpi_db:
                    schemas = get_schemas(st.session_state.conn, kpi_db)
                    kpi_source_schema = st.selectbox("Source Schema", schemas, key="kpi_source")
                    kpi_target_schema = st.selectbox("Target Schema", schemas, key="kpi_target", index=1 if len(schemas) > 1 else 0)
                    
                    st.subheader("Select KPIs to Validate")
                    
                    kpi_select_all = st.checkbox("Select All", value=True, key="kpi_select_all")
                    
                    col_a, col_b, col_c = st.columns(3)
                    with col_a:
                        kpi_total_orders = st.checkbox("Total Orders", value=kpi_select_all, key="kpi_total_orders")
                        kpi_total_revenue = st.checkbox("Total Revenue", value=kpi_select_all, key="kpi_total_revenue")
                        kpi_avg_order = st.checkbox("Average Order Value", value=kpi_select_all, key="kpi_avg_order")
                    
                    with col_b:
                        kpi_max_order = st.checkbox("Maximum Order Value", value=kpi_select_all, key="kpi_max_order")
                        kpi_min_order = st.checkbox("Minimum Order Value", value=kpi_select_all, key="kpi_min_order")
                        kpi_completed = st.checkbox("Completed Orders", value=kpi_select_all, key="kpi_completed")
                    
                    with col_c:
                        kpi_cancelled = st.checkbox("Cancelled Orders", value=kpi_select_all, key="kpi_cancelled")
                        kpi_april_orders = st.checkbox("Orders in April 2025", value=kpi_select_all, key="kpi_april_orders")
                        kpi_unique_customers = st.checkbox("Unique Customers", value=kpi_select_all, key="kpi_unique_customers")
                    
                    if st.button("Execute DriftWatch", type="primary", use_container_width=True):
                        with st.spinner("Validating KPIs..."):
                            # This would need the validate_kpis function which wasn't in the original code
                            st.info("KPI validation function needs to be implemented")
            
            with col2:
                st.subheader("ChangeLens / KPI Validation Report")
                st.info("Run KPI validation to see results")
        
        # ===== TEST CASE VALIDATION =====
        elif validation_type == "Test Case Validation":
            col1, col2 = st.columns([1, 2])
            
            with col1:
                st.subheader("Test Automation")
                
                databases = get_databases(st.session_state.conn)
                tc_db = st.selectbox("Database", databases, key="tc_db")
                
                if tc_db:
                    schemas = get_schemas(st.session_state.conn, tc_db)
                    tc_schema = st.selectbox("Schema", schemas, key="tc_schema")
                    
                    if tc_schema:
                        tables = get_test_case_tables(st.session_state.conn, tc_db, tc_schema)
                        tc_table = st.selectbox("Category", tables, key="tc_table")
                        
                        test_cases = get_test_cases(st.session_state.conn, tc_db, tc_schema, tc_table)
                        
                        st.subheader("Select Test Cases")
                        tc_select_all = st.checkbox("Select All", value=True, key="tc_select_all")
                        
                        test_case_names = [f"{case[1]}" for case in test_cases]
                        
                        if tc_select_all:
                            selected_cases = st.multiselect(
                                "Available Test Cases",
                                test_case_names,
                                default=test_case_names,
                                key="tc_selected_cases"
                            )
                        else:
                            selected_cases = st.multiselect(
                                "Available Test Cases",
                                test_case_names,
                                key="tc_selected_cases_manual"
                            )
                        
                        if st.button("Execute DriftWatch", type="primary", use_container_width=True):
                            if selected_cases:
                                with st.spinner("Running test cases..."):
                                    selected_test_cases = []
                                    for name in selected_cases:
                                        for case in test_cases:
                                            if case[1] == name:
                                                selected_test_cases.append(case)
                                                break
                                    
                                    df, msg = validate_test_cases(st.session_state.conn, tc_db, tc_schema, selected_test_cases)
                                    st.session_state.test_results = df
                                    
                                    if not df.empty:
                                        st.success(msg)
                                    else:
                                        st.warning(msg)
                            else:
                                st.warning("Please select at least one test case")
            
            with col2:
                st.subheader("ChangeLens / Test Automation Report")
                
                if 'test_results' in st.session_state and not st.session_state.test_results.empty:
                    st.dataframe(st.session_state.test_results, use_container_width=True)
                    
                    csv = st.session_state.test_results.to_csv(index=False)
                    st.download_button(
                        "📥 Download Test Report",
                        csv,
                        f"Test_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        "text/csv",
                        use_container_width=True
                    )
                else:
                    st.info("Run test validation to see results")
        
        # ===== DATA QUALITY VALIDATION =====
        elif validation_type == "Data Quality Validation":
            col1, col2 = st.columns([1, 2])
            
            with col1:
                st.subheader("Data Quality Configuration")
                
                databases = get_databases(st.session_state.conn)
                dq_db = st.selectbox("Database", databases, key="dq_db")
                
                if dq_db:
                    schemas = get_schemas(st.session_state.conn, dq_db)
                    dq_schema = st.selectbox("Schema", schemas, key="dq_schema")
                    
                    if dq_schema:
                        tables = get_tables(st.session_state.conn, dq_db, dq_schema)
                        dq_table = st.selectbox("Table", tables, key="dq_table")
                        
                        st.subheader("Standard Table Checks")
                        dq_check_row_count = st.checkbox("Row Count", value=True, key="dq_check_row_count")
                        
                        if dq_check_row_count:
                            dq_min_rows = st.number_input("Minimum Expected Rows", value=1, min_value=0, key="dq_min_rows")
                        else:
                            dq_min_rows = 1
                        
                        dq_check_duplicate_rows = st.checkbox("Duplicate Rows", value=True, key="dq_check_duplicate_rows")
                        
                        st.subheader("Column Checks")
                        dq_check_column_null_pct = st.checkbox("Column Null Percentage Check", value=False, key="dq_check_column_null_pct")
                        
                        if dq_check_column_null_pct and dq_table:
                            all_columns = get_columns_for_table(st.session_state.conn, dq_db, dq_schema, dq_table)
                            dq_selected_columns_null = st.multiselect(
                                "Select Columns for Null Check (empty = all columns)",
                                all_columns,
                                key="dq_selected_columns_null"
                            )
                            dq_null_threshold = st.slider("Null % Threshold", 0, 100, 10, key="dq_null_threshold")
                        else:
                            dq_selected_columns_null = []
                            dq_null_threshold = 10
                        
                        if st.button("Run Data Quality Checks", type="primary", use_container_width=True):
                            with st.spinner("Running data quality checks..."):
                                validator = DataQualityValidator(st.session_state.conn)
                                
                                summary_df, detailed_df, status_msg, score_html, fig = validator.run_dq_checks(
                                    conn=st.session_state.conn,
                                    database=dq_db,
                                    schema=dq_schema,
                                    table=dq_table,
                                    dq_selected_columns_null=dq_selected_columns_null,
                                    dq_null_threshold=dq_null_threshold,
                                    dq_check_table_overall_null_pct=False,
                                    dq_table_null_threshold=5,
                                    dq_value_range_table=pd.DataFrame(),
                                    dq_date_range_table=pd.DataFrame(),
                                    dq_selected_columns_regex=[],
                                    dq_check_row_count=dq_check_row_count,
                                    dq_min_rows=dq_min_rows,
                                    dq_check_duplicate_rows=dq_check_duplicate_rows,
                                    dq_check_value_range=False,
                                    dq_check_date_range=False,
                                    dq_check_regex_pattern=False,
                                    dq_pattern="",
                                    dq_check_fk=False,
                                    dq_fk_column=None,
                                    dq_fk_ref_table=None,
                                    dq_fk_ref_column=None,
                                    dq_check_column_null_pct=dq_check_column_null_pct
                                )
                                
                                st.session_state.dq_summary = summary_df
                                st.session_state.dq_detailed = detailed_df
                                st.session_state.dq_score = score_html
                                st.session_state.dq_fig = fig
                                st.session_state.dq_status = status_msg
            
            with col2:
                st.subheader("Data Quality Results")
                
                if 'dq_status' in st.session_state:
                    if "✅" in st.session_state.dq_status:
                        st.success(st.session_state.dq_status)
                    elif "❌" in st.session_state.dq_status:
                        st.error(st.session_state.dq_status)
                    else:
                        st.info(st.session_state.dq_status)
                
                tab_summary, tab_detailed = st.tabs(["Summary", "Detailed Results"])
                
                with tab_summary:
                    if 'dq_summary' in st.session_state and not st.session_state.dq_summary.empty:
                        st.dataframe(st.session_state.dq_summary, use_container_width=True)
                        
                        score = st.session_state.dq_score
                        try:
                            score_val = float(score.split('/')[0])
                            if score_val >= 80:
                                score_class = "passed-score"
                            elif score_val >= 50:
                                score_class = "warning-score"
                            else:
                                score_class = "failed-score"
                        except:
                            score_class = "failed-score"
                        
                        st.markdown(f'<div class="score-box {score_class}">Quality Score: {score}</div>', unsafe_allow_html=True)
                        
                        if 'dq_fig' in st.session_state and st.session_state.dq_fig is not None:
                            st.pyplot(st.session_state.dq_fig)
                    else:
                        st.info("Run data quality checks to see summary")
                
                with tab_detailed:
                    if 'dq_detailed' in st.session_state and not st.session_state.dq_detailed.empty:
                        st.dataframe(st.session_state.dq_detailed, use_container_width=True)
                        
                        csv = st.session_state.dq_detailed.to_csv(index=False)
                        st.download_button(
                            "📥 Download Data Quality Report",
                            csv,
                            f"DQ_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            "text/csv",
                            use_container_width=True
                        )
                    else:
                        st.info("Run data quality checks to see detailed results")

else:
    st.info("👈 Please login using the sidebar to access the application")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 20px;'>
    <p>DeploySure Suite v1.0 | Powered by Streamlit & Snowflake</p>
</div>
""", unsafe_allow_html=True)