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
import base64
import os
from PIL import Image
import io

# Create a simple base64 encoded logo FIRST
def get_logo_base64():
    """Create a simple base64 encoded SVG logo"""
    svg_logo = '''
    <svg width="40" height="40" viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg">
        <rect width="40" height="40" rx="8" fill="url(#grad)" />
        <defs>
            <linearGradient id="grad" x1="0" y1="0" x2="40" y2="40" gradientUnits="userSpaceOnUse">
                <stop stop-color="#667eea"/>
                <stop offset="1" stop-color="#764ba2"/>
            </linearGradient>
        </defs>
        <text x="8" y="28" font-family="Arial" font-size="20" fill="white" font-weight="bold">DS</text>
    </svg>
    '''
    return base64.b64encode(svg_logo.encode('utf-8')).decode('utf-8')

# Then define the image loader function
def get_image_base64(image_path):
    """Convert an image file to base64 string"""
    try:
        with open(image_path, "rb") as img_file:
            return base64.b64encode(img_file.read()).decode('utf-8')
    except Exception as e:
        # Fallback to the SVG logo if file not found
        return get_logo_base64()

# Try to load your logo file, fallback to SVG if not found
logo_path = "logo-clbs- (1).png"  # Make sure this path is correct
if os.path.exists(logo_path):
    logo_base64 = get_image_base64(logo_path)
    logo_mime = "image/png"
else:
    logo_base64 = get_logo_base64()
    logo_mime = "image/svg+xml"

# REMOVE this duplicate line: logo_base64 = get_logo_base64()

try:
    from matplotlib import pyplot as plt
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
/* REMOVE top-right sidebar toggle ( » ) */
.css-1rs6os.edgvbvh3 { 
    display: none !important;
}

/* Remove the top padding Streamlit adds */
.block-container {
    padding-top: 0 !important;
}

/* Hide the default Streamlit header completely */
header {visibility: hidden !important;}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<style>
    /* Hide default streamlit elements */
    #MainMenu {visibility: hidden;}
    
    
    /* Make the app take full height */
    .stApp {
        display: flex;
        flex-direction: column;
        min-height: 100vh;
    }
    
    /* Main content should take remaining space */
    .main > div {
        flex: 1 1 auto;
        display: flex;
        flex-direction: column;
    }
    
    /* Push content to take available space */
    .block-container {
        flex: 1 1 auto;
        display: flex;
        flex-direction: column;
        padding-top: 0 !important;
    }
    
    /* Make only the header section full width */
    .header-full {
        width: 100vw;
        position: relative;
        left: 50%;
        right: 50%;
        margin-left: -50vw;
        margin-right: -50vw;
        background: linear-gradient(90deg, #0a0f1e, #13203d, #1f3d6d);
        padding: 10px 60px;
        display: flex;
        align-items: center;
        justify-content: space-between;
        box-shadow: 0 4px 15px rgba(0,0,0,0.4);
        border-bottom: 2px solid #2c4e8a;
        z-index: 10;
        margin-bottom: 20px;
        flex-shrink: 0;
    }

    .header-logo img {
        height: 40px;
    }

    .header-text h1 {
        font-size: 34px;
        font-weight: 800;
        color: #ffffff;
        margin: 0;
        letter-spacing: 1px;
    }

    .header-text p {
        font-size: 16px;
        color: #b0c4de;
        margin-top: 5px;
    }
    
    /* Footer styling - always at bottom */
    .fixed-footer {
        width: 100vw;
        position: relative;
        left: 50%;
        right: 50%;
        margin-left: -50vw;
        margin-right: -50vw;
        text-align: center;
        padding: 12px;
        background: linear-gradient(90deg, #0a0f1e, #13203d, #1f3d6d);
        color: #b0c4de;
        border-top: 2px solid #2c4e8a;
        font-size: 13px;
        box-shadow: 0 -4px 10px rgba(0,0,0,0.2);
        flex-shrink: 0;
        margin-top: auto;
    }
    
    /* Content wrapper to push footer down */
    .content-wrapper {
        flex: 1 1 auto;
        display: flex;
        flex-direction: column;
    }
    
    /* Score box styling */
    .score-box {
        text-align: center;
        padding: 20px;
        border-radius: 10px;
        font-size: 24px;
        font-weight: bold;
        margin: 20px 0;
    }
    .passed-score { background-color: #d4edda; border: 2px solid #28a745; color: #155724; }
    .warning-score { background-color: #fff3cd; border: 2px solid #ffc107; color: #856404; }
    .failed-score { background-color: #f8d7da; border: 2px solid #dc3545; color: #721c24; }
    
    /* Button styling - Blue theme */
    .stButton button {
        width: 100%;
        border-radius: 8px;
        font-weight: 600;
        background-color: #0066cc !important;
        color: white !important;
        border: none !important;
        transition: all 0.3s ease;
    }
    
    .stButton button:hover {
        background-color: #0052a3 !important;
        color: white !important;
        transform: translateY(-2px);
        box-shadow: 0 5px 15px rgba(0,102,204,0.3);
    }
    
    .stButton button:active {
        background-color: #004080 !important;
        transform: translateY(0);
    }
    
    /* Primary button styling */
    .stButton button[kind="primary"] {
        background-color: #004080 !important;
    }
    
    .stButton button[kind="primary"]:hover {
        background-color: #003366 !important;
    }
    
    /* Form submit button */
    .stForm button {
        background-color: #0066cc !important;
        color: white !important;
        border: none !important;
    }
    
    .stForm button:hover {
        background-color: #0052a3 !important;
    }
    
    /* KPI card styling */
    .kpi-card {
        background: white; border-radius: 8px; padding: 16px;
        border-left: 4px solid #0066cc;
        box-shadow: 0 2px 4px rgba(0,0,0,0.08); margin-bottom: 8px;
    }
    
    /* Remove top-right sidebar toggle */
    .css-1rs6os.edgvbvh3 { 
        display: none !important;
    }
    
    /* Hide default Streamlit header */
    header {visibility: hidden !important;}
</style>
""", unsafe_allow_html=True)

# ========== SNOWFLAKE FUNCTIONS (ORIGINAL - UNCHANGED) ==========
def get_snowflake_connection(user, password, account):
    try:
        conn = snowflake.connector.connect(
            user=user, password=password, account=account, authenticator='snowflake'
        )
        return conn, "✅ Successfully connected!"
    except Exception as e:
        return None, f"❌ Connection failed: {str(e)}"

def get_databases(conn):
    if not conn: return []
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        return [row[1] for row in cursor.fetchall()]
    except: return []

def get_schemas(conn, database):
    if not conn or not database: return []
    try:
        cursor = conn.cursor()
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {database}")
        return [row[1] for row in cursor.fetchall()]
    except: return []

def get_tables(conn, database, schema):
    if not conn or not database or not schema: return []
    try:
        cursor = conn.cursor()
        cursor.execute(f"SHOW TABLES IN SCHEMA {database}.{schema}")
        tables = [row[1] for row in cursor.fetchall()]
        return [t for t in tables if t.upper() not in ('TEST_CASES', 'ORDER_KPIS')]
    except: return []

def get_columns_for_table(conn, database, schema, table):
    if not conn or not database or not schema or not table: return []
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COLUMN_NAME FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
        """)
        return [row[0] for row in cursor.fetchall()]
    except: return []

def _get_column_details_for_dq(conn, database, schema, table):
    if not conn or not database or not schema or not table: return []
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
        """)
        return [{'name': row[0], 'type': row[1].upper()} for row in cursor.fetchall()]
    except: return []

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
    if not conn: return False, "❌ Not connected to Snowflake.", pd.DataFrame()
    if not source_db or not source_schema or not target_schema:
        return False, "⚠️ Please provide all required fields.", pd.DataFrame()
    cursor = conn.cursor()
    try:
        cursor.execute(f"SHOW SCHEMAS LIKE '{source_schema}' IN DATABASE {source_db}")
        if not cursor.fetchall():
            return False, f"❌ Source schema doesn't exist", pd.DataFrame()
        cursor.execute(f"CREATE OR REPLACE SCHEMA {source_db}.{target_schema} CLONE {source_db}.{source_schema}")
        cursor.execute(f"SHOW TABLES IN SCHEMA {source_db}.{source_schema}")
        source_tables = [row[1] for row in cursor.fetchall()]
        cursor.execute(f"SHOW TABLES IN SCHEMA {source_db}.{target_schema}")
        clone_tables = [row[1] for row in cursor.fetchall()]
        df = pd.DataFrame({
            'Database': [source_db], 'Source Schema': [source_schema],
            'Clone Schema': [target_schema], 'Source Tables': [len(source_tables)],
            'Cloned Tables': [len(clone_tables)],
            'Status': ['✅ Success' if len(source_tables) == len(clone_tables) else '⚠️ Partial']
        })
        return True, f"✅ Successfully Mirrored Schema", df
    except Exception as e:
        return False, f"❌ Clone failed: {str(e)}", pd.DataFrame()

def compare_table_differences(conn, db_name, source_schema, clone_schema):
    if not conn: return pd.DataFrame()
    cursor = conn.cursor()
    query = f"""
    WITH source_tables AS (
        SELECT table_name FROM {db_name}.information_schema.tables WHERE table_schema = '{source_schema}'
    ), clone_tables AS (
        SELECT table_name FROM {db_name}.information_schema.tables WHERE table_schema = '{clone_schema}'
    )
    SELECT COALESCE(s.table_name, c.table_name) AS table_name,
        CASE WHEN s.table_name IS NULL THEN 'Missing in source'
             WHEN c.table_name IS NULL THEN 'Missing in clone'
             ELSE 'Present in both' END AS difference
    FROM source_tables s
    FULL OUTER JOIN clone_tables c ON s.table_name = c.table_name
    WHERE s.table_name IS NULL OR c.table_name IS NULL
    ORDER BY difference, table_name
    """
    try:
        cursor.execute(query)
        return pd.DataFrame(cursor.fetchall(), columns=['Table', 'Difference'])
    except: return pd.DataFrame()

def compare_column_differences(conn, db_name, source_schema, clone_schema):
    if not conn: return pd.DataFrame(), pd.DataFrame()
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT s.table_name FROM {db_name}.information_schema.tables s
            JOIN {db_name}.information_schema.tables c ON s.table_name = c.table_name
            WHERE s.table_schema = '{source_schema}' AND c.table_schema = '{clone_schema}'
        """)
        common_tables = [row[0] for row in cursor.fetchall()]
    except: return pd.DataFrame(), pd.DataFrame()
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
                    column_diff_data.append({'Table': table, 'Column': col, 'Difference': 'Missing in source', 'Source Type': None, 'Clone Type': clone_cols.get(col)})
                elif col not in clone_cols:
                    column_diff_data.append({'Table': table, 'Column': col, 'Difference': 'Missing in clone', 'Source Type': source_cols.get(col), 'Clone Type': None})
                elif source_cols[col] != clone_cols[col]:
                    datatype_diff_data.append({'Table': table, 'Column': col, 'Source Type': source_cols[col], 'Clone Type': clone_cols[col], 'Difference': 'Type Changed'})
        except: continue
    return pd.DataFrame(column_diff_data), pd.DataFrame(datatype_diff_data)

def get_test_case_tables(conn, database, schema):
    if not conn or not database or not schema: return ["All"]
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COUNT(*) FROM {database}.information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = 'TEST_CASES'
        """)
        if cursor.fetchone()[0] == 0: return ["All"]
        cursor.execute(f"""
            SELECT DISTINCT TABLE_NAME FROM {database}.{schema}.TEST_CASES
            WHERE TABLE_NAME IS NOT NULL ORDER BY TABLE_NAME
        """)
        return ["All"] + [row[0] for row in cursor.fetchall()]
    except: return ["All"]

def get_test_cases(conn, database, schema, table):
    if not conn or not database or not schema: return []
    try:
        cursor = conn.cursor()
        if table == "All":
            query = f"""SELECT TEST_CASE_ID, TEST_ABBREVIATION, TABLE_NAME, TEST_DESCRIPTION, SQL_CODE, EXPECTED_RESULT
                        FROM {database}.{schema}.TEST_CASES ORDER BY TEST_CASE_ID"""
        else:
            query = f"""SELECT TEST_CASE_ID, TEST_ABBREVIATION, TABLE_NAME, TEST_DESCRIPTION, SQL_CODE, EXPECTED_RESULT
                        FROM {database}.{schema}.TEST_CASES WHERE TABLE_NAME = '{table}' ORDER BY TEST_CASE_ID"""
        cursor.execute(query)
        return cursor.fetchall()
    except: return []

def validate_test_cases(conn, database, schema, test_cases):
    if not conn or not test_cases: return pd.DataFrame(), "❌ No connection or test cases"
    cursor = conn.cursor()
    results = []
    for case in test_cases:
        test_id, abbrev, table_name, desc, sql, expected = case
        expected = str(expected).strip()
        try:
            qualified_sql = re.sub(rf'\b{re.escape(table_name)}\b', f'{database}.{schema}.{table_name}', sql, flags=re.IGNORECASE)
            cursor.execute(qualified_sql)
            result = cursor.fetchone()
            actual = str(result[0]) if result else "0"
            status = "✅ PASS" if actual == expected else "❌ FAIL"
            results.append({'Test Case': abbrev, 'Category': table_name, 'Expected': expected, 'Actual': actual, 'Status': status})
        except Exception as e:
            results.append({'Test Case': abbrev, 'Category': table_name, 'Expected': expected, 'Actual': f"ERROR: {str(e)[:50]}", 'Status': "❌ ERROR"})
    return pd.DataFrame(results), "✅ Validation completed"

def validate_kpis(conn, database, source_schema, target_schema):
    if not conn: return pd.DataFrame(), "❌ Not connected"
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT KPI_ID, KPI_NAME, KPI_VALUE FROM {database}.{source_schema}.ORDER_KPIS")
        kpis = cursor.fetchall()
        if not kpis: return pd.DataFrame(), "⚠️ No KPIs found"
        results = []
        for kpi_id, kpi_name, kpi_sql in kpis:
            try:
                source_query = re.sub(r'\bORDER_DATA\b', f'{database}.{source_schema}.ORDER_DATA', kpi_sql, flags=re.IGNORECASE)
                cursor.execute(source_query)
                source_val = cursor.fetchone()[0]
            except: source_val = "ERROR"
            try:
                clone_query = re.sub(r'\bORDER_DATA\b', f'{database}.{target_schema}.ORDER_DATA', kpi_sql, flags=re.IGNORECASE)
                cursor.execute(clone_query)
                clone_val = cursor.fetchone()[0]
            except: clone_val = "ERROR"
            if isinstance(source_val, (int, float)) and isinstance(clone_val, (int, float)):
                diff = float(source_val) - float(clone_val)
                status = '✅ Match' if diff == 0 else '⚠️ Mismatch'
            else:
                diff = "N/A"
                status = '✅ Match' if str(source_val) == str(clone_val) else '⚠️ Mismatch'
            results.append({'KPI': kpi_name, 'Source': source_val, 'Clone': clone_val, 'Difference': diff, 'Status': status})
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

    def _get_column_details_map(self, database, schema, table):
        """Returns dict mapping UPPER(col_name) -> {DATA_TYPE, IS_NULLABLE, ...}"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM {database}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            """)
            return {row[0].upper(): {"name": row[0], "DATA_TYPE": row[1].upper(), "IS_NULLABLE": row[2]}
                    for row in cursor.fetchall()}
        except:
            return {}

    # ---- helpers ----
    @staticmethod
    def _pass(check_name, column, expected, actual, details):
        return {"Check": check_name, "Column": column, "Expected": expected,
                "Actual": actual, "Status": "Pass", "Result": "✅ Pass", "Details": details}

    @staticmethod
    def _fail(check_name, column, expected, actual, details):
        return {"Check": check_name, "Column": column, "Expected": expected,
                "Actual": actual, "Status": "Fail", "Result": "❌ Fail", "Details": details}

    @staticmethod
    def _error(check_name, column, details):
        return {"Check": check_name, "Column": column, "Expected": "N/A",
                "Actual": "Error", "Status": "Error", "Result": "⚠️ Error", "Details": details}

    @staticmethod
    def _skip(check_name, details):
        return {"Check": check_name, "Column": "N/A", "Expected": "N/A",
                "Actual": "N/A", "Status": "Skip", "Result": "⏭️ Skip", "Details": details}

    # --- Standard checks ---
    def _run_row_count_check(self, database, schema, table, min_rows):
        count = self._execute_query(f"SELECT COUNT(*) FROM {database}.{schema}.{table}").iloc[0, 0]
        label = "📊 Row Count"
        if count >= min_rows:
            return self._pass(label, "N/A", f">= {min_rows}", str(count), f"Actual rows: {count}, Minimum expected: {min_rows}")
        return self._fail(label, "N/A", f">= {min_rows}", str(count), f"Actual rows: {count}, Minimum expected: {min_rows}")

    def _run_duplicate_check(self, database, schema, table):
        label = "🔁 Duplicate Rows"
        columns = _get_column_details_for_dq(self.conn, database, schema, table)
        if not columns:
            return self._skip(label, "No columns found in table")
        cols_str = ", ".join([f'"{col["name"]}"' for col in columns])
        dup_count = self._execute_query(f"""
            SELECT COUNT(*) FROM (
                SELECT {cols_str} FROM {database}.{schema}.{table}
                GROUP BY {cols_str} HAVING COUNT(*) > 1
            )""").iloc[0, 0]
        if dup_count == 0:
            return self._pass(label, "All Columns", "0 duplicates", "0 duplicates", "No duplicate rows found")
        return self._fail(label, "All Columns", "0 duplicates", f"{dup_count} duplicates", f"Number of duplicate rows: {dup_count}")

    # --- Null checks ---
    def _run_column_null_pct_check(self, database, schema, table, selected_columns, threshold):
        label = "🔍 Column Null %"
        all_cols = _get_column_details_for_dq(self.conn, database, schema, table)
        all_col_names = [c["name"] for c in all_cols]
        cols_to_check = selected_columns if selected_columns else all_col_names
        if not cols_to_check:
            return [self._skip(label, "No columns to check")]
        total_rows = self._execute_query(f"SELECT COUNT(*) FROM {database}.{schema}.{table}").iloc[0, 0]
        if total_rows == 0:
            return [self._skip(label, "Table is empty")]
        results = []
        for col in cols_to_check:
            if col not in all_col_names:
                results.append(self._error(label, col, f"Column '{col}' not found in table"))
                continue
            null_count = self._execute_query(
                f'SELECT COUNT(*) FROM {database}.{schema}.{table} WHERE "{col}" IS NULL').iloc[0, 0]
            pct = (float(null_count) / float(total_rows)) * 100
            expected = f"<= {threshold}%"
            actual = f"{pct:.2f}%"
            detail = f"Null count: {null_count} / {total_rows} rows ({pct:.2f}%)"
            if pct <= threshold:
                results.append(self._pass(label, col, expected, actual, detail))
            else:
                results.append(self._fail(label, col, expected, actual, detail))
        return results

    def _run_table_overall_null_pct_check(self, database, schema, table, threshold):
        label = "📋 Overall Null %"
        all_cols = _get_column_details_for_dq(self.conn, database, schema, table)
        if not all_cols:
            return self._skip(label, "No columns found")
        total_rows = self._execute_query(f"SELECT COUNT(*) FROM {database}.{schema}.{table}").iloc[0, 0]
        if total_rows == 0:
            return self._skip(label, "Table is empty")
        total_null_cells = sum(
            self._execute_query(f'SELECT COUNT(*) FROM {database}.{schema}.{table} WHERE "{c["name"]}" IS NULL').iloc[0, 0]
            for c in all_cols
        )
        total_cells = float(total_rows) * len(all_cols)
        pct = (float(total_null_cells) / total_cells) * 100
        expected = f"<= {threshold}%"
        actual = f"{pct:.2f}%"
        detail = f"Total null cells: {total_null_cells} / {int(total_cells)} ({pct:.2f}%)"
        if pct <= threshold:
            return self._pass(label, "All Columns", expected, actual, detail)
        return self._fail(label, "All Columns", expected, actual, detail)

    # --- Value range check ---
    def _run_value_range_check(self, database, schema, table, value_range_rows):
        label = "🔢 Value Range"
        results = []
        col_map = self._get_column_details_map(database, schema, table)
        for row in value_range_rows:
            col_name = str(row[0]).strip() if row[0] else ""
            min_val_str = str(row[1]).strip() if row[1] else ""
            max_val_str = str(row[2]).strip() if row[2] else ""
            if not col_name or (not min_val_str and not max_val_str):
                continue
            col_detail = col_map.get(col_name.upper())
            if not col_detail:
                results.append(self._error(label, col_name, f"Column '{col_name}' not found"))
                continue
            col_type = col_detail["DATA_TYPE"]
            if not any(t in col_type for t in ["NUMBER", "INT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC"]):
                results.append(self._error(label, col_name, f"Not a numeric column (type: {col_type})"))
                continue
            try:
                where_parts = []
                if min_val_str: where_parts.append(f'"{col_name}" < {float(min_val_str)}')
                if max_val_str: where_parts.append(f'"{col_name}" > {float(max_val_str)}')
                violations = self._execute_query(
                    f'SELECT COUNT(*) FROM {database}.{schema}.{table} WHERE {" OR ".join(where_parts)}').iloc[0, 0]
                range_str = f"[{min_val_str or '-∞'}, {max_val_str or '+∞'}]"
                detail = f"Out-of-range rows: {violations}"
                if violations == 0:
                    results.append(self._pass(label, col_name, range_str, "0 violations", detail))
                else:
                    results.append(self._fail(label, col_name, range_str, f"{violations} violations", detail))
            except Exception as e:
                results.append(self._error(label, col_name, str(e)[:100]))
        return results

    # --- Date range check ---
    def _run_date_range_check(self, database, schema, table, date_range_rows):
        label = "📅 Date Range"
        results = []
        col_map = self._get_column_details_map(database, schema, table)
        for row in date_range_rows:
            col_name = str(row[0]).strip() if row[0] else ""
            min_date = str(row[1]).strip() if row[1] else ""
            max_date = str(row[2]).strip() if row[2] else ""
            if not col_name or (not min_date and not max_date):
                continue
            col_detail = col_map.get(col_name.upper())
            if not col_detail:
                results.append(self._error(label, col_name, f"Column '{col_name}' not found"))
                continue
            col_type = col_detail["DATA_TYPE"]
            if not any(t in col_type for t in ["DATE", "TIMESTAMP", "TIME"]):
                results.append(self._error(label, col_name, f"Not a date/timestamp column (type: {col_type})"))
                continue
            try:
                where_parts = []
                if min_date: where_parts.append(f'"{col_name}" < \'{min_date}\'')
                if max_date: where_parts.append(f'"{col_name}" > \'{max_date}\'')
                violations = self._execute_query(
                    f'SELECT COUNT(*) FROM {database}.{schema}.{table} WHERE {" OR ".join(where_parts)}').iloc[0, 0]
                range_str = f"[{min_date or '-∞'}, {max_date or '+∞'}]"
                detail = f"Out-of-range rows: {violations}"
                if violations == 0:
                    results.append(self._pass(label, col_name, range_str, "0 violations", detail))
                else:
                    results.append(self._fail(label, col_name, range_str, f"{violations} violations", detail))
            except Exception as e:
                results.append(self._error(label, col_name, str(e)[:100]))
        return results

    # --- Regex pattern check ---
    def _run_regex_pattern_check(self, database, schema, table, selected_columns, pattern):
        label = "🔤 Regex Pattern"
        if not pattern:
            return []
        all_cols = _get_column_details_for_dq(self.conn, database, schema, table)
        string_cols = [c["name"] for c in all_cols if any(t in c["type"] for t in ["VARCHAR", "TEXT", "STRING", "CHAR"])]
        cols_to_check = selected_columns if selected_columns else string_cols
        if not cols_to_check:
            return [self._skip(label, "No string columns to check")]
        results = []
        for col in cols_to_check:
            col_meta = next((c for c in all_cols if c["name"] == col), None)
            if not col_meta or not any(t in col_meta["type"] for t in ["VARCHAR", "TEXT", "STRING", "CHAR"]):
                results.append(self._error(label, col, f"Column '{col}' not found or not a string type"))
                continue
            try:
                violations = self._execute_query(
                    f"SELECT COUNT(*) FROM {database}.{schema}.{table} WHERE \"{col}\" NOT RLIKE '{pattern}' AND \"{col}\" IS NOT NULL").iloc[0, 0]
                detail = f"Rows not matching pattern: {violations}"
                if violations == 0:
                    results.append(self._pass(label, col, f"All match '{pattern}'", "0 violations", detail))
                else:
                    results.append(self._fail(label, col, f"All match '{pattern}'", f"{violations} violations", detail))
            except Exception as e:
                results.append(self._error(label, col, str(e)[:100]))
        return results

    # --- Foreign key check ---
    def _run_foreign_key_check(self, database, schema, table, fk_col, ref_table, ref_col):
        label = "🔗 Foreign Key"
        if not (fk_col and ref_table and ref_col):
            return None
        try:
            violations = self._execute_query(f"""
                SELECT COUNT(*) FROM {database}.{schema}.{table} t1
                LEFT JOIN {database}.{schema}.{ref_table} t2
                    ON t1."{fk_col}" = t2."{ref_col}"
                WHERE t2."{ref_col}" IS NULL AND t1."{fk_col}" IS NOT NULL
            """).iloc[0, 0]
            expected = f"All keys in {ref_table}.{ref_col}"
            detail = f"Unmatched FK rows: {violations}  ({table}.{fk_col} → {ref_table}.{ref_col})"
            if violations == 0:
                return self._pass(label, fk_col, expected, "0 unmatched", detail)
            return self._fail(label, fk_col, expected, f"{violations} unmatched", detail)
        except Exception as e:
            return self._error(label, fk_col, str(e)[:100])

    # --- Master run method ---
    def run_checks(self, database, schema, table,
                   check_row_count, min_rows,
                   check_duplicates,
                   check_col_null_pct, col_null_cols, col_null_threshold,
                   check_table_null_pct, table_null_threshold,
                   check_value_range, value_range_rows,
                   check_date_range, date_range_rows,
                   check_regex, regex_cols, regex_pattern,
                   check_fk, fk_col, fk_ref_table, fk_ref_col):

        all_results = []
        total = passed = failed = errors = 0

        def _add(res_or_list):
            nonlocal total, passed, failed, errors
            items = res_or_list if isinstance(res_or_list, list) else ([res_or_list] if res_or_list else [])
            for r in items:
                if r is None or r.get("Status") == "Skip":
                    continue
                total += 1
                all_results.append(r)
                if r["Status"] == "Pass":
                    passed += 1
                elif r["Status"] == "Fail":
                    failed += 1
                elif r["Status"] == "Error":
                    errors += 1

        if check_row_count:
            _add(self._run_row_count_check(database, schema, table, min_rows))
        if check_duplicates:
            _add(self._run_duplicate_check(database, schema, table))
        if check_col_null_pct:
            _add(self._run_column_null_pct_check(database, schema, table, col_null_cols, col_null_threshold))
        if check_table_null_pct:
            _add(self._run_table_overall_null_pct_check(database, schema, table, table_null_threshold))
        if check_value_range and value_range_rows:
            _add(self._run_value_range_check(database, schema, table, value_range_rows))
        if check_date_range and date_range_rows:
            _add(self._run_date_range_check(database, schema, table, date_range_rows))
        if check_regex and regex_pattern:
            _add(self._run_regex_pattern_check(database, schema, table, regex_cols, regex_pattern))
        if check_fk:
            _add(self._run_foreign_key_check(database, schema, table, fk_col, fk_ref_table, fk_ref_col))

        score = max(0, (passed / total * 100) if total > 0 else 0)
        summary = pd.DataFrame([
            {"Metric": "Table", "Value": f"{database}.{schema}.{table}"},
            {"Metric": "Total Checks", "Value": total},
            {"Metric": "Passed", "Value": passed},
            {"Metric": "Failed", "Value": failed},
            {"Metric": "Errors", "Value": errors},
            {"Metric": "Quality Score", "Value": f"{score:.1f}%"}
        ])
        return summary, pd.DataFrame(all_results) if all_results else pd.DataFrame(
            columns=["Check", "Column", "Expected", "Actual", "Status", "Result", "Details"]), score


# ========== PERFORMANCE MONITORING FUNCTIONS ==========

def _execute_perf_query(conn, query, error_context=""):
    """Execute a query and return DataFrame. Returns empty DataFrame with error message on failure."""
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        if not rows:
            return pd.DataFrame()
        cols = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=cols)
        # Convert Decimal/numeric types
        for col in df.columns:
            try:
                converted = pd.to_numeric(df[col], errors='ignore')
                if converted.dtype != object:
                    df[col] = converted
            except:
                pass
        return df
    except Exception as e:
        logging.error(f"Query error {error_context}: {str(e)}\nQuery: {query[:200]}")
        raise  # Re-raise so callers can show specific error messages

def _build_where_conditions(start_date, end_date, database=None, schema=None,
                             warehouse=None, user=None, query_type=None,
                             extra_conditions=None, table_alias=""):
    """Build a list of WHERE conditions (no WHERE keyword). Extra safety for nulls."""
    prefix = f"{table_alias}." if table_alias else ""
    conditions = [f"{prefix}START_TIME BETWEEN '{start_date}' AND '{end_date}'"]
    if database and database != "All":
        conditions.append(f"{prefix}DATABASE_NAME = '{database}'")
    if schema and schema != "All":
        conditions.append(f"{prefix}SCHEMA_NAME = '{schema}'")
    if warehouse and warehouse != "All":
        conditions.append(f"{prefix}WAREHOUSE_NAME = '{warehouse}'")
    if user and user != "All":
        conditions.append(f"{prefix}USER_NAME = '{user}'")
    if query_type and query_type != "All":
        conditions.append(f"{prefix}QUERY_TYPE = '{query_type}'")
    if extra_conditions:
        conditions.extend(extra_conditions)
    return "WHERE " + " AND ".join(conditions)

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

def get_all_users(conn):
    if not conn: return ["All"]
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT USER_NAME FROM SNOWFLAKE.ACCOUNT_USAGE.USERS WHERE DELETED_ON IS NULL ORDER BY USER_NAME")
        return ["All"] + [row[0] for row in cursor.fetchall()]
    except: return ["All"]

def get_all_warehouses(conn):
    if not conn: return ["All"]
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW WAREHOUSES")
        return ["All"] + [row[0] for row in cursor.fetchall()]
    except: return ["All"]

# ---- Query Performance ----
def fetch_longest_running_queries(conn, start_date, end_date, database, schema, warehouse, user, query_type):
    where = _build_where_conditions(start_date, end_date, database, schema, warehouse, user, query_type)
    query = f"""
    SELECT QUERY_ID AS "Query ID",
           ROUND(EXECUTION_TIME / 1000, 2) AS "Exec Time (s)",
           USER_NAME AS "User",
           START_TIME AS "Start Time",
           WAREHOUSE_NAME AS "Warehouse",
           LEFT(QUERY_TEXT, 120) AS "Query Preview"
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    {where}
    ORDER BY EXECUTION_TIME DESC
    LIMIT 10
    """
    return _execute_perf_query(conn, query, "longest_running")

def fetch_expensive_queries(conn, start_date, end_date, database, schema, warehouse, user, query_type):
    """Expensive by bytes scanned (avoids complex JOIN with metering history)."""
    where = _build_where_conditions(start_date, end_date, database, schema, warehouse, user, query_type)
    query = f"""
    SELECT QUERY_ID AS "Query ID",
           LEFT(QUERY_TEXT, 120) AS "Query Preview",
           USER_NAME AS "User",
           WAREHOUSE_NAME AS "Warehouse",
           BYTES_SCANNED AS "Bytes Scanned",
           ROUND(EXECUTION_TIME / 1000, 2) AS "Exec Time (s)",
           START_TIME AS "Start Time"
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    {where}
    ORDER BY BYTES_SCANNED DESC NULLS LAST
    LIMIT 10
    """
    return _execute_perf_query(conn, query, "expensive_queries")

def fetch_top_frequent_queries(conn, start_date, end_date, database, schema, warehouse, user, query_type):
    where = _build_where_conditions(start_date, end_date, database, schema, warehouse, user, query_type)
    query = f"""
    SELECT LEFT(QUERY_TEXT, 120) AS "Query Preview",
           COUNT(*) AS "Execution Count",
           USER_NAME AS "User",
           ROUND(AVG(EXECUTION_TIME / 1000), 2) AS "Avg Exec Time (s)"
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    {where}
    GROUP BY QUERY_TEXT, USER_NAME
    ORDER BY COUNT(*) DESC
    LIMIT 10
    """
    return _execute_perf_query(conn, query, "frequent_queries")

def fetch_failed_queries(conn, start_date, end_date, database, schema, warehouse, user):
    where = _build_where_conditions(
        start_date, end_date, database, schema, warehouse, user,
        extra_conditions=["EXECUTION_STATUS != 'SUCCESS'"]
    )
    query = f"""
    SELECT QUERY_ID AS "Query ID",
           LEFT(QUERY_TEXT, 120) AS "Query Preview",
           USER_NAME AS "User",
           LEFT(ERROR_MESSAGE, 200) AS "Error",
           START_TIME AS "Start Time",
           EXECUTION_STATUS AS "Status"
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    {where}
    ORDER BY START_TIME DESC
    LIMIT 50
    """
    return _execute_perf_query(conn, query, "failed_queries")

def fetch_query_profile_summary(conn, start_date, end_date, database, schema, warehouse, user, query_type):
    where = _build_where_conditions(
        start_date, end_date, database, schema, warehouse, user, query_type,
        extra_conditions=["QUERY_TYPE IN ('SELECT','INSERT','UPDATE','DELETE','MERGE')"]
    )
    query = f"""
    SELECT QUERY_ID AS "Query ID",
           USER_NAME AS "User",
           WAREHOUSE_NAME AS "Warehouse",
           EXECUTION_STATUS AS "Status",
           ROUND(TOTAL_ELAPSED_TIME / 1000, 2) AS "Elapsed (s)",
           ROUND(COMPILATION_TIME / 1000, 2) AS "Compile (s)",
           ROUND(EXECUTION_TIME / 1000, 2) AS "Exec (s)",
           BYTES_SCANNED AS "Bytes Scanned",
           ROWS_PRODUCED AS "Rows Produced"
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    {where}
    ORDER BY COMPILATION_TIME DESC NULLS LAST
    LIMIT 10
    """
    return _execute_perf_query(conn, query, "query_profile")

# ---- User Adoption ----
def fetch_top_active_users(conn, start_date, end_date, database, schema, query_type):
    where = _build_where_conditions(start_date, end_date, database, schema, query_type=query_type)
    query = f"""
    SELECT USER_NAME AS "User",
           COUNT(*) AS "Query Count",
           COUNT(DISTINCT SESSION_ID) AS "Sessions",
           ROUND(SUM(EXECUTION_TIME / 1000), 2) AS "Total Exec Time (s)"
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    {where}
    GROUP BY USER_NAME
    ORDER BY "Query Count" DESC
    LIMIT 10
    """
    return _execute_perf_query(conn, query, "top_active_users")

def fetch_active_users_over_time(conn, start_date, end_date, database, schema, warehouse, user, query_type):
    where = _build_where_conditions(start_date, end_date, database, schema, warehouse, user, query_type)
    query = f"""
    SELECT TO_DATE(START_TIME) AS "Date",
           COUNT(DISTINCT USER_NAME) AS "Active Users"
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    {where}
    GROUP BY TO_DATE(START_TIME)
    ORDER BY TO_DATE(START_TIME)
    """
    return _execute_perf_query(conn, query, "users_over_time")

def fetch_queries_per_user(conn, start_date, end_date, database, schema, warehouse, user):
    where = _build_where_conditions(start_date, end_date, database, schema, warehouse, user)
    query = f"""
    SELECT USER_NAME AS "User", COUNT(*) AS "Query Count"
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    {where}
    GROUP BY USER_NAME
    ORDER BY "Query Count" DESC
    LIMIT 10
    """
    return _execute_perf_query(conn, query, "queries_per_user")

# ---- Compute Cost ----
def fetch_warehouse_credits(conn, start_date, end_date, warehouse):
    conditions = [f"START_TIME BETWEEN '{start_date}' AND '{end_date}'"]
    if warehouse and warehouse != "All":
        conditions.append(f"WAREHOUSE_NAME = '{warehouse}'")
    where = "WHERE " + " AND ".join(conditions)
    query = f"""
    SELECT WAREHOUSE_NAME AS "Warehouse",
           TO_DATE(START_TIME) AS "Date",
           ROUND(SUM(CREDITS_USED), 4) AS "Credits Used",
           ROUND(SUM(CREDITS_USED_COMPUTE), 4) AS "Compute Credits",
           ROUND(SUM(CREDITS_USED_CLOUD_SERVICES), 4) AS "Cloud Services Credits"
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    {where}
    GROUP BY WAREHOUSE_NAME, TO_DATE(START_TIME)
    ORDER BY TO_DATE(START_TIME) ASC, "Credits Used" DESC
    """
    return _execute_perf_query(conn, query, "warehouse_credits")

def fetch_credit_usage_over_time(conn, start_date, end_date, warehouse):
    conditions = [f"START_TIME BETWEEN '{start_date}' AND '{end_date}'"]
    if warehouse and warehouse != "All":
        conditions.append(f"WAREHOUSE_NAME = '{warehouse}'")
    where = "WHERE " + " AND ".join(conditions)
    date_diff = (datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days
    if date_diff <= 31:
        date_expr = "TO_DATE(START_TIME)"
        date_col = "Date"
    else:
        date_expr = "TO_CHAR(START_TIME, 'YYYY-MM')"
        date_col = "Month"
    query = f"""
    SELECT {date_expr} AS "{date_col}",
           ROUND(SUM(CREDITS_USED), 4) AS "Credits Used"
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    {where}
    GROUP BY {date_expr}
    ORDER BY {date_expr}
    """
    return _execute_perf_query(conn, query, "credit_over_time"), date_col

def fetch_cost_heatmap_data(conn, start_date, end_date, warehouse):
    conditions = [f"START_TIME BETWEEN '{start_date}' AND '{end_date}'"]
    if warehouse and warehouse != "All":
        conditions.append(f"WAREHOUSE_NAME = '{warehouse}'")
    where = "WHERE " + " AND ".join(conditions)
    query = f"""
    SELECT TO_CHAR(START_TIME, 'DY') AS "DayOfWeek",
           EXTRACT(HOUR FROM START_TIME) AS "HourOfDay",
           ROUND(SUM(CREDITS_USED), 4) AS "Credits"
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    {where}
    GROUP BY TO_CHAR(START_TIME, 'DY'), EXTRACT(HOUR FROM START_TIME)
    """
    return _execute_perf_query(conn, query, "cost_heatmap")

# ---- Storage ----
def fetch_daily_storage_usage(conn, start_date, end_date):
    query = f"""
    SELECT USAGE_DATE AS "Date",
           ROUND(AVERAGE_DATABASE_BYTES / POWER(1024, 3), 4) AS "Avg DB Storage (GB)",
           ROUND(AVERAGE_FAILSAFE_BYTES / POWER(1024, 3), 4) AS "Avg Failsafe (GB)"
    FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY
    WHERE USAGE_DATE BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY USAGE_DATE ASC
    """
    return _execute_perf_query(conn, query, "daily_storage")

def fetch_table_storage_metrics(conn, database, schema):
    if not database or database == "All" or not schema or schema == "All":
        return pd.DataFrame()
    query = f"""
    SELECT TABLE_NAME AS "Table",
           ROUND(ACTIVE_BYTES / POWER(1024, 2), 2) AS "Active Size (MB)",
           ROUND(TIME_TRAVEL_BYTES / POWER(1024, 2), 2) AS "Time Travel (MB)"
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
    WHERE TABLE_CATALOG = '{database}' AND TABLE_SCHEMA = '{schema}'
    ORDER BY ACTIVE_BYTES DESC NULLS LAST
    LIMIT 100
    """
    return _execute_perf_query(conn, query, "table_storage")

def fetch_total_storage_over_time(conn):
    query = """
    SELECT USAGE_DATE AS "Date",
           ROUND(STORAGE_BYTES / POWER(1024, 3), 4) AS "Total Storage (GB)"
    FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE
    ORDER BY USAGE_DATE DESC
    LIMIT 90
    """
    return _execute_perf_query(conn, query, "total_storage")

# ---- Warehouse Activity ----
def fetch_warehouse_utilization(conn, start_date, end_date, warehouse):
    conditions = [f"START_TIME BETWEEN '{start_date}' AND '{end_date}'"]
    if warehouse and warehouse != "All":
        conditions.append(f"WAREHOUSE_NAME = '{warehouse}'")
    where = "WHERE " + " AND ".join(conditions)
    query = f"""
    SELECT WAREHOUSE_NAME AS "Warehouse",
           TO_DATE(START_TIME) AS "Date",
           ROUND(AVG(AVG_RUNNING), 4) AS "Avg Running",
           ROUND(AVG(AVG_QUEUED_LOAD), 4) AS "Avg Queued",
           ROUND(AVG(AVG_QUEUED_PROVISIONING), 4) AS "Avg Queued Prov.",
           ROUND(AVG(AVG_BLOCKED), 4) AS "Avg Blocked"
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
    {where}
    GROUP BY WAREHOUSE_NAME, TO_DATE(START_TIME)
    ORDER BY WAREHOUSE_NAME, TO_DATE(START_TIME)
    """
    return _execute_perf_query(conn, query, "warehouse_utilization")


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
        # Add the new full-width header
        st.markdown(f"""
        <div class="header-full">
            <div class="header-logo">
                <img src="data:{logo_mime};base64,{logo_base64}" alt="DeploySure Logo">
            </div>
            <div class="header-text">
                <h1>DeploySure Suite</h1>
                <p>Snowflake Data Validation & Quality Management</p>
            </div>
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
    # Add the new full-width header with user info
    st.markdown(f"""
    <div class="header-full">
        <div class="header-logo">
            <img src="data:{logo_mime};base64,{logo_base64}" alt="DeploySure Logo">
        </div>
        <div class="header-text">
            <h1>DeploySure Suite</h1>
            <p>Welcome, {st.session_state.username}!</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

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
        except: pass

    tab1, tab2, tab3 = st.tabs(["⎘ MirrorSchema", "🔍 DriftWatch", "📊 Performance Monitoring"])

    # ===== MIRROR SCHEMA (ORIGINAL - UNCHANGED) =====
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
                                success, msg, df = clone_schema(st.session_state.conn, source_db, source_schema, target_schema)
                                if success:
                                    st.success(msg)
                                    if not df.empty: st.dataframe(df, use_container_width=True)
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

    # ===== DRIFTWATCH (ORIGINAL - UNCHANGED) =====
    with tab2:
        st.header("DriftWatch")
        validation_type = st.selectbox(
            "Validation Type",
            ["Schema Validation", "KPI Validation", "Test Case Validation", "Data Quality Validation"]
        )
        st.markdown("---")

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
                        st.download_button("📥 Download", st.session_state.table_diff.to_csv(index=False), f"table_diff_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                    else: st.info("No differences found")
                with sub_tab2:
                    if 'col_diff' in st.session_state and not st.session_state.col_diff.empty:
                        st.dataframe(st.session_state.col_diff, use_container_width=True)
                        st.download_button("📥 Download", st.session_state.col_diff.to_csv(index=False), f"col_diff_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                    else: st.info("No differences found")
                with sub_tab3:
                    if 'type_diff' in st.session_state and not st.session_state.type_diff.empty:
                        st.dataframe(st.session_state.type_diff, use_container_width=True)
                        st.download_button("📥 Download", st.session_state.type_diff.to_csv(index=False), f"type_diff_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                    else: st.info("No differences found")

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
                                if not df.empty: st.success(msg)
                                else: st.warning(msg)
                    else: st.warning("Need at least 2 schemas")
            with col2:
                st.subheader("📊 Results")
                if 'kpi_results' in st.session_state and not st.session_state.kpi_results.empty:
                    st.dataframe(st.session_state.kpi_results, use_container_width=True)
                    st.download_button("📥 Download", st.session_state.kpi_results.to_csv(index=False), f"kpi_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                else: st.info("Run validation to see results")

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
                                selected = st.multiselect("Test Cases", test_names, default=test_names, key="tc_selected")
                            else:
                                selected = st.multiselect("Test Cases", test_names, key="tc_selected_manual")
                            if st.button("Execute DriftWatch", type="primary", use_container_width=True):
                                if selected:
                                    with st.spinner("Running tests..."):
                                        selected_cases = [case for case in test_cases if case[1] in selected]
                                        df, msg = validate_test_cases(st.session_state.conn, tc_db, tc_schema, selected_cases)
                                        st.session_state.test_results = df
                                        if not df.empty: st.success(msg)
                                        else: st.warning(msg)
                                else:
                                    st.warning("Select at least one test case")
                        else: st.warning("No test cases found")
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
                    st.download_button("📥 Download", st.session_state.test_results.to_csv(index=False), f"test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                else: st.info("Run validation to see results")

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
                            # Fetch columns for this table (used by multiple checks)
                            all_col_details = _get_column_details_for_dq(
                                st.session_state.conn, dq_db, dq_schema, dq_table)
                            all_col_names = [c["name"] for c in all_col_details]
                            num_cols = [c["name"] for c in all_col_details
                                        if any(t in c["type"] for t in ["NUMBER","INT","FLOAT","DOUBLE","DECIMAL","NUMERIC"])]
                            date_cols_list = [c["name"] for c in all_col_details
                                              if any(t in c["type"] for t in ["DATE","TIMESTAMP","TIME"])]
                            str_cols = [c["name"] for c in all_col_details
                                        if any(t in c["type"] for t in ["VARCHAR","TEXT","STRING","CHAR"])]

                            st.markdown("---")
                            st.markdown("#### ✅ Select Checks to Run")

                            # --- Check 1: Row Count ---
                            dq_row_count = st.checkbox("📊 Row Count Check", value=True, key="dq_row")
                            dq_min_rows = 1
                            if dq_row_count:
                                dq_min_rows = st.number_input("Minimum expected rows", value=1, min_value=0, key="dq_min")

                            st.markdown("---")

                            # --- Check 2: Duplicates ---
                            dq_duplicates = st.checkbox("🔁 Duplicate Rows Check", value=True, key="dq_dup")

                            st.markdown("---")

                            # --- Check 3: Column Null % ---
                            dq_col_null = st.checkbox("🔍 Column Null % Check", value=False, key="dq_col_null")
                            dq_col_null_cols = []
                            dq_col_null_threshold = 5.0
                            if dq_col_null:
                                dq_col_null_cols = st.multiselect(
                                    "Columns to check (empty = all columns)",
                                    all_col_names, key="dq_col_null_cols")
                                dq_col_null_threshold = st.number_input(
                                    "Max allowed null % per column", value=5.0,
                                    min_value=0.0, max_value=100.0, step=0.5, key="dq_col_null_thr")

                            st.markdown("---")

                            # --- Check 4: Overall Table Null % ---
                            dq_table_null = st.checkbox("📋 Overall Table Null % Check", value=False, key="dq_tbl_null")
                            dq_table_null_threshold = 10.0
                            if dq_table_null:
                                dq_table_null_threshold = st.number_input(
                                    "Max allowed overall null %", value=10.0,
                                    min_value=0.0, max_value=100.0, step=0.5, key="dq_tbl_null_thr")

                            st.markdown("---")

                            # --- Check 5: Value Range (numeric) ---
                            dq_val_range = st.checkbox("🔢 Value Range Check (Numeric Columns)", value=False, key="dq_val_range")
                            dq_val_range_rows = []
                            if dq_val_range:
                                if num_cols:
                                    st.caption("Define min/max for each numeric column (leave blank to skip that bound)")
                                    vr_col_sel = st.multiselect("Select numeric columns", num_cols, key="dq_vr_cols")
                                    for vc in vr_col_sel:
                                        vrc1, vrc2 = st.columns(2)
                                        vmin = vrc1.text_input(f"{vc} — Min", key=f"dq_vr_min_{vc}", placeholder="e.g. 0")
                                        vmax = vrc2.text_input(f"{vc} — Max", key=f"dq_vr_max_{vc}", placeholder="e.g. 1000000")
                                        if vmin or vmax:
                                            dq_val_range_rows.append((vc, vmin, vmax))
                                else:
                                    st.info("No numeric columns detected in this table.")

                            st.markdown("---")

                            # --- Check 6: Date Range ---
                            dq_date_range = st.checkbox("📅 Date Range Check", value=False, key="dq_date_range")
                            dq_date_range_rows = []
                            if dq_date_range:
                                if date_cols_list:
                                    st.caption("Define min/max dates for each date column (YYYY-MM-DD)")
                                    dr_col_sel = st.multiselect("Select date columns", date_cols_list, key="dq_dr_cols")
                                    for dc in dr_col_sel:
                                        drc1, drc2 = st.columns(2)
                                        dmin = drc1.text_input(f"{dc} — Min date", key=f"dq_dr_min_{dc}", placeholder="e.g. 2020-01-01")
                                        dmax = drc2.text_input(f"{dc} — Max date", key=f"dq_dr_max_{dc}", placeholder="e.g. 2025-12-31")
                                        if dmin or dmax:
                                            dq_date_range_rows.append((dc, dmin, dmax))
                                else:
                                    st.info("No date/timestamp columns detected in this table.")

                            st.markdown("---")

                            # --- Check 7: Regex Pattern ---
                            dq_regex = st.checkbox("🔤 Regex Pattern Check (String Columns)", value=False, key="dq_regex")
                            dq_regex_cols = []
                            dq_regex_pattern = ""
                            if dq_regex:
                                if str_cols:
                                    dq_regex_cols = st.multiselect(
                                        "Columns to check (empty = all string columns)",
                                        str_cols, key="dq_regex_cols")
                                    dq_regex_pattern = st.text_input(
                                        "Regex pattern (Snowflake RLIKE syntax)",
                                        placeholder=r"e.g. ^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$",
                                        key="dq_regex_pat")
                                    if dq_regex_pattern:
                                        st.caption(f"Will flag rows where value does NOT match: `{dq_regex_pattern}`")
                                else:
                                    st.info("No string columns detected in this table.")

                            st.markdown("---")

                            # --- Check 8: Foreign Key ---
                            dq_fk = st.checkbox("🔗 Foreign Key Check", value=False, key="dq_fk")
                            dq_fk_col = dq_fk_ref_table = dq_fk_ref_col = ""
                            if dq_fk:
                                all_tbls = get_tables(st.session_state.conn, dq_db, dq_schema)
                                fk1, fk2, fk3 = st.columns(3)
                                dq_fk_col = fk1.selectbox("FK Column (this table)", [""] + all_col_names, key="dq_fk_col")
                                dq_fk_ref_table = fk2.selectbox("Referenced Table", [""] + all_tbls, key="dq_fk_ref_tbl")
                                if dq_fk_ref_table:
                                    ref_cols = get_columns_for_table(st.session_state.conn, dq_db, dq_schema, dq_fk_ref_table)
                                    dq_fk_ref_col = fk3.selectbox("Referenced Column", [""] + ref_cols, key="dq_fk_ref_col")

                            st.markdown("---")

                            if st.button("🚀 Run Quality Checks", type="primary", use_container_width=True):
                                with st.spinner("Running checks..."):
                                    try:
                                        validator = DataQualityValidator(st.session_state.conn)
                                        summary, details, score = validator.run_checks(
                                            dq_db, dq_schema, dq_table,
                                            dq_row_count, dq_min_rows,
                                            dq_duplicates,
                                            dq_col_null, dq_col_null_cols, dq_col_null_threshold,
                                            dq_table_null, dq_table_null_threshold,
                                            dq_val_range, dq_val_range_rows,
                                            dq_date_range, dq_date_range_rows,
                                            dq_regex, dq_regex_cols, dq_regex_pattern,
                                            dq_fk, dq_fk_col, dq_fk_ref_table, dq_fk_ref_col
                                        )
                                        st.session_state.dq_summary = summary
                                        st.session_state.dq_details = details
                                        st.session_state.dq_score = score
                                        st.success("✅ Quality checks completed!")
                                    except Exception as e:
                                        st.error(f"❌ Error running checks: {str(e)}")

            with col2:
                st.subheader("📊 Results")
                if 'dq_score' in st.session_state:
                    score = st.session_state.dq_score
                    score_class = "passed-score" if score >= 80 else ("warning-score" if score >= 50 else "failed-score")
                    st.markdown(
                        f'<div class="score-box {score_class}">Quality Score: {score:.0f}/100</div>',
                        unsafe_allow_html=True)

                    # Quick metric bar
                    if 'dq_details' in st.session_state and not st.session_state.dq_details.empty:
                        det = st.session_state.dq_details
                        mc1, mc2, mc3, mc4 = st.columns(4)
                        mc1.metric("Total Checks", len(det))
                        mc2.metric("✅ Passed", len(det[det["Status"] == "Pass"]))
                        mc3.metric("❌ Failed", len(det[det["Status"] == "Fail"]))
                        mc4.metric("⚠️ Errors", len(det[det["Status"] == "Error"]))

                dq_tab1, dq_tab2 = st.tabs(["📋 Summary", "🔍 Check Details"])
                with dq_tab1:
                    if 'dq_summary' in st.session_state and not st.session_state.dq_summary.empty:
                        st.dataframe(st.session_state.dq_summary, use_container_width=True)
                    else:
                        st.info("Configure and run checks to see results here.")
                with dq_tab2:
                    if 'dq_details' in st.session_state and not st.session_state.dq_details.empty:
                        det = st.session_state.dq_details.copy()

                        # Filter by status
                        status_filter = st.selectbox(
                            "Filter by status", ["All", "Pass", "Fail", "Error"],
                            key="dq_status_filter")
                        if status_filter != "All":
                            det = det[det["Status"] == status_filter]

                        # Display with column config - hide internal Status, show Result prominently
                        display_cols = ["Check", "Column", "Expected", "Actual", "Result", "Details"]
                        display_cols = [c for c in display_cols if c in det.columns]
                        st.dataframe(
                            det[display_cols],
                            use_container_width=True,
                            column_config={
                                "Check": st.column_config.TextColumn("Check", width="medium"),
                                "Column": st.column_config.TextColumn("Column", width="medium"),
                                "Expected": st.column_config.TextColumn("Expected", width="small"),
                                "Actual": st.column_config.TextColumn("Actual", width="small"),
                                "Result": st.column_config.TextColumn("Result", width="small"),
                                "Details": st.column_config.TextColumn("Details", width="large"),
                            },
                            hide_index=True
                        )
                        st.download_button(
                            "📥 Download Full Report",
                            st.session_state.dq_details.to_csv(index=False),
                            f"dq_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            key="dq_dl_btn")
                    else:
                        st.info("Run checks to see detailed results here.")

    # ===== PERFORMANCE MONITORING (NEW TAB - FIXED) =====
    with tab3:
        st.header("📊 Performance Monitoring & Cost Analysis")

        # ---- Global Filters ----
        with st.expander("🔧 Global Filters", expanded=True):
            fcol1, fcol2 = st.columns(2)
            with fcol1:
                time_range_opt = st.selectbox(
                    "Time Range",
                    ["Last 7 days", "Last 24 hours", "Last 30 days", "Custom"],
                    key="perf_time_range"
                )
            with fcol2:
                _default_start, _default_end = get_date_range(time_range_opt)
                if time_range_opt == "Custom":
                    perf_start = st.text_input("Start Date (YYYY-MM-DD)", value=_default_start, key="perf_start_custom")
                    perf_end   = st.text_input("End Date (YYYY-MM-DD)",   value=_default_end,   key="perf_end_custom")
                else:
                    perf_start = _default_start
                    perf_end   = _default_end
                    st.info(f"📅 {perf_start}  →  {perf_end}")

            fcol3, fcol4, fcol5, fcol6 = st.columns(4)
            with fcol3:
                perf_warehouses = get_all_warehouses(st.session_state.conn)
                perf_warehouse  = st.selectbox("Warehouse", perf_warehouses, key="perf_warehouse")
            with fcol4:
                perf_dbs = ["All"] + get_databases(st.session_state.conn)
                perf_db  = st.selectbox("Database", perf_dbs, key="perf_db")
            with fcol5:
                perf_schemas_list = ["All"] + (get_schemas(st.session_state.conn, perf_db) if perf_db != "All" else [])
                perf_schema = st.selectbox("Schema", perf_schemas_list, key="perf_schema")
            with fcol6:
                perf_users = get_all_users(st.session_state.conn)
                perf_user  = st.selectbox("User", perf_users, key="perf_user")

            perf_query_type = st.selectbox(
                "Query Type", ["All", "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE"],
                key="perf_query_type"
            )

        # ---- Sub-tabs ----
        perf_tab1, perf_tab2, perf_tab3, perf_tab4, perf_tab5 = st.tabs([
            "👤 User Adoption", "⚡ Query Performance",
            "💰 Compute Cost", "🗄️ Storage", "🏭 Warehouse Activity"
        ])

        # =========================================================
        # Helper: render a horizontal bar chart (always works)
        # =========================================================
        def _hbar_chart(df, label_col, value_col, title, color="#4C72B0"):
            """Render a horizontal bar chart using st.bar_chart (native) or matplotlib."""
            if df is None or df.empty:
                return
            
            # Check if the required columns exist
            if label_col not in df.columns or value_col not in df.columns:
                st.warning(f"Required columns '{label_col}' or '{value_col}' not found in data. Available columns: {list(df.columns)}")
                return
            
            df = df.copy()
            df[value_col] = pd.to_numeric(df[value_col], errors='coerce').fillna(0)
            df = df.nlargest(10, value_col).sort_values(value_col)

            if MATPLOTLIB_AVAILABLE:
                fig, ax = plt.subplots(figsize=(10, max(3, len(df) * 0.5)))
                ax.barh(df[label_col].astype(str), df[value_col], color=color)
                ax.set_xlabel(value_col)
                ax.set_title(title)
                ax.grid(axis='x', alpha=0.3)
                plt.tight_layout()
                st.pyplot(fig); plt.close()
            else:
                chart_df = df.set_index(label_col)[[value_col]]
                st.markdown(f"**{title}**")
                st.bar_chart(chart_df)

        def _line_chart(df, date_col, value_col, title, color="#2E86C1"):
            """Render a line chart using matplotlib or st.line_chart."""
            if df is None or df.empty:
                return
            df = df.copy()
            df[value_col] = pd.to_numeric(df[value_col], errors='coerce').fillna(0)

            if MATPLOTLIB_AVAILABLE:
                fig, ax = plt.subplots(figsize=(10, 4))
                try:
                    df[date_col] = pd.to_datetime(df[date_col])
                    df = df.sort_values(date_col)
                except Exception:
                    pass
                ax.plot(df[date_col].astype(str), df[value_col], marker='o', color=color, linewidth=2)
                ax.set_xlabel(date_col); ax.set_ylabel(value_col)
                ax.set_title(title); ax.grid(True, alpha=0.3)
                plt.xticks(rotation=45, ha='right'); plt.tight_layout()
                st.pyplot(fig); plt.close()
            else:
                try:
                    df[date_col] = pd.to_datetime(df[date_col])
                    df = df.sort_values(date_col)
                except Exception:
                    pass
                chart_df = df.set_index(date_col)[[value_col]]
                st.markdown(f"**{title}**")
                st.line_chart(chart_df)

        def _area_chart(df, date_col, value_col, title):
            """Render an area/fill chart."""
            if df is None or df.empty:
                return
            df = df.copy()
            df[value_col] = pd.to_numeric(df[value_col], errors='coerce').fillna(0)

            if MATPLOTLIB_AVAILABLE:
                fig, ax = plt.subplots(figsize=(10, 4))
                try:
                    df[date_col] = pd.to_datetime(df[date_col])
                    df = df.sort_values(date_col)
                except Exception:
                    pass
                vals = df[value_col].values
                labels = df[date_col].astype(str).values
                ax.fill_between(range(len(vals)), vals, color='lightgreen', alpha=0.7)
                ax.plot(range(len(vals)), vals, color='darkgreen', marker='o', linewidth=2)
                ax.set_xticks(range(len(vals)))
                ax.set_xticklabels(labels, rotation=45, ha='right')
                ax.set_ylabel(value_col); ax.set_title(title)
                ax.grid(True, alpha=0.3); plt.tight_layout()
                st.pyplot(fig); plt.close()
            else:
                try:
                    df[date_col] = pd.to_datetime(df[date_col])
                    df = df.sort_values(date_col)
                except Exception:
                    pass
                chart_df = df.set_index(date_col)[[value_col]]
                st.markdown(f"**{title}**")
                st.area_chart(chart_df)

        def _stacked_bar(df, group_col, value_cols, title):
            """Render a stacked bar chart."""
            if df is None or df.empty:
                return
            for c in value_cols:
                df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
            grp = df.groupby(group_col)[value_cols].sum()

            if MATPLOTLIB_AVAILABLE:
                fig, ax = plt.subplots(figsize=(10, 5))
                grp.plot(kind='bar', stacked=True, ax=ax, colormap='coolwarm')
                ax.set_xlabel(group_col); ax.set_ylabel("Credits")
                ax.set_title(title)
                plt.xticks(rotation=45, ha='right'); plt.tight_layout()
                st.pyplot(fig); plt.close()
            else:
                st.markdown(f"**{title}**")
                st.bar_chart(grp)

        # =========================================================
        # 👤 USER ADOPTION
        # =========================================================
        with perf_tab1:
            st.subheader("👤 User Adoption")
            if st.button("🔄 Load User Adoption Data", key="load_ua", type="primary"):
                with st.spinner("Loading..."):
                    errors = []
                    try:
                        st.session_state.ua_top_users = fetch_top_active_users(
                            st.session_state.conn, perf_start, perf_end,
                            perf_db, perf_schema, perf_query_type)
                    except Exception as e:
                        errors.append(f"Top Users: {e}")
                        st.session_state.ua_top_users = pd.DataFrame()
                    try:
                        st.session_state.ua_over_time = fetch_active_users_over_time(
                            st.session_state.conn, perf_start, perf_end,
                            perf_db, perf_schema, perf_warehouse, perf_user, perf_query_type)
                    except Exception as e:
                        errors.append(f"Users Over Time: {e}")
                        st.session_state.ua_over_time = pd.DataFrame()
                    try:
                        st.session_state.ua_queries_per_user = fetch_queries_per_user(
                            st.session_state.conn, perf_start, perf_end,
                            perf_db, perf_schema, perf_warehouse, perf_user)
                    except Exception as e:
                        errors.append(f"Queries Per User: {e}")
                        st.session_state.ua_queries_per_user = pd.DataFrame()
                    if errors:
                        for err in errors: st.error(f"❌ {err}")
                    else:
                        st.success("✅ Data loaded!")

            ua_col1, ua_col2 = st.columns(2)
            with ua_col1:
                st.markdown("#### 🏆 Top Active Users")
                if 'ua_top_users' in st.session_state and not st.session_state.ua_top_users.empty:
                    st.dataframe(st.session_state.ua_top_users, use_container_width=True)
                    st.download_button("📥 Download", st.session_state.ua_top_users.to_csv(index=False),
                                       f"top_users_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_ua_users")
                    _hbar_chart(st.session_state.ua_top_users, "User", "Query Count",
                                "Top Users by Query Count", color="steelblue")
                else:
                    st.info("Click 'Load User Adoption Data' to see results.")

            with ua_col2:
                st.markdown("#### 📈 Active Users Over Time")
                if 'ua_over_time' in st.session_state and not st.session_state.ua_over_time.empty:
                    st.dataframe(st.session_state.ua_over_time, use_container_width=True)
                    st.download_button("📥 Download", st.session_state.ua_over_time.to_csv(index=False),
                                       f"users_over_time_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_ua_time")
                    _line_chart(st.session_state.ua_over_time, "Date", "Active Users",
                                "Active Users Over Time", color="purple")
                else:
                    st.info("Click 'Load User Adoption Data' to see results.")

            if 'ua_queries_per_user' in st.session_state and not st.session_state.ua_queries_per_user.empty:
                st.markdown("#### 📊 Query Execution Count by User")
                st.dataframe(st.session_state.ua_queries_per_user, use_container_width=True)
                _hbar_chart(st.session_state.ua_queries_per_user, "User", "Query Count",
                    "Total Queries per User", color="mediumseagreen")

        # =========================================================
        # ⚡ QUERY PERFORMANCE
        # =========================================================
        with perf_tab2:
            st.subheader("⚡ Query Performance")
            if st.button("🔄 Load Query Performance Data", key="load_qp", type="primary"):
                with st.spinner("Loading..."):
                    errors = []
                    for fetch_fn, key, label in [
                        (lambda: fetch_longest_running_queries(st.session_state.conn, perf_start, perf_end, perf_db, perf_schema, perf_warehouse, perf_user, perf_query_type), "qp_longest", "Longest Running"),
                        (lambda: fetch_expensive_queries(st.session_state.conn, perf_start, perf_end, perf_db, perf_schema, perf_warehouse, perf_user, perf_query_type), "qp_expensive", "Most Expensive"),
                        (lambda: fetch_top_frequent_queries(st.session_state.conn, perf_start, perf_end, perf_db, perf_schema, perf_warehouse, perf_user, perf_query_type), "qp_frequent", "Most Frequent"),
                        (lambda: fetch_failed_queries(st.session_state.conn, perf_start, perf_end, perf_db, perf_schema, perf_warehouse, perf_user), "qp_failed", "Failed Queries"),
                        (lambda: fetch_query_profile_summary(st.session_state.conn, perf_start, perf_end, perf_db, perf_schema, perf_warehouse, perf_user, perf_query_type), "qp_profile", "Query Profile"),
                    ]:
                        try:
                            st.session_state[key] = fetch_fn()
                        except Exception as e:
                            errors.append(f"{label}: {e}")
                            st.session_state[key] = pd.DataFrame()
                    if errors:
                        for err in errors: st.error(f"❌ {err}")
                    else:
                        st.success("✅ Data loaded!")

            qp_sub1, qp_sub2, qp_sub3, qp_sub4, qp_sub5 = st.tabs([
                "⏱ Longest Running", "💸 Most Expensive (Bytes)", "🔁 Most Frequent", "❌ Failed", "🔍 Query Profile"
            ])

            with qp_sub1:
                if 'qp_longest' in st.session_state and not st.session_state.qp_longest.empty:
                    st.dataframe(st.session_state.qp_longest, use_container_width=True)
                    st.download_button("📥 Download", st.session_state.qp_longest.to_csv(index=False),
                                       f"longest_queries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_qp_long")
                    _hbar_chart(st.session_state.qp_longest, "Query ID", "Exec Time (s)",
                                "Top 10 Longest Running Queries (seconds)", color="tomato")
                else:
                    st.info("Click 'Load Query Performance Data' to see results.")

            with qp_sub2:
                if 'qp_expensive' in st.session_state and not st.session_state.qp_expensive.empty:
                    st.dataframe(st.session_state.qp_expensive, use_container_width=True)
                    st.download_button("📥 Download", st.session_state.qp_expensive.to_csv(index=False),
                                       f"expensive_queries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_qp_exp")
                    _hbar_chart(st.session_state.qp_expensive, "Query ID", "Bytes Scanned",
                                "Top 10 Queries by Bytes Scanned", color="salmon")
                else:
                    st.info("Click 'Load Query Performance Data' to see results.")

            with qp_sub3:
                if 'qp_frequent' in st.session_state and not st.session_state.qp_frequent.empty:
                    st.dataframe(st.session_state.qp_frequent, use_container_width=True)
                    st.download_button("📥 Download", st.session_state.qp_frequent.to_csv(index=False),
                                       f"frequent_queries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_qp_freq")
                    _hbar_chart(st.session_state.qp_frequent, "Query Preview", "Execution Count",
                                "Top 10 Most Frequent Queries", color="cornflowerblue")
                else:
                    st.info("Click 'Load Query Performance Data' to see results.")

            with qp_sub4:
                if 'qp_failed' in st.session_state and not st.session_state.qp_failed.empty:
                    df_f = st.session_state.qp_failed
                    st.metric("Total Failed Queries", len(df_f))
                    # Pie of error types if available
                    if MATPLOTLIB_AVAILABLE and "Status" in df_f.columns:
                        status_counts = df_f["Status"].value_counts()
                        fig, ax = plt.subplots(figsize=(5, 4))
                        ax.pie(status_counts.values, labels=status_counts.index, autopct='%1.0f%%', startangle=90)
                        ax.set_title("Failed Query Status Breakdown")
                        plt.tight_layout()
                        st.pyplot(fig); plt.close()
                    st.dataframe(df_f, use_container_width=True)
                    st.download_button("📥 Download", df_f.to_csv(index=False),
                                       f"failed_queries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_qp_fail")
                else:
                    st.info("Click 'Load Query Performance Data' to see results.")

            with qp_sub5:
                if 'qp_profile' in st.session_state and not st.session_state.qp_profile.empty:
                    df_prof = st.session_state.qp_profile
                    st.dataframe(df_prof, use_container_width=True)
                    # Compile vs Execute time comparison
                    if MATPLOTLIB_AVAILABLE and "Compile (s)" in df_prof.columns and "Exec (s)" in df_prof.columns:
                        fig, ax = plt.subplots(figsize=(10, 4))
                        df_p = df_prof.copy()
                        df_p["Compile (s)"] = pd.to_numeric(df_p["Compile (s)"], errors='coerce').fillna(0)
                        df_p["Exec (s)"] = pd.to_numeric(df_p["Exec (s)"], errors='coerce').fillna(0)
                        x = range(len(df_p))
                        ax.bar([i - 0.2 for i in x], df_p["Compile (s)"], 0.4, label="Compile", color="steelblue")
                        ax.bar([i + 0.2 for i in x], df_p["Exec (s)"], 0.4, label="Execute", color="tomato")
                        ax.set_xlabel("Query"); ax.set_ylabel("Seconds")
                        ax.set_title("Compile vs Execute Time per Query")
                        ax.set_xticks(list(x)); ax.set_xticklabels(df_p["Query ID"].astype(str), rotation=45, ha='right')
                        ax.legend(); plt.tight_layout()
                        st.pyplot(fig); plt.close()
                    else:
                        # Native fallback
                        cols = [c for c in ["Compile (s)", "Exec (s)"] if c in df_prof.columns]
                        if cols:
                            st.markdown("**Compile vs Execute Time**")
                            chart_df = df_prof[cols].apply(pd.to_numeric, errors='coerce').fillna(0)
                            st.bar_chart(chart_df)
                    st.download_button("📥 Download", df_prof.to_csv(index=False),
                                       f"query_profile_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_qp_prof")
                else:
                    st.info("Click 'Load Query Performance Data' to see results.")

        # =========================================================
        # 💰 COMPUTE COST
        # =========================================================
        with perf_tab3:
            st.subheader("💰 Compute Cost Analysis")
            if st.button("🔄 Load Cost Data", key="load_cc", type="primary"):
                with st.spinner("Loading..."):
                    errors = []
                    try:
                        st.session_state.cc_credits = fetch_warehouse_credits(
                            st.session_state.conn, perf_start, perf_end, perf_warehouse)
                    except Exception as e:
                        errors.append(f"Warehouse Credits: {e}")
                        st.session_state.cc_credits = pd.DataFrame()
                    try:
                        result = fetch_credit_usage_over_time(
                            st.session_state.conn, perf_start, perf_end, perf_warehouse)
                        st.session_state.cc_over_time = result[0]
                        st.session_state.cc_date_col  = result[1]
                    except Exception as e:
                        errors.append(f"Credits Over Time: {e}")
                        st.session_state.cc_over_time = pd.DataFrame()
                        st.session_state.cc_date_col  = "Date"
                    try:
                        st.session_state.cc_heatmap = fetch_cost_heatmap_data(
                            st.session_state.conn, perf_start, perf_end, perf_warehouse)
                    except Exception as e:
                        errors.append(f"Cost Heatmap: {e}")
                        st.session_state.cc_heatmap = pd.DataFrame()
                    if errors:
                        for err in errors: st.error(f"❌ {err}")
                    else:
                        st.success("✅ Data loaded!")

            # KPI summary row
            if 'cc_credits' in st.session_state and not st.session_state.cc_credits.empty:
                total_credits = pd.to_numeric(
                    st.session_state.cc_credits.get("Credits Used", pd.Series()), errors='coerce').sum()
                wh_count = st.session_state.cc_credits["Warehouse"].nunique() if "Warehouse" in st.session_state.cc_credits.columns else "—"
                kc1, kc2, kc3 = st.columns(3)
                kc1.metric("💳 Total Credits Used", f"{total_credits:,.2f}")
                kc2.metric("💵 Est. Cost @ $3/credit", f"${total_credits * 3:,.2f}")
                kc3.metric("🏭 Warehouses", wh_count)

            cc_sub1, cc_sub2, cc_sub3 = st.tabs(["📈 Credits Over Time", "🏭 By Warehouse", "🌡️ Cost Heatmap"])

            with cc_sub1:
                if 'cc_over_time' in st.session_state and not st.session_state.cc_over_time.empty:
                    date_col = st.session_state.get("cc_date_col", "Date")
                    st.dataframe(st.session_state.cc_over_time, use_container_width=True)
                    st.download_button("📥 Download", st.session_state.cc_over_time.to_csv(index=False),
                                       f"credit_usage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_cc_time")
                    _area_chart(st.session_state.cc_over_time, date_col, "Credits Used",
                                f"Credit Usage Over Time ({date_col})")
                else:
                    st.info("Click 'Load Cost Data' to see results.")

            with cc_sub2:
                if 'cc_credits' in st.session_state and not st.session_state.cc_credits.empty:
                    st.dataframe(st.session_state.cc_credits, use_container_width=True)
                    st.download_button("📥 Download", st.session_state.cc_credits.to_csv(index=False),
                                       f"warehouse_credits_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_cc_wh")
                    vc = [c for c in ["Compute Credits", "Cloud Services Credits"] if c in st.session_state.cc_credits.columns]
                    if vc:
                        _stacked_bar(st.session_state.cc_credits, "Warehouse", vc, "Cost Breakdown by Warehouse")
                else:
                    st.info("Click 'Load Cost Data' to see results.")

            with cc_sub3:
                if 'cc_heatmap' in st.session_state and not st.session_state.cc_heatmap.empty:
                    df_heat = st.session_state.cc_heatmap.copy()
                    st.download_button("📥 Download", df_heat.to_csv(index=False),
                                       f"cost_heatmap_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_cc_heat")
                    if MATPLOTLIB_AVAILABLE and all(c in df_heat.columns for c in ["DayOfWeek", "HourOfDay", "Credits"]):
                        df_heat["Credits"] = pd.to_numeric(df_heat["Credits"], errors='coerce').fillna(0)
                        day_order = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
                        pivot = df_heat.pivot_table(index='HourOfDay', columns='DayOfWeek', values='Credits', fill_value=0)
                        pivot = pivot.reindex(columns=[d for d in day_order if d in pivot.columns], fill_value=0)
                        fig, ax = plt.subplots(figsize=(12, 7))
                        sns.heatmap(pivot, cmap="YlGnBu", annot=True, fmt=".1f", linewidths=0.5, ax=ax)
                        ax.set_title("Credits Consumed by Day of Week & Hour of Day")
                        plt.tight_layout()
                        st.pyplot(fig); plt.close()
                    elif all(c in df_heat.columns for c in ["DayOfWeek", "HourOfDay", "Credits"]):
                        # Native fallback: pivot as a dataframe heatmap
                        df_heat["Credits"] = pd.to_numeric(df_heat["Credits"], errors='coerce').fillna(0)
                        pivot = df_heat.pivot_table(index='HourOfDay', columns='DayOfWeek', values='Credits', fill_value=0)
                        st.markdown("**Credits by Day of Week & Hour (install seaborn for heatmap visualization)**")
                        st.dataframe(pivot.style.background_gradient(cmap='Blues', axis=None), use_container_width=True)
                else:
                    st.info("Click 'Load Cost Data' to see results.")

        # =========================================================
        # 🗄️ STORAGE
        # =========================================================
        with perf_tab4:
            st.subheader("🗄️ Storage Analysis")
            if st.button("🔄 Load Storage Data", key="load_st", type="primary"):
                with st.spinner("Loading..."):
                    errors = []
                    try:
                        st.session_state.st_daily = fetch_daily_storage_usage(
                            st.session_state.conn, perf_start, perf_end)
                    except Exception as e:
                        errors.append(f"Daily Storage: {e}")
                        st.session_state.st_daily = pd.DataFrame()
                    try:
                        st.session_state.st_tables = fetch_table_storage_metrics(
                            st.session_state.conn,
                            perf_db   if perf_db   != "All" else None,
                            perf_schema if perf_schema != "All" else None)
                    except Exception as e:
                        errors.append(f"Table Storage: {e}")
                        st.session_state.st_tables = pd.DataFrame()
                    if errors:
                        for err in errors: st.error(f"❌ {err}")
                    else:
                        st.success("✅ Data loaded!")

            st_col1, st_col2 = st.columns(2)

            with st_col1:
                st.markdown("#### 📅 Daily Storage Usage (GB)")
                if 'st_daily' in st.session_state and not st.session_state.st_daily.empty:
                    st.dataframe(st.session_state.st_daily, use_container_width=True)
                    st.download_button("📥 Download", st.session_state.st_daily.to_csv(index=False),
                                       f"daily_storage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_st_daily")
                    _line_chart(st.session_state.st_daily, "Date", "Avg DB Storage (GB)",
                                "Average Daily Database Storage (GB)", color="teal")
                    # Also show failsafe trend if available
                    if "Avg Failsafe (GB)" in st.session_state.st_daily.columns:
                        _line_chart(st.session_state.st_daily, "Date", "Avg Failsafe (GB)",
                                    "Average Failsafe Storage (GB)", color="darkorange")
                else:
                    st.info("Click 'Load Storage Data' to see results.")

            with st_col2:
                st.markdown("#### 🗃️ Table Storage Details (MB)")
                if 'st_tables' in st.session_state and not st.session_state.st_tables.empty:
                    st.dataframe(st.session_state.st_tables, use_container_width=True)
                    st.download_button("📥 Download", st.session_state.st_tables.to_csv(index=False),
                                       f"table_storage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_st_tables")
                    _hbar_chart(st.session_state.st_tables, "Table", "Active Size (MB)",
                                "Top 10 Tables by Active Storage (MB)", color="mediumslateblue")
                else:
                    st.info("Select a specific Database and Schema, then click 'Load Storage Data'.")

        # =========================================================
        # 🏭 WAREHOUSE ACTIVITY
        # =========================================================
        with perf_tab5:
            st.subheader("🏭 Warehouse Activity")
            if st.button("🔄 Load Warehouse Activity Data", key="load_wa", type="primary"):
                with st.spinner("Loading..."):
                    errors = []
                    try:
                        st.session_state.wa_util = fetch_warehouse_utilization(
                            st.session_state.conn, perf_start, perf_end, perf_warehouse)
                    except Exception as e:
                        errors.append(f"Warehouse Utilization: {e}")
                        st.session_state.wa_util = pd.DataFrame()
                    if errors:
                        for err in errors: st.error(f"❌ {err}")
                    else:
                        st.success("✅ Data loaded!")

            if 'wa_util' in st.session_state and not st.session_state.wa_util.empty:
                df_wa = st.session_state.wa_util
                st.dataframe(df_wa, use_container_width=True)
                st.download_button("📥 Download", df_wa.to_csv(index=False),
                                   f"warehouse_activity_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", key="dl_wa")

                wa_col1, wa_col2 = st.columns(2)
                with wa_col1:
                    # Daily avg running trend
                    if "Date" in df_wa.columns and "Avg Running" in df_wa.columns:
                        df_trend = df_wa.copy()
                        df_trend["Avg Running"] = pd.to_numeric(df_trend["Avg Running"], errors='coerce').fillna(0)
                        try:
                            df_trend["Date"] = pd.to_datetime(df_trend["Date"])
                        except Exception:
                            pass
                        daily = df_trend.groupby("Date")["Avg Running"].mean().reset_index()
                        _line_chart(daily, "Date", "Avg Running",
                                    "Daily Avg Running Queries", color="royalblue")

                with wa_col2:
                    # Avg load by warehouse (grouped bar)
                    if "Warehouse" in df_wa.columns:
                        cols = [c for c in ["Avg Running", "Avg Queued", "Avg Blocked"] if c in df_wa.columns]
                        if cols:
                            for c in cols:
                                df_wa[c] = pd.to_numeric(df_wa[c], errors='coerce').fillna(0)
                            grp = df_wa.groupby("Warehouse")[cols].mean()
                            if MATPLOTLIB_AVAILABLE:
                                fig, ax = plt.subplots(figsize=(8, 4))
                                grp.plot(kind='bar', ax=ax, cmap='Set2')
                                ax.set_xlabel("Warehouse"); ax.set_ylabel("Average Count")
                                ax.set_title("Avg Running / Queued / Blocked by Warehouse")
                                plt.xticks(rotation=45, ha='right'); plt.tight_layout()
                                st.pyplot(fig); plt.close()
                            else:
                                st.markdown("**Avg Load by Warehouse**")
                                st.bar_chart(grp)
            else:
                st.info("Click 'Load Warehouse Activity Data' to see results.")


# ========== MAIN EXECUTION ==========
if st.session_state.is_logged_in:
    show_main_app()
else:
    show_login_page()

st.markdown("---")
# ========== FIXED FOOTER ==========
st.markdown("""
<div class="fixed-footer">
    <p>DeploySure Suite v2.0 | CloudLabs Inc | © 2024</p>
</div>
""", unsafe_allow_html=True)