# -*- coding: utf-8 -*-
import streamlit as st
import snowflake.connector
import pandas as pd
from datetime import datetime
import re
import logging
import traceback
from matplotlib import pyplot as plt

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Page configuration - MUST BE FIRST
st.set_page_config(
    page_title="DeploySure Suite",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
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
    
    # Main tabs
    tab1, tab2 = st.tabs(["⎘ MirrorSchema", "🔍 DriftWatch"])
    
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
                    
                    # Show pass/fail summary
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
                        
                        # Create visualization
                        if 'dq_details' in st.session_state and not st.session_state.dq_details.empty:
                            details = st.session_state.dq_details
                            pass_count = len(details[details['Status'].str.contains('Pass')])
                            fail_count = len(details[details['Status'].str.contains('Fail')])
                            
                            if pass_count + fail_count > 0:
                                fig, ax = plt.subplots(figsize=(8, 4))
                                ax.bar(['Passed', 'Failed'], [pass_count, fail_count], color=['green', 'red'])
                                ax.set_ylabel('Number of Checks')
                                ax.set_title('Data Quality Check Results')
                                st.pyplot(fig)
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

# ========== MAIN EXECUTION ==========
if st.session_state.is_logged_in:
    show_main_app()
else:
    show_login_page()

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 10px;'>
    <p>DeploySure Suite v1.0 | Powered by Streamlit & Snowflake</p>
</div>
""", unsafe_allow_html=True)