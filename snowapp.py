import streamlit as st
import snowflake.connector
import pandas as pd

st.set_page_config(page_title="Investigation Report", layout="wide")

st.title("🔎 Investigation Report - Query Input Screen")

# --- Helper function: label + From input + "to" + To input ---
def input_pair(label, type="text"):
    col_label, col_from, col_txt, col_to = st.columns([1, 2, 0.26, 2])

    with col_label:
        st.markdown(f"**{label}**")

    with col_from:
        if type == "date":
            val_from = st.date_input(f"{label} From", value=None, label_visibility="collapsed", key=f"{label}_from")
        else:
            val_from = st.text_input(f"{label} From", label_visibility="collapsed", key=f"{label}_from")

    with col_txt:
        st.write("to")

    with col_to:
        if type == "date":
            val_to = st.date_input(f"{label} To", value=None, label_visibility="collapsed", key=f"{label}_to")
        else:
            val_to = st.text_input(f"{label} To", label_visibility="collapsed", key=f"{label}_to")

    return val_from, val_to


# --- Layout: Form + Image side by side ---
col_form, col_img = st.columns([1.7, 1])

with col_form:
    with st.form("query_form"):
        st.subheader("Report Selection")

        plant_from, plant_to = input_pair("Plant")
        material_from, material_to = input_pair("Material")
        batch_from, batch_to = input_pair("Batch Number")
        serial_from, serial_to = input_pair("Serial Number")
        prod_date_from, prod_date_to = input_pair("Production Order Date", type="date")
        firmware_from, firmware_to = input_pair("Firmware Version")
        sterile_from, sterile_to = input_pair("Sterile Load Number")
        vendor_from, vendor_to = input_pair("Vendor Batch Number")
        gr_date_from, gr_date_to = input_pair("Goods Receipt Date", type="date")

        # --- Buttons Side by Side ---
        col1, col2 = st.columns(2)
        with col1:
            submitted = st.form_submit_button("Submit")
        with col2:
            run_query = st.form_submit_button("Run Sample Query")

with col_img:
    # ✅ Replace "your_image.png" with a local path or URL
    st.image("Screenshot 2025-09-05 174237.png", caption="Report Guide", use_container_width=True)
# --- Process Results ---
if submitted:
    results = {}

    def add_if_filled(label, val_from, val_to):
        if val_from not in (None, ""):
            val_from = str(val_from)
        else:
            val_from = ""

        if val_to not in (None, ""):
            val_to = str(val_to)
        else:
            val_to = ""

        if val_from and val_to:
            results[label] = f"{val_from} to {val_to}"
        elif val_from:
            results[label] = f"{val_from}"
        elif val_to:
            results[label] = f"till {val_to}"

    add_if_filled("Plant", plant_from, plant_to)
    add_if_filled("Material", material_from, material_to)
    add_if_filled("Batch Number", batch_from, batch_to)
    add_if_filled("Serial Number", serial_from, serial_to)
    add_if_filled("Production Order Date", prod_date_from, prod_date_to)
    add_if_filled("Firmware Version", firmware_from, firmware_to)
    add_if_filled("Sterile Load Number", sterile_from, sterile_to)
    add_if_filled("Vendor Batch Number", vendor_from, vendor_to)
    add_if_filled("Goods Receipt Date", gr_date_from, gr_date_to)

    if results:
        with st.expander("📌 Filled Fields", expanded=True):
            for k, v in results.items():
                st.write(f"**{k}**: {v}")
    else:
        st.warning("⚠️ No fields were filled in.")

# --- Run Sample Snowflake Query ---


if run_query:
    st.info("Running sample query...")

    # Convert input to integers
    plant_from_val = int(plant_from) if plant_from not in (None, "") else 0
    plant_to_val = int(plant_to) if plant_to not in (None, "") else 999999

    # Build query
    query = f"""
    SELECT * 
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.CUSTOMER
    WHERE C_CUSTKEY >= {plant_from_val} AND C_CUSTKEY <= {plant_to_val};
    """

    # Connect and run
    conn = snowflake.connector.connect(
        user="Snow",
        password="Technology@98983",
        account="wzuqsfs"
        # warehouse="YOUR_WAREHOUSE",
        # database="SNOWFLAKE_SAMPLE_DATA",
        # schema="TPCH_SF10"
    )

    df = pd.read_sql(query, conn)
    conn.close()

    st.dataframe(df)

    csv = df.to_csv(index=False)
    st.download_button(
        label="📥 Download Query Result as CSV",
        data=csv,
        file_name="query_result.csv",
        mime="text/csv"
    )

# Upload CSV file
uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file is not None:
    # Read CSV into dataframe
    df = pd.read_csv(uploaded_file)
    st.write("Preview of uploaded data:", df.head())

    if st.button("Upload to Snowflake"):
        try:
            conn = snowflake.connector.connect(
                user="Snow",
                password="Technology@98983",
                account="wzuqsfs"
            )
            cur = conn.cursor()

            # Replace with your existing table name
            table_name = "SAMPLE_SAP.Example.MY_NEW_TABLE"

            # Create insert statement dynamically
            cols = ",".join(df.columns)
            placeholders = ",".join(["?"] * len(df.columns))
            insert_query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

            # Insert all rows
            cur.executemany(insert_query, df.values.tolist())
            conn.commit()

            st.success(f"✅ Successfully uploaded {len(df)} rows to {table_name}")

        except Exception as e:
            st.error(f"❌ Error: {e}")

        finally:
            cur.close()
            conn.close()