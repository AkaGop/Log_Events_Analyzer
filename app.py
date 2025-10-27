# app.py
import streamlit as st
import pandas as pd
from log_parser import parse_log_file
from config import CEID_MAP, ALARM_CODE_MAP
from analyzer import analyze_data, perform_eda

st.set_page_config(page_title="Hirata Log Analyzer", layout="wide")
st.title("Hirata Equipment Log Analyzer")

with st.sidebar:
    st.title("🤖 Log Analyzer")
    uploaded_file = st.file_uploader("Upload Hirata Log File", type=['txt', 'log'])
    st.info("This tool provides engineering analysis of Hirata SECS/GEM logs.")

if uploaded_file:
    with st.spinner("Analyzing log file..."):
        parsed_events = parse_log_file(uploaded_file)
        df = pd.json_normalize(parsed_events)

        if 'details.CEID' in df.columns:
            df['EventName'] = pd.to_numeric(df['details.CEID'], errors='coerce').map(CEID_MAP)
            if 'details.RCMD' in df.columns:
                df['EventName'].fillna(df['details.RCMD'], inplace=True)
            df['EventName'].fillna("Unknown", inplace=True)
        elif 'details.RCMD' in df.columns:
            df['EventName'] = df['details.RCMD']
        else:
            df['EventName'] = "Unknown"

        if 'details.AlarmID' in df.columns:
            df['AlarmDescription'] = pd.to_numeric(df['details.AlarmID'], errors='coerce').map(ALARM_CODE_MAP).fillna('')
        
        summary = analyze_data(df)
        eda_results = perform_eda(df)

    st.header("Job Performance Dashboard")
    st.markdown("---")
    
    c1, c2, c3 = st.columns(3)
    c1.metric("Job Status", summary['job_status'])
    c2.metric("Lot ID", str(summary['lot_id']))
    c3.metric("Total Panels", int(summary['panel_count']))

    st.markdown("---")
    
    st.subheader("Job Context")
    c1, c2, c3 = st.columns(3)
    
    c1.metric("Magazine ID", summary['magazine_id'])

    # --- START OF HIGHLIGHTED FIX ---
    with c2:
        st.write("**Operator(s) Logged In**")
        if summary['operator_ids']:
            op_df = pd.DataFrame(summary['operator_ids'], columns=["Operator ID"])
            st.dataframe(op_df, hide_index=True, use_container_width=True)
        else:
            st.metric("Operator ID", "N/A")
    # --- END OF HIGHLIGHTED FIX ---

    c3.metric("Machine Status", summary['machine_status'])

    st.subheader("Alarms Triggered During Job")
    if summary['unique_alarms_count'] > 0:
        st.error(f"**{summary['unique_alarms_count']}** unique alarm(s) occurred:")
        for alarm_desc in summary['alarms_list']:
            st.markdown(f"- `{alarm_desc}`")
    else:
        st.success("✅ No Alarms Found During This Job")

    with st.expander("Show Full Log Exploratory Data Analysis (EDA)"):
        st.subheader("Event Frequency (Entire Log)")
        # ... (rest of the app remains the same)
