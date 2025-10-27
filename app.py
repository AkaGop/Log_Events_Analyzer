# app.py
import streamlit as st
import pandas as pd
from log_parser import parse_log_file
from config import CEID_MAP, ALARM_CODE_MAP # Import the new alarm map
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

        # --- START OF NEW MAPPING LOGIC ---
        if 'details.AlarmID' in df.columns:
            # Create a new column for the human-readable alarm description
            df['AlarmDescription'] = pd.to_numeric(df['details.AlarmID'], errors='coerce').map(ALARM_CODE_MAP).fillna('')
        # --- END OF NEW MAPPING LOGIC ---

        summary = analyze_data(df)
        eda_results = perform_eda(df)

    st.header("Job Performance Dashboard")
    st.markdown("---")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Job Status", summary['job_status'])
    c2.metric("Lot ID", str(summary['lot_id']))
    c3.metric("Total Panels", int(summary['panel_count']))
    c4.metric("Alarms Triggered", summary['unique_alarms_count'])

    with st.expander("Show Exploratory Data Analysis (EDA)"):
        st.subheader("Event Frequency")
        if not eda_results['event_counts'].empty: st.bar_chart(eda_results['event_counts'])
        else: st.info("No events to analyze.")
        
        st.subheader("Alarm Analysis")
        if not eda_results['alarm_counts'].empty:
            st.write("Alarm Counts:"); st.bar_chart(eda_results['alarm_counts'])
            # Update the displayed table to include the new description
            st.write("Alarm Events Log:"); st.dataframe(eda_results['alarm_table'], use_container_width=True)
        else: st.success("✅ No Alarms Found in Log")

    st.header("Detailed Event Log")
    if not df.empty:
        # Add 'AlarmDescription' to the list of columns to display
        cols = [
            "timestamp", "msg_name", "EventName", "details.AlarmID", "AlarmDescription", 
            "details.LotID", "details.PanelCount", "details.MagazineID", "details.OperatorID", 
            "details.PortID", "details.PortStatus"
        ]
        display_cols = [col for col in cols if col in df.columns]
        st.dataframe(df[display_cols].style.format(na_rep='-'), hide_index=True, use_container_width=True)
    else: st.warning("No meaningful events were found.")
else:
    st.title("Welcome"); st.info("⬅️ Please upload a log file to begin.")
