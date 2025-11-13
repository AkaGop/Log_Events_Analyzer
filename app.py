# app.py
import streamlit as st
import pandas as pd
from log_parser import parse_log_file
from config import CEID_MAP, ALARM_DB
from analyzer import analyze_data, perform_eda # analyzer functions are now updated

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
        else:
            df['EventName'] = "Unknown"

        if 'details.AlarmID' in df.columns:
            df['AlarmDescription'] = pd.to_numeric(df['details.AlarmID'], errors='coerce').map(
                {k: v['description'] for k, v in ALARM_DB.items()}
            ).fillna('')
        
        summary = analyze_data(df)
        eda_results = perform_eda(df)

    st.header("Job Performance Dashboard")
    st.markdown("---")
    
    # MODIFIED: Added Mapping metrics
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("First Lot ID Found", str(summary['lot_id']))
    c2.metric("Total Panels Processed", summary['panel_count'])
    c3.metric("Mapping Start Time", summary['mapping_details']['start_time'])
    c4.metric("Mapping Duration (sec)", f"{summary['mapping_details']['duration_sec']:.2f}")

    st.markdown("---")
    
    st.subheader("Log Context Overview")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.write("**Operator(s) Logged In**")
        st.dataframe(pd.DataFrame(summary['operator_ids'], columns=["ID"]), hide_index=True)
    with c2:
        st.write("**Magazine ID(s) Used**")
        st.dataframe(pd.DataFrame(summary['magazine_ids'], columns=["ID"]), hide_index=True)
    with c3:
        st.write("**Machine Status(es)**")
        st.dataframe(pd.DataFrame(summary['machine_statuses'], columns=["Status"]), hide_index=True)
            
    st.markdown("---")

    # NEW SECTION: Display Panel and Slot information
    st.subheader("Panel & Slot Details")
    col1, col2 = st.columns([1, 2])
    with col1:
        st.write("**Unique Panel IDs Found**")
        if summary['panel_info']['panel_ids']:
            st.dataframe(pd.DataFrame(summary['panel_info']['panel_ids'], columns=["Panel ID"]), hide_index=True)
        else:
            st.info("No Panel IDs found.")
    with col2:
        st.write("**Panel to Magazine Slot Map**")
        if not summary['panel_info']['panel_slot_map'].empty:
            st.dataframe(summary['panel_info']['panel_slot_map'], hide_index=True, use_container_width=True)
        else:
            st.info("No Panel/Slot mapping found.")

    st.markdown("---")
    
    alarm_title = "Downtime Analysis"
    st.subheader(alarm_title)
    if summary['alarms_with_context']:
        alarm_df = pd.DataFrame(summary['alarms_with_context'])
        st.dataframe(alarm_df, hide_index=True, use_container_width=True)
    else:
        st.success("✅ No Downtime Incidents Found")

    with st.expander("Show Full Log Exploratory Data Analysis (EDA)"):
        # (EDA section remains the same)
        st.subheader("Event Frequency (Entire Log)")
        st.bar_chart(eda_results['event_counts'])
        st.subheader("Alarm Analysis (Entire Log)")
        if not eda_results['alarm_counts'].empty:
            st.bar_chart(eda_results['alarm_counts'])
            st.dataframe(eda_results['alarm_table'], use_container_width=True)
        else:
            st.success("✅ No Alarms Found in the Entire Log")

    st.header("Detailed Event Log")
    if not df.empty:
        cols = ["timestamp", "EventName", "details.AlarmID", "AlarmDescription", "details.LotID", "details.PanelCount", "details.MagazineID", "details.OperatorID"]
        display_cols = [col for col in cols if col in df.columns]
        st.dataframe(df[display_cols].style.format(na_rep='-'), hide_index=True, use_container_width=True)
    else:
        st.warning("No meaningful events were found.")
else:
    st.title("Welcome"); st.info("⬅️ Please upload a log file to begin.")
