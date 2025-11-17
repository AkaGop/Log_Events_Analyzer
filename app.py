# app.py
import streamlit as st
import pandas as pd
import sys
import os

# --- THIS IS THE FIX ---
# Add the script's directory to Python's path to ensure it can find the other modules.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# ---------------------

from log_parser import parse_log_file
from config import CEID_MAP, ALARM_DB
from analyzer import analyze_data, perform_eda

st.set_page_config(page_title="Hirata Log Analyzer", layout="wide")
st.title("Hirata Equipment Log Analyzer")
# (Sidebar code remains the same)
# ...

if uploaded_file:
    with st.spinner("Analyzing log file..."):
        # (File parsing logic remains the same)
        # ...
        summary = analyze_data(df)
        eda_results = perform_eda(df)

    # --- NEW: Tabbed Interface ---
    tab1, tab2 = st.tabs(["Main Dashboard", "Process Details"])

    with tab1:
        st.header("Job Performance Dashboard")
        st.markdown("---")
        
        # --- MODIFIED: Main KPIs ---
        processing_time = summary['cycle_time_details']['total_processing_time_sec']
        panel_count = summary['panel_count']
        
        avg_cycle_time = (processing_time / panel_count) if panel_count > 0 else 0
        uph = (panel_count / processing_time * 3600) if processing_time > 0 else 0
        
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Total Processing Time (sec)", f"{processing_time:.2f}")
        c2.metric("Total Downtime (sec)", f"{summary['total_downtime_sec']:.2f}")
        c3.metric("Total Panels Processed", panel_count)
        c4.metric("Avg. Cycle Time (sec)", f"{avg_cycle_time:.2f}")
        c5.metric("Units Per Hour (UPH)", f"{uph:.1f}")

        st.markdown("---")
        
        st.subheader("Downtime Analysis")
        if summary['alarms_with_context']:
            st.dataframe(pd.DataFrame(summary['alarms_with_context']), hide_index=True, use_container_width=True)
        else:
            st.success("✅ No Downtime Incidents Found")

    with tab2:
        st.header("Process Details")
        st.markdown("---")

        # --- NEW: Mapping Metrics ---
        st.subheader("Magazine Mapping Performance")
        c1, c2, c3 = st.columns(3)
        c1.metric("Mapping Start Time", summary['mapping_details']['start_time'])
        c2.metric("Mapping End Time", summary['mapping_details']['end_time'])
        c3.metric("Mapping Duration (sec)", f"{summary['mapping_details']['duration_sec']:.2f}")
        
        st.markdown("---")

        # --- NEW: Panel & Slot Details ---
        st.subheader("Panel & Slot Details")
        col1, col2 = st.columns([1, 2])
        with col1:
            st.write("**Unique Panel IDs Found**")
            st.dataframe(pd.DataFrame(summary['panel_info']['panel_ids'], columns=["Panel ID"]), hide_index=True)
        with col2:
            st.write("**Panel to Magazine Slot Map**")
            st.dataframe(summary['panel_info']['panel_slot_map'], hide_index=True, use_container_width=True)

        st.markdown("---")

        # --- NEW: Cycle Time Chart ---
        st.subheader("Cycle Time per Panel")
        if not summary['cycle_time_details']['cycle_times'].empty:
            st.line_chart(summary['cycle_time_details']['cycle_times'])
        else:
            st.info("No cycle time data to display.")
    
    # (EDA and Detailed Log sections remain the same)
    # ...

else:
    st.title("Welcome"); st.info("⬅️ Please upload a log file to begin.")
