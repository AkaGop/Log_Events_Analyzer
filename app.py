# app.py
import streamlit as st
import pandas as pd
from log_parser import parse_log_file
from config import CEID_MAP, ALARM_DB
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
        else:
            df['EventName'] = "Unknown"

        if 'details.AlarmID' in df.columns:
            df['AlarmDescription'] = pd.to_numeric(df['details.AlarmID'], errors='coerce').map(
                {k: v.get('description', 'Unknown') for k, v in ALARM_DB.items()}
            ).fillna('')
        
        summary = analyze_data(df)
        eda_results = perform_eda(df)

    tab1, tab2 = st.tabs(["Main Dashboard", "Process Details"])

    with tab1:
        st.header("Job Performance Dashboard")
        st.markdown("---")
        
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

        st.subheader("Magazine Mapping Performance")
        c1, c2, c3 = st.columns(3)
        c1.metric("Mapping Start Time", summary['mapping_details']['start_time'])
        c2.metric("Mapping End Time", summary['mapping_details']['end_time'])
        c3.metric("Mapping Duration (sec)", f"{summary['mapping_details']['duration_sec']:.2f}")
        
        st.markdown("---")

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
        
        st.subheader("Lot to Panel ID Mapping")
        if summary['lot_to_panel_map']:
            for lot_id, panel_ids in summary['lot_to_panel_map'].items():
                st.write(f"**Lot ID:** `{lot_id}`")
                st.dataframe(pd.DataFrame(panel_ids, columns=["Panel ID"]), hide_index=True)
        else:
            st.info("No Lot to Panel mapping found in this log.")

        st.markdown("---")

        st.subheader("Cycle Time per Panel")
        if not summary['cycle_time_details']['cycle_times'].empty:
            st.line_chart(summary['cycle_time_details']['cycle_times'])
        else:
            st.info("No cycle time data to display.")
    
    with st.expander("Show Full Log Exploratory Data Analysis (EDA)"):
        st.subheader("Event Frequency (Entire Log)")
        if not eda_results['event_counts'].empty:
            st.bar_chart(eda_results['event_counts'])
        else:
            st.info("No events to analyze.")
        
        st.subheader("Alarm Analysis (Entire Log)")
        if not eda_results['alarm_counts'].empty:
            st.write("Alarm Counts:")
            st.bar_chart(eda_results['alarm_counts'])
            st.write("Alarm Events Log:")
            st.dataframe(eda_results['alarm_table'], use_container_width=True)
        else:
            st.success("✅ No Alarms Found in the Entire Log")

    st.header("Detailed Event Log")
    if not df.empty:
        cols = ["timestamp", "EventName", "details.AlarmID", "AlarmDescription", "details.LotID", "details.PanelCount", "details.MagazineID", "details.OperatorID"]
        display_cols = [col for col in cols if col in df.columns]

        display_df = df[display_cols].copy()
        display_df['timestamp'] = pd.to_datetime(display_df['timestamp']).dt.strftime('%Y/%m/%d %H:%M:%S')

        st.dataframe(display_df.style.format(na_rep='-'), hide_index=True, use_container_width=True)
    else:
        st.warning("No meaningful events were found.")
else:
    st.title("Welcome"); st.info("⬅️ Please upload a log file to begin.")
