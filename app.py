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
        else:
            df['EventName'] = "Unknown"

        if 'details.AlarmID' in df.columns:
            df['AlarmDescription'] = pd.to_numeric(df['details.AlarmID'], errors='coerce').map(ALARM_CODE_MAP).fillna('')
        
        summary = analyze_data(df)
        eda_results = perform_eda(df)

    st.header("Job Performance Dashboard")
    st.markdown("---")
    
    c1, c2 = st.columns(2)
    c1.metric("First Lot ID Found", str(summary['lot_id']))
    c2.metric("Total Panels in First Lot", int(summary['panel_count']))

    st.markdown("---")
    
    st.subheader("Log Context Overview")
    c1, c2, c3, c4 = st.columns(4)

    # --- START OF HIGHLIGHTED FIX ---
    # Replaced flawed logic with a robust if/elif/else structure.
    with c1:
        st.write("**Lot ID(s) Found**")
        if not summary['lot_ids']:
            st.info("Dummy Lot or NA")
        elif len(summary['lot_ids']) == 1:
            st.metric(label="Lot ID", value=summary['lot_ids'][0])
        else:
            st.dataframe(pd.DataFrame(summary['lot_ids'], columns=["ID"]), hide_index=True)

    with c2:
        st.write("**Magazine ID(s) Used**")
        if not summary['magazine_ids']:
            st.info("N/A")
        elif len(summary['magazine_ids']) == 1:
            st.metric(label="Magazine ID", value=summary['magazine_ids'][0])
        else:
            st.dataframe(pd.DataFrame(summary['magazine_ids'], columns=["ID"]), hide_index=True)

    with c3:
        st.write("**Operator(s) Logged In**")
        if not summary['operator_ids']:
            st.info("N/A")
        elif len(summary['operator_ids']) == 1:
            st.metric(label="Operator ID", value=summary['operator_ids'][0])
        else:
            st.dataframe(pd.DataFrame(summary['operator_ids'], columns=["ID"]), hide_index=True)

    with c4:
        st.write("**Machine Status(es)**")
        if not summary['machine_statuses']:
            st.info("Unknown")
        elif len(summary['machine_statuses']) == 1:
            st.metric(label="Status", value=summary['machine_statuses'][0])
        else:
            st.dataframe(pd.DataFrame(summary['machine_statuses'], columns=["Status"]), hide_index=True)
    # --- END OF HIGHLIGHTED FIX ---
            
    st.markdown("---")
    
    st.subheader("Key Event Timestamps")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.write("**Operator Logins**")
        if summary['login_events']: st.dataframe(pd.DataFrame(summary['login_events']), hide_index=True)
        else: st.info("No login events.")
    with c2:
        st.write("**Magazine Dock Events**")
        if summary['dock_events']: st.dataframe(pd.DataFrame(summary['dock_events']), hide_index=True)
        else: st.info("No dock events.")
    with c3:
        st.write("**Machine Status Changes**")
        if summary['status_events']: st.dataframe(pd.DataFrame(summary['status_events']), hide_index=True)
        else: st.info("No status changes.")
            
    st.markdown("---")
    
    alarm_title = "Alarms Triggered During Job" if summary['job_status'] != "No Job Found" else "Alarms Found in Log"
    st.subheader(alarm_title)
    if summary['alarms_with_context']:
        alarm_df = pd.DataFrame(summary['alarms_with_context'])
        st.dataframe(alarm_df, hide_index=True, use_container_width=True)
    else:
        st.success("✅ No Alarms Found")

    with st.expander("Show Full Log Exploratory Data Analysis (EDA)"):
        st.subheader("Event Frequency (Entire Log)")
        if not eda_results['event_counts'].empty: st.bar_chart(eda_results['event_counts'])
        else: st.info("No events to analyze.")
        
        st.subheader("Alarm Analysis (Entire Log)")
        if not eda_results['alarm_counts'].empty:
            st.write("Alarm Counts:"); st.bar_chart(eda_results['alarm_counts'])
            st.write("Alarm Events Log:"); st.dataframe(eda_results['alarm_table'], use_container_width=True)
        else: st.success("✅ No Alarms Found in the Entire Log")

    st.header("Detailed Event Log")
    if not df.empty:
        cols = ["timestamp", "EventName", "details.AlarmID", "AlarmDescription", "details.LotID", "details.PanelCount", "details.MagazineID", "details.OperatorID"]
        display_cols = [col for col in cols if col in df.columns]
        st.dataframe(df[display_cols].style.format(na_rep='-'), hide_index=True, use_container_width=True)
    else:
        st.warning("No meaningful events were found.")
else:
    st.title("Welcome"); st.info("⬅️ Please upload a log file to begin.")
