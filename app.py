# app.py
import streamlit as st
import pandas as pd
from log_parser import parse_log_file
from config import CEID_MAP, ALARM_DB # Use ALARM_DB now
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
            # Map the description using the new ALARM_DB
            df['AlarmDescription'] = pd.to_numeric(df['details.AlarmID'], errors='coerce').map(
                {k: v['description'] for k, v in ALARM_DB.items()}
            ).fillna('')
        
        summary = analyze_data(df)
        eda_results = perform_eda(df)

    st.header("Job Performance Dashboard")
    st.markdown("---")
    
    # --- START OF HIGHLIGHTED FIX ---
    c1, c2, c3 = st.columns(3)
    c1.metric("First Lot ID Found", str(summary['lot_id']))
    c2.metric("Total Panels in First Lot", int(summary['panel_count']))
    c3.metric("Total Downtime (sec)", f"{summary['total_downtime_sec']:.2f}")

    st.markdown("---")
    
    st.subheader("Log Context Overview")
    c1, c2, c3 = st.columns(3) # Simplified layout
    with c1:
        st.write("**Operator(s) Logged In**")
        if summary['operator_ids']: st.dataframe(pd.DataFrame(summary['operator_ids'], columns=["ID"]), hide_index=True)
        else: st.info("N/A")
    with c2:
        st.write("**Magazine ID(s) Used**")
        if summary['magazine_ids']: st.dataframe(pd.DataFrame(summary['magazine_ids'], columns=["ID"]), hide_index=True)
        else: st.info("N/A")
    with c3:
        st.write("**Machine Status(es)**")
        if summary['machine_statuses']: st.dataframe(pd.DataFrame(summary['machine_statuses'], columns=["Status"]), hide_index=True)
        else: st.info("Unknown")
            
    st.markdown("---")

    st.subheader("Downtime Analysis")
    if summary['alarms_with_context']:
        alarm_df = pd.DataFrame(summary['alarms_with_context'])
        st.dataframe(alarm_df, hide_index=True, use_container_width=True)
    else:
        st.success("✅ No Downtime Incidents Found")
    # --- END OF HIGHLIGHTED FIX ---

    with st.expander("Show Full Log Exploratory Data Analysis (EDA)"):
        # ... (rest of the app remains the same)
