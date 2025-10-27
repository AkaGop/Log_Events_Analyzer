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
    
    c1, c2 = st.columns(2)
    c1.metric("First Lot ID Found", str(summary['lot_id']))
    c2.metric("Total Panels in First Lot", int(summary['panel_count']))

    st.markdown("---")
    
    st.subheader("Log Context Overview")
    c1, c2, c3, c4 = st.columns(4)
    # This section remains unchanged and is correct.
    with c1:
        st.write("**Lot ID(s) Found**"); st.dataframe(pd.DataFrame(summary['lot_ids'], columns=["ID"]), hide_index=True) if summary['lot_ids'] else st.info("Dummy Lot or NA")
    with c2:
        st.write("**Magazine ID(s) Used**"); st.dataframe(pd.DataFrame(summary['magazine_ids'], columns=["ID"]), hide_index=True) if summary['magazine_ids'] else st.info("N/A")
    with c3:
        st.write("**Operator(s) Logged In**"); st.dataframe(pd.DataFrame(summary['operator_ids'], columns=["ID"]), hide_index=True) if summary['operator_ids'] else st.info("N/A")
    with c4:
        st.write("**Machine Status(es)**"); st.dataframe(pd.DataFrame(summary['machine_statuses'], columns=["Status"]), hide_index=True) if summary['machine_statuses'] else st.info("Unknown")

    st.subheader("Key Event Timestamps")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.write("**Operator Logins**"); st.dataframe(pd.DataFrame(summary['login_events']), hide_index=True) if summary['login_events'] else st.info("No login events.")
    with c2:
        st.write("**Magazine Dock Events**"); st.dataframe(pd.DataFrame(summary['dock_events']), hide_index=True) if summary['dock_events'] else st.info("No dock events.")
    with c3:
        st.write("**Machine Status Changes**"); st.dataframe(pd.DataFrame(summary['status_events']), hide_index=True) if summary['status_events'] else st.info("No status changes.")

    # --- START OF HIGHLIGHTED FIX ---
    # Update the title based on whether a job was found or not
    alarm_title = "Alarms Triggered During Job" if summary['job_status'] != "No Job Found" else "Alarms Found in Log"
    st.subheader(alarm_title)
    if summary['unique_alarms_count'] > 0:
        st.error(f"**{summary['unique_alarms_count']}** unique alarm(s) occurred:")
        for alarm_desc in summary['alarms_list']:
            st.markdown(f"- `{alarm_desc}`")
    else:
        st.success("✅ No Alarms Found")
    # --- END OF HIGHLIGHTED FIX ---

    with st.expander("Show Full Log Exploratory Data Analysis (EDA)"):
        # This section remains unchanged.
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
