# analyzer.py
from datetime import datetime
import pandas as pd
from config import ALARM_DB

def perform_eda(df: pd.DataFrame) -> dict:
    # This function is correct.
    # ... (code from previous response)

def format_time(timestamp_str: str) -> str:
    # This function is correct.
    # ... (code from previous response)

# --- START OF HIGHLIGHTED FIX ---
def analyze_data(df: pd.DataFrame) -> dict:
    """Analyzes a dataframe of parsed events to calculate high-level and contextual KPIs."""
    summary = {
        "job_status": "No Job Found", "lot_id": "N/A", "panel_count": 0,
        "total_downtime_sec": 0.0, "alarms_with_context": [],
        "magazine_ids": [], "operator_ids": [], "machine_statuses": [], "lot_ids": [],
        "login_events": [], "dock_events": [], "status_events": []
    }
    
    if df.empty: return summary

    # General Context gathering is correct.
    # ... (code for operator_ids, magazine_ids, etc. from previous response)

    analysis_scope_df = df
    
    start_events = df[df['EventName'] == 'LOADSTART']
    if start_events.empty:
        summary['lot_id'] = "Dummy Lot or NA"
    else:
        # Job finding logic is correct.
        # ... (code from previous response)

    # --- DOWNTIME CALCULATION LOGIC (REVISED AND CORRECTED) ---
    downtime_incidents = []
    total_downtime = 0.0

    # 1. Create a set of valid alarm/error codes for efficient lookup.
    stoppable_alarm_codes = {k for k, v in ALARM_DB.items() if v['level'] in ['Error', 'Alarm']}

    # 2. Create a numeric version of the AlarmID column, converting non-numbers to NaN.
    numeric_alarm_ids = pd.to_numeric(analysis_scope_df['details.AlarmID'], errors='coerce')

    # 3. Filter the dataframe to get only rows that are stoppable alarms.
    fault_events = analysis_scope_df[numeric_alarm_ids.isin(stoppable_alarm_codes)].copy()
    
    full_log_list = df.to_dict('records')

    for index, alarm_row in fault_events.iterrows():
        alarm_time = datetime.strptime(alarm_row['timestamp'], "%Y/%m/%d %H:%M:%S.%f")
        
        # 4. Correctly get the alarm_id for the CURRENT row in the loop.
        alarm_id = int(alarm_row['details.AlarmID'])
        alarm_info = ALARM_DB.get(alarm_id, {'description': 'Unknown Alarm'})
        
        try:
            alarm_log_index = df.index.get_loc(index)
        except KeyError:
            continue

        recovery_time = None
        for i in range(alarm_log_index + 1, len(full_log_list)):
            next_event = full_log_list[i]
            if next_event.get('EventName') != 'Alarm Set':
                recovery_time = datetime.strptime(next_event['timestamp'], "%Y/%m/%d %H:%M:%S.%f")
                break
        
        if recovery_time is None:
            recovery_time = datetime.strptime(df.iloc[-1]['timestamp'], "%Y/%m/%d %H:%M:%S.%f")

        duration = (recovery_time - alarm_time).total_seconds()
        
        if duration > 0:
            total_downtime += duration
            downtime_incidents.append({
                'Alarm Time': alarm_time.strftime("%H:%M:%S"),
                'Alarm Description': alarm_info['description'],
                'Recovery Time': recovery_time.strftime("%H:%M:%S"),
                'Downtime (sec)': round(duration, 2)
            })

    summary['total_downtime_sec'] = round(total_downtime, 2)
    summary['alarms_with_context'] = downtime_incidents
            
    return summary
