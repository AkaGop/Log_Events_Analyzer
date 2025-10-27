# analyzer.py
from datetime import datetime
import pandas as pd
from config import ALARM_CODE_MAP

def perform_eda(df: pd.DataFrame) -> dict:
    """A robust EDA function that defensively checks for the existence of columns."""
    # This function remains unchanged and is already correct.
    eda_results = {}
    if 'EventName' in df.columns:
        eda_results['event_counts'] = df['EventName'].value_counts()
    else:
        eda_results['event_counts'] = pd.Series(dtype='int64')

    if 'details.AlarmID' in df.columns and 'AlarmDescription' in df.columns:
        alarm_events = df[df['EventName'].isin(['Alarm Set', 'AlarmSet'])].copy()
        if not alarm_events.empty:
            eda_results['alarm_counts'] = alarm_events['AlarmDescription'].value_counts()
            display_cols = ['timestamp', 'EventName', 'details.AlarmID', 'AlarmDescription']
            eda_results['alarm_table'] = alarm_events[[c for c in display_cols if c in alarm_events.columns]]
        else:
            eda_results['alarm_counts'] = pd.Series(dtype='int64')
            eda_results['alarm_table'] = pd.DataFrame()
    else:
        eda_results['alarm_counts'] = pd.Series(dtype='int64')
        eda_results['alarm_table'] = pd.DataFrame()
    return eda_results

# --- START OF HIGHLIGHTED FIX ---
def analyze_data(df: pd.DataFrame) -> dict:
    """Analyzes a dataframe of parsed events to calculate high-level and contextual KPIs."""
    summary = {
        "job_status": "No Job Found", "lot_id": "N/A", "panel_count": 0,
        "total_duration_sec": 0.0, "unique_alarms_count": 0, "alarms_list": [],
        "magazine_id": "N/A", "operator_ids": [], "machine_status": "Unknown"
    }
    
    if df.empty: return summary

    # --- Find ALL Unique Operator IDs across the entire log ---
    if 'details.OperatorID' in df.columns:
        all_operators = df['details.OperatorID'].dropna().unique().tolist()
        summary['operator_ids'] = all_operators

    start_events = df[df['EventName'] == 'LOADSTART']
    if start_events.empty:
        summary['lot_id'] = "Test Lot / No Job"
        return summary
        
    first_start_event = start_events.iloc[0]
    summary['lot_id'] = first_start_event.get('details.LotID') if pd.notna(first_start_event.get('details.LotID')) else "N/A"
    summary['panel_count'] = int(first_start_event.get('details.PanelCount', 0))
    summary['job_start_time'] = first_start_event['timestamp']
    summary['job_status'] = "Started"

    df_after_start = df[df['timestamp'] >= summary['job_start_time']]
    end_events = df_after_start[df_after_start['EventName'] == 'MagToMagCompleted']

    end_timestamp_str = df.iloc[-1]['timestamp']
    if not end_events.empty:
        summary['job_status'] = "Completed"
        end_timestamp_str = end_events.iloc[0]['timestamp']
    else:
        summary['job_status'] = "Did not complete"

    try:
        t_start = datetime.strptime(summary['job_start_time'], "%Y/%m/%d %H:%M:%S.%f")
        t_end = datetime.strptime(end_timestamp_str, "%Y/%m/%d %H:%M:%S.%f")
        duration = (t_end - t_start).total_seconds()
        summary['total_duration_sec'] = round(duration, 2)

        job_df = df[(df['timestamp'] >= t_start.strftime("%Y/%m/%d %H:%M:%S.%f")) & 
                    (df['timestamp'] <= t_end.strftime("%Y/%m/%d %H:%M:%S.%f"))]
        
        # Find Magazine ID associated with the job
        mag_id_event = job_df[job_df['EventName'] == 'RequestMagazineDock']
        if not mag_id_event.empty:
            summary['magazine_id'] = mag_id_event.iloc[0]['details.MagazineID']

        # Find Machine Status during the job
        status_event = job_df[job_df['EventName'].isin(['Control State Local', 'Control State Remote'])]
        if not status_event.empty:
            summary['machine_status'] = status_event.iloc[-1]['EventName'].replace("Control State ", "")

        if 'details.AlarmID' in job_df.columns:
            alarm_events_in_job = job_df[job_df['EventName'] == 'Alarm Set'].copy()
            if not alarm_events_in_job.empty:
                alarm_events_in_job['AlarmDescription'] = pd.to_numeric(alarm_events_in_job['details.AlarmID'], errors='coerce').map(ALARM_CODE_MAP)
                summary['alarms_list'] = alarm_events_in_job['AlarmDescription'].dropna().unique().tolist()
                summary['unique_alarms_count'] = len(summary['alarms_list'])

    except (ValueError, TypeError):
        summary['job_status'] = "Time Calculation Error"
            
    return summary
# --- END OF HIGHLIGHTED FIX ---
