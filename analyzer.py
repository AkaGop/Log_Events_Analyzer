# analyzer.py
from datetime import datetime
import pandas as pd

def perform_eda(df: pd.DataFrame) -> dict:
    """
    A robust EDA function that defensively checks for the existence of columns.
    """
    eda_results = {}

    if 'EventName' in df.columns:
        eda_results['event_counts'] = df['EventName'].value_counts()
    else:
        eda_results['event_counts'] = pd.Series(dtype='int64')

    # --- START OF UPDATED ALARM ANALYSIS ---
    if 'details.AlarmID' in df.columns:
        alarm_events = df[df['details.AlarmID'].notna()].copy()
        if not alarm_events.empty:
            alarm_ids = pd.to_numeric(alarm_events['details.AlarmID'], errors='coerce').dropna()
            eda_results['alarm_counts'] = alarm_ids.value_counts()
            # Include the new 'AlarmDescription' column in the output table
            display_cols = ['timestamp', 'EventName', 'details.AlarmID', 'AlarmDescription']
            # Ensure all display columns exist before trying to select them
            eda_results['alarm_table'] = alarm_events[[c for c in display_cols if c in alarm_events.columns]]
        else:
            eda_results['alarm_counts'] = pd.Series(dtype='int64')
            eda_results['alarm_table'] = pd.DataFrame()
    else:
        eda_results['alarm_counts'] = pd.Series(dtype='int64')
        eda_results['alarm_table'] = pd.DataFrame()
    # --- END OF UPDATED ALARM ANALYSIS ---
        
    return eda_results

# The analyze_data function remains the same as the previous version, as its logic is sound.
def analyze_data(df: pd.DataFrame) -> dict:
    """Analyzes a dataframe of parsed events to calculate high-level KPIs."""
    summary = {
        "job_status": "No Job Found", "lot_id": "N/A", "panel_count": 0,
        "total_duration_sec": 0.0, "avg_cycle_time_sec": 0.0,
        "unique_alarms_count": 0, "alarms": []
    }
    
    if df.empty:
        return summary

    start_events = df[df['EventName'] == 'LOADSTART']
    if start_events.empty:
        summary['lot_id'] = "Test Lot / No Job"
        return summary
        
    first_start_event = start_events.iloc[0]
    summary['lot_id'] = first_start_event.get('details.LotID', 'N/A')
    try:
        summary['panel_count'] = int(first_start_event.get('details.PanelCount', 0))
    except (ValueError, TypeError):
        summary['panel_count'] = 0
    
    summary['job_start_time'] = first_start_event['timestamp']
    summary['job_status'] = "Started"

    df_after_start = df[df['timestamp'] >= summary['job_start_time']]
    end_events = df_after_start[df_after_start['EventName'].isin(['LoadToToolCompleted', 'UnloadFromToolCompleted'])]

    if not end_events.empty:
        first_end_event = end_events.iloc[0]
        summary['job_status'] = "Completed"
        try:
            t_start = datetime.strptime(summary['job_start_time'], "%Y/%m/%d %H:%M:%S.%f")
            t_end = datetime.strptime(first_end_event['timestamp'], "%Y/%m/%d %H:%M:%S.%f")
            duration = (t_end - t_start).total_seconds()

            if duration >= 0:
                summary['total_duration_sec'] = round(duration, 2)
                if summary['panel_count'] > 0:
                    summary['avg_cycle_time_sec'] = round(duration / summary['panel_count'], 2)

            job_df = df[(df['timestamp'] >= t_start.strftime("%Y/%m/%d %H:%M:%S.%f")) & 
                        (df['timestamp'] <= t_end.strftime("%Y/%m/%d %H:%M:%S.%f"))]
            
            if 'details.AlarmID' in job_df.columns:
                job_alarms = job_df[job_df['EventName'].isin(['Alarm Set', 'AlarmSet'])]['details.AlarmID'].dropna().unique()
                summary['alarms'] = list(job_alarms)
                summary['unique_alarms_count'] = len(job_alarms)

        except (ValueError, TypeError):
            summary['job_status'] = "Time Calculation Error"
    else:
        summary['job_status'] = "Did not complete"
            
    return summary
