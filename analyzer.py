# analyzer.py
from datetime import datetime
import pandas as pd
from config import ALARM_DB

def perform_eda(df: pd.DataFrame) -> dict:
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

def format_time(timestamp_str: str) -> str:
    try:
        return datetime.strptime(timestamp_str, "%Y/%m/%d %H:%M:%S.%f").strftime("%H:%M:%S")
    except ValueError:
        return timestamp_str

def analyze_data(df: pd.DataFrame) -> dict:
    summary = {
        "job_status": "No Job Found", "lot_id": "N/A", "panel_count": 0,
        "total_downtime_sec": 0.0, "alarms_with_context": [],
        "magazine_ids": [], "operator_ids": [], "machine_statuses": [], "lot_ids": [],
        "login_events": [], "dock_events": [], "status_events": []
    }
    
    if df.empty: return summary

    if 'details.OperatorID' in df.columns:
        summary['operator_ids'] = df['details.OperatorID'].dropna().unique().tolist()
        for _, row in df[df['EventName'] == 'RequestOperatorLogin'].dropna(subset=['details.OperatorID']).iterrows():
            summary['login_events'].append({'Time': format_time(row['timestamp']), 'Operator ID': row['details.OperatorID']})

    if 'details.MagazineID' in df.columns:
        summary['magazine_ids'] = df['details.MagazineID'].dropna().unique().tolist()
        for _, row in df[df['EventName'] == 'MagazineDocked'].dropna(subset=['details.MagazineID', 'details.PortID']).iterrows():
            summary['dock_events'].append({'Time': format_time(row['timestamp']), 'Magazine ID': row['details.MagazineID'], 'Port ID': row['details.PortID']})

    if 'details.LotID' in df.columns:
        summary['lot_ids'] = df['details.LotID'].dropna().unique().tolist()

    if 'EventName' in df.columns:
        status_df = df[df['EventName'].isin(['Control State Local', 'Control State Remote'])]
        summary['machine_statuses'] = status_df['EventName'].str.replace("Control State ", "").unique().tolist()
        for _, row in status_df.iterrows():
            summary['status_events'].append({'Time': format_time(row['timestamp']), 'Status': row['EventName'].replace("Control State ", "")})

    analysis_scope_df = df
    
    start_events = df[df['EventName'] == 'LOADSTART']
    if start_events.empty:
        summary['lot_id'] = "Dummy Lot or NA"
    else:
        first_start_event = start_events.iloc[0]
        summary['lot_id'] = first_start_event.get('details.LotID', "N/A")
        summary['panel_count'] = int(first_start_event.get('details.PanelCount', 0))
        
        df_after_start = df[df['timestamp'] >= first_start_event['timestamp']]
        end_events = df_after_start[df_after_start['EventName'] == 'MagToMagCompleted']
        
        end_timestamp_str = df.iloc[-1]['timestamp']
        summary['job_status'] = "Did not complete"
        if not end_events.empty:
            summary['job_status'] = "Completed"
            end_timestamp_str = end_events.iloc[0]['timestamp']

        try:
            t_start_str = first_start_event['timestamp']
            t_start = datetime.strptime(t_start_str, "%Y/%m/%d %H:%M:%S.%f")
            t_end = datetime.strptime(end_timestamp_str, "%Y/%m/%d %H:%M:%S.%f")
            analysis_scope_df = df[(df['timestamp'] >= t_start_str) & (df['timestamp'] <= end_timestamp_str)]
        except (ValueError, TypeError):
            summary['job_status'] = "Time Calculation Error"
            analysis_scope_df = df

    downtime_incidents = []
    total_downtime = 0.0

    stoppable_alarm_codes = {k for k, v in ALARM_DB.items() if v['level'] in ['Error', 'Alarm']}
    numeric_alarm_ids = pd.to_numeric(analysis_scope_df['details.AlarmID'], errors='coerce')
    fault_events = analysis_scope_df[numeric_alarm_ids.isin(stoppable_alarm_codes)].copy()
    
    full_log_list = df.to_dict('records')

    for index, alarm_row in fault_events.iterrows():
        alarm_time = datetime.strptime(alarm_row['timestamp'], "%Y/%m/%d %H:%M:%S.%f")
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
