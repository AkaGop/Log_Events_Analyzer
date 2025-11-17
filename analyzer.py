# analyzer.py
import pandas as pd
from datetime import datetime
from config import ALARM_DB, CEID_MAP

def get_mapping_details(df: pd.DataFrame) -> dict:
    details = {
        "start_time": "N/A",
        "end_time": "N/A",
        "duration_sec": 0.0
    }
    start_events = df[df['EventName'] == 'MagazineDocked'].sort_values('timestamp')
    end_events = df[df['EventName'] == 'MappingCompleted'].sort_values('timestamp')

    if not start_events.empty and not end_events.empty:
        start_time_str = start_events.iloc[0]['timestamp']
        end_event = end_events[end_events['timestamp'] > start_time_str]
        if not end_event.empty:
            end_time_str = end_event.iloc[0]['timestamp']
            
            t_start = datetime.strptime(start_time_str, "%Y/%m/%d %H:%M:%S.%f")
            t_end = datetime.strptime(end_time_str, "%Y/%m/%d %H:%M:%S.%f")
            
            details["start_time"] = t_start.strftime("%H:%M:%S")
            details["end_time"] = t_end.strftime("%H:%M:%S")
            details["duration_sec"] = (t_end - t_start).total_seconds()
            
    return details

def get_panel_slot_map(df: pd.DataFrame) -> dict:
    panel_info = {
        "panel_ids": [],
        "panel_slot_map": pd.DataFrame()
    }
    
    if 'details.PanelID' in df.columns and 'details.SlotID' in df.columns:
        id_read_events = df[df['EventName'] == 'IDRead'].copy()
        if not id_read_events.empty:
            slot_map_df = id_read_events[['details.PanelID', 'details.SlotID']].dropna().drop_duplicates()
            slot_map_df.rename(columns={'details.PanelID': 'Panel ID', 'details.SlotID': 'Slot'}, inplace=True)
            slot_map_df['Slot'] = pd.to_numeric(slot_map_df['Slot'])
            slot_map_df = slot_map_df.sort_values(by='Slot').reset_index(drop=True)
            
            panel_info["panel_ids"] = slot_map_df['Panel ID'].unique().tolist()
            panel_info["panel_slot_map"] = slot_map_df
            
    return panel_info

def get_cycle_time_details(df: pd.DataFrame) -> dict:
    details = {
        "total_processing_time_sec": 0.0,
        "cycle_times": pd.DataFrame()
    }
    
    start_events = df[df['EventName'] == 'LoadStarted']
    end_events = df[df['EventName'] == 'LoadToToolCompleted']
    
    if start_events.empty or end_events.empty:
        return details

    t_start_str = start_events.iloc[0]['timestamp']
    t_end_str = end_events.iloc[0]['timestamp']
    t_start = datetime.strptime(t_start_str, "%Y/%m/%d %H:%M:%S.%f")
    t_end = datetime.strptime(t_end_str, "%Y/%m/%d %H:%M:%S.%f")
    
    details["total_processing_time_sec"] = (t_end - t_start).total_seconds()
    
    loaded_events = df[df['EventName'] == 'LoadedToTool'].sort_values('timestamp').copy()
    if not loaded_events.empty:
        loaded_events['timestamp_dt'] = pd.to_datetime(loaded_events['timestamp'], format="%Y/%m/%d %H:%M:%S.%f")
        first_panel_time = loaded_events['timestamp_dt'].iloc[0]
        initial_cycle_duration = (first_panel_time - t_start).total_seconds()
        
        cycle_durations = loaded_events['timestamp_dt'].diff().dt.total_seconds().fillna(initial_cycle_duration)
        
        cycle_df = pd.DataFrame({
            'Panel Index': range(1, len(loaded_events) + 1),
            'Cycle Time (sec)': cycle_durations.values
        })
        details['cycle_times'] = cycle_df.set_index('Panel Index')
        
    return details

def perform_eda(df: pd.DataFrame) -> dict:
    # This function remains unchanged.
    # ...
    return eda_results

def format_time(timestamp_str: str) -> str:
    # This function remains unchanged.
    # ...
    return timestamp_str

def analyze_data(df: pd.DataFrame) -> dict:
    summary = {
        "job_status": "No Job Found", "lot_id": "N/A", "panel_count": 0,
        "total_downtime_sec": 0.0, "alarms_with_context": [],
        "magazine_ids": [], "operator_ids": [], "machine_statuses": [], "lot_ids": [],
        "mapping_details": {}, "panel_info": {}, "cycle_time_details": {}
    }
    
    if df.empty: return summary

    if 'details.OperatorID' in df.columns: summary['operator_ids'] = df['details.OperatorID'].dropna().unique().tolist()
    if 'details.MagazineID' in df.columns: summary['magazine_ids'] = df['details.MagazineID'].dropna().unique().tolist()
    if 'details.LotID' in df.columns: summary['lot_ids'] = df['details.LotID'].dropna().unique().tolist()
    if 'EventName' in df.columns:
        status_df = df[df['EventName'].isin(['Control State Local', 'Control State Remote'])]
        summary['machine_statuses'] = status_df['EventName'].str.replace("Control State ", "").unique().tolist()
        
    summary['mapping_details'] = get_mapping_details(df)
    summary['panel_info'] = get_panel_slot_map(df)
    summary['cycle_time_details'] = get_cycle_time_details(df)
    
    start_events = df[df['EventName'] == 'LOADSTART']
    if not start_events.empty:
        first_start_event = start_events.iloc[0]
        summary['lot_id'] = first_start_event.get('details.LotID', "N/A")
        summary['panel_count'] = len(summary['panel_info']['panel_ids'])
        
        end_events = df[df['EventName'] == 'LoadToToolCompleted']
        summary['job_status'] = "Completed" if not end_events.empty else "Did not complete"
            
    # CORRECTED: Downtime calculation is now independent of finding a specific job.
    # This makes the tool more robust for logs that might not contain a full job.
    downtime_incidents = []
    total_downtime = 0.0
    stoppable_alarm_codes = {k for k, v in ALARM_DB.items() if v['level'] in ['Error', 'Alarm']}
    
    if 'details.AlarmID' in df.columns:
        numeric_alarm_ids = pd.to_numeric(df['details.AlarmID'], errors='coerce')
        fault_events = df[numeric_alarm_ids.isin(stoppable_alarm_codes)].copy()
        
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
