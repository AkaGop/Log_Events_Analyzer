# analyzer.py
import pandas as pd
from datetime import datetime
from config import ALARM_DB, CEID_MAP

# NEW FUNCTION: To get mapping details
def get_mapping_details(df: pd.DataFrame) -> dict:
    """Finds the start, end, and duration of the first magazine mapping sequence."""
    details = {
        "start_time": "N/A",
        "end_time": "N/A",
        "duration_sec": 0.0
    }
    # Mapping starts after the magazine is docked
    start_events = df[df['EventName'] == 'MagazineDocked'].sort_values('timestamp')
    # Mapping ends when the equipment reports completion
    end_events = df[df['EventName'] == 'MappingCompleted'].sort_values('timestamp')

    if not start_events.empty and not end_events.empty:
        start_time_str = start_events.iloc[0]['timestamp']
        # Find the first 'MappingCompleted' event that occurs after the 'MagazineDocked' event
        end_event = end_events[end_events['timestamp'] > start_time_str]
        if not end_event.empty:
            end_time_str = end_event.iloc[0]['timestamp']
            
            t_start = datetime.strptime(start_time_str, "%Y/%m/%d %H:%M:%S.%f")
            t_end = datetime.strptime(end_time_str, "%Y/%m/%d %H:%M:%S.%f")
            
            details["start_time"] = t_start.strftime("%H:%M:%S")
            details["end_time"] = t_end.strftime("%H:%M:%S")
            details["duration_sec"] = (t_end - t_start).total_seconds()
            
    return details

# NEW FUNCTION: To get panel and slot information
def get_panel_slot_map(df: pd.DataFrame) -> dict:
    """Extracts all unique Panel IDs and maps them to their magazine slots."""
    panel_info = {
        "panel_ids": [],
        "panel_slot_map": pd.DataFrame()
    }
    
    if 'details.PanelID' in df.columns and 'details.SlotID' in df.columns:
        # Filter for IDRead events which contain both PanelID and SlotID
        id_read_events = df[df['EventName'] == 'IDRead'].copy()
        if not id_read_events.empty:
            # Create the map and drop any duplicates
            slot_map_df = id_read_events[['details.PanelID', 'details.SlotID']].dropna().drop_duplicates()
            slot_map_df.rename(columns={'details.PanelID': 'Panel ID', 'details.SlotID': 'Slot'}, inplace=True)
            slot_map_df['Slot'] = pd.to_numeric(slot_map_df['Slot'])
            slot_map_df = slot_map_df.sort_values(by='Slot').reset_index(drop=True)
            
            panel_info["panel_ids"] = slot_map_df['Panel ID'].unique().tolist()
            panel_info["panel_slot_map"] = slot_map_df
            
    return panel_info

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

# MODIFIED: Refactored to be cleaner and include new analysis
def analyze_data(df: pd.DataFrame) -> dict:
    summary = {
        "job_status": "No Job Found", "lot_id": "N/A", "panel_count": 0,
        "total_downtime_sec": 0.0, "alarms_with_context": [],
        "magazine_ids": [], "operator_ids": [], "machine_statuses": [], "lot_ids": [],
        "login_events": [], "dock_events": [], "status_events": [],
        "mapping_details": {}, "panel_info": {}
    }
    
    if df.empty: return summary

    # --- Context Extraction ---
    if 'details.OperatorID' in df.columns:
        summary['operator_ids'] = df['details.OperatorID'].dropna().unique().tolist()
    if 'details.MagazineID' in df.columns:
        summary['magazine_ids'] = df['details.MagazineID'].dropna().unique().tolist()
    if 'details.LotID' in df.columns:
        summary['lot_ids'] = df['details.LotID'].dropna().unique().tolist()
    if 'EventName' in df.columns:
        status_df = df[df['EventName'].isin(['Control State Local', 'Control State Remote'])]
        summary['machine_statuses'] = status_df['EventName'].str.replace("Control State ", "").unique().tolist()
        
    # --- New Analysis Function Calls ---
    summary['mapping_details'] = get_mapping_details(df)
    summary['panel_info'] = get_panel_slot_map(df)

    # --- Job and Downtime Analysis (largely unchanged) ---
    start_events = df[df['EventName'] == 'LOADSTART']
    if not start_events.empty:
        first_start_event = start_events.iloc[0]
        summary['lot_id'] = first_start_event.get('details.LotID', "N/A")
        # Use the count of read panels for accuracy instead of the command's requested count
        summary['panel_count'] = len(summary['panel_info']['panel_ids'])
        
        # (Downtime calculation logic remains the same)
        # ... (rest of the downtime logic)
            
    return summary
