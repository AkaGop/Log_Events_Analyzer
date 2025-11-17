# analyzer.py
import pandas as pd
from datetime import datetime
from config import ALARM_DB, CEID_MAP

def get_mapping_details(df: pd.DataFrame) -> dict:
    # ... (this function remains the same)
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
    # ... (this function remains the same)
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

# NEW FUNCTION: To calculate cycle times
def get_cycle_time_details(df: pd.DataFrame) -> dict:
    """Calculates total processing time and per-panel cycle times."""
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
    
    # Calculate cycle time for each panel
    loaded_events = df[df['EventName'] == 'LoadedToTool'].sort_values('timestamp').copy()
    if not loaded_events.empty:
        loaded_events['timestamp_dt'] = pd.to_datetime(loaded_events['timestamp'], format="%Y/%m/%d %H:%M:%S.%f")
        # The first cycle time is from LoadStarted to the first panel loaded
        first_panel_time = loaded_events['timestamp_dt'].iloc[0]
        initial_cycle_duration = (first_panel_time - t_start).total_seconds()
        
        # Subsequent cycle times are the difference between consecutive panel loads
        cycle_durations = loaded_events['timestamp_dt'].diff().dt.total_seconds().fillna(initial_cycle_duration)
        
        cycle_df = pd.DataFrame({
            'Panel Index': range(1, len(loaded_events) + 1),
            'Cycle Time (sec)': cycle_durations.values
        })
        details['cycle_times'] = cycle_df.set_index('Panel Index')
        
    return details

# (perform_eda and format_time functions remain the same)
# ...

# MODIFIED: analyze_data function is now more modular
def analyze_data(df: pd.DataFrame) -> dict:
    summary = {
        "job_status": "No Job Found", "lot_id": "N/A", "panel_count": 0,
        "total_downtime_sec": 0.0, "alarms_with_context": [],
        "magazine_ids": [], "operator_ids": [], "machine_statuses": [], "lot_ids": [],
        "mapping_details": {}, "panel_info": {}, "cycle_time_details": {}
    }
    
    if df.empty: return summary

    # --- Context Extraction ---
    if 'details.OperatorID' in df.columns: summary['operator_ids'] = df['details.OperatorID'].dropna().unique().tolist()
    if 'details.MagazineID' in df.columns: summary['magazine_ids'] = df['details.MagazineID'].dropna().unique().tolist()
    if 'details.LotID' in df.columns: summary['lot_ids'] = df['details.LotID'].dropna().unique().tolist()
    if 'EventName' in df.columns:
        status_df = df[df['EventName'].isin(['Control State Local', 'Control State Remote'])]
        summary['machine_statuses'] = status_df['EventName'].str.replace("Control State ", "").unique().tolist()
        
    # --- New Modular Analysis ---
    summary['mapping_details'] = get_mapping_details(df)
    summary['panel_info'] = get_panel_slot_map(df)
    summary['cycle_time_details'] = get_cycle_time_details(df) # New
    
    # --- Job and Downtime Analysis ---
    start_events = df[df['EventName'] == 'LOADSTART']
    if not start_events.empty:
        first_start_event = start_events.iloc[0]
        summary['lot_id'] = first_start_event.get('details.LotID', "N/A")
        summary['panel_count'] = len(summary['panel_info']['panel_ids'])
        
        end_events = df[df['EventName'] == 'LoadToToolCompleted']
        summary['job_status'] = "Completed" if not end_events.empty else "Did not complete"

    # (Downtime calculation logic remains the same)
    # ...
            
    return summary
