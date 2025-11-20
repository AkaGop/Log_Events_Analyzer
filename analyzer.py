# analyzer.py
import pandas as pd
from datetime import datetime
from config import ALARM_DB, CEID_MAP

def get_mapping_details(df: pd.DataFrame) -> dict:
    # ... (This function remains unchanged)
    details = { "start_time": "N/A", "end_time": "N/A", "duration_sec": 0.0 }
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
    # ... (This function remains unchanged)
    panel_info = { "panel_ids": [], "panel_slot_map": pd.DataFrame() }
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
    # ... (This function remains unchanged)
    # ...
    return details

# --- NEW FUNCTION TO MAP PANELS TO LOTS ---
def get_lot_to_panel_map(df: pd.DataFrame) -> dict:
    """Correlates IDRead events to the last preceding LOADSTART command."""
    start_events = df[df['EventName'] == 'LOADSTART'][['timestamp', 'details.LotID']].dropna().rename(columns={'details.LotID': 'LotID'})
    id_read_events = df[df['EventName'] == 'IDRead'][['timestamp', 'details.PanelID']].dropna().rename(columns={'details.PanelID': 'PanelID'})

    if start_events.empty or id_read_events.empty:
        return {}

    start_events['timestamp'] = pd.to_datetime(start_events['timestamp'])
    id_read_events['timestamp'] = pd.to_datetime(id_read_events['timestamp'])

    # Use merge_asof to find the last start event for each ID read
    merged_df = pd.merge_asof(
        id_read_events.sort_values('timestamp'),
        start_events.sort_values('timestamp'),
        on='timestamp',
        direction='backward'
    )

    # Group by LotID and aggregate the unique PanelIDs
    lot_panel_map = merged_df.groupby('LotID')['PanelID'].unique().apply(list).to_dict()
    
    return lot_panel_map
# --- END OF NEW FUNCTION ---

def perform_eda(df: pd.DataFrame) -> dict:
    # ... (This function remains unchanged)
    # ...
    return eda_results

def format_time(timestamp_str: str) -> str:
    # ... (This function remains unchanged)
    # ...
    return timestamp_str

def analyze_data(df: pd.DataFrame) -> dict:
    summary = {
        "job_status": "No Job Found", "lot_id": "N/A", "panel_count": 0,
        "total_downtime_sec": 0.0, "alarms_with_context": [],
        "magazine_ids": [], "operator_ids": [], "machine_statuses": [], "lot_ids": [],
        "mapping_details": {}, "panel_info": {}, "cycle_time_details": {},
        # Add new key to summary dictionary
        "lot_to_panel_map": {} 
    }
    
    if df.empty: return summary

    # ... (Context extraction code remains the same)
    # ...
        
    # --- MODIFIED: Call new function ---
    summary['mapping_details'] = get_mapping_details(df)
    summary['panel_info'] = get_panel_slot_map(df)
    summary['cycle_time_details'] = get_cycle_time_details(df)
    summary['lot_to_panel_map'] = get_lot_to_panel_map(df) # Call the new function
    
    # ... (Job and Downtime analysis code remains the same)
    # ...
            
    return summary
