# log_parser.py
import re
from io import StringIO
from config import CEID_MAP, RPTID_MAP

def _parse_s6f11_report(full_text: str) -> dict:
    """Final, robust, token-based parser for S6F11 reports."""
    data = {}
    # This regex is quite effective at tokenizing the SECS-II text format.
    tokens = re.findall(r"<(?:A|U\d|B)\s\[\d+\]\s(?:'([^']*)'|(\d+))>", full_text)
    flat_values = [s if s else i for s, i in tokens]

    # An S6F11 report needs at least a DATAID, CEID, and RPTID list to be valid
    if len(flat_values) < 3: return {}

    try:
        # According to the log and spec, the first two U4/U2 values are DATAID and CEID
        dataid = int(flat_values[0])
        ceid = int(flat_values[1])
        data['DATAID'] = dataid
        data['CEID'] = ceid
    except (ValueError, IndexError): 
        return {} # Not a valid S6F11 if these can't be parsed

    if "Alarm" in CEID_MAP.get(ceid, ''):
        # If the event is an alarm, the AlarmID is often the CEID itself or in the payload
        data['AlarmID'] = ceid

    # The payload containing the RPTID and its data follows the CEID
    payload = flat_values[2:]
    
    # Find the RPTID, which should be the first integer in the payload
    rptid = None
    rptid_index = -1
    for i, val in enumerate(payload):
        if val.isdigit():
            rptid = int(val)
            rptid_index = i
            break
            
    if rptid in RPTID_MAP:
        data['RPTID'] = rptid
        # The actual data for the report follows the RPTID
        data_payload = payload[rptid_index + 1:]
        
        # A simple but effective filter for timestamps to prevent misalignment of data fields
        data_payload_filtered = [val for val in data_payload if not (len(val) >= 14 and val.isdigit())]

        field_names = RPTID_MAP.get(rptid, [])
        for i, name in enumerate(field_names):
            if i < len(data_payload_filtered):
                data[name] = data_payload_filtered[i]
            
    return data

def _parse_s2f49_command(full_text: str) -> dict:
    """Parses S2F49 Remote Commands."""
    data = {}
    rcmd_match = re.search(r"<\s*A\s*\[\d+\]\s*'([A-Z_]{5,})'", full_text)
    if rcmd_match: data['RCMD'] = rcmd_match.group(1)
    
    lotid_match = re.search(r"'LOTID'\s*>\s*<A\[\d+\]\s*'([^']*)'", full_text, re.IGNORECASE)
    if lotid_match: data['LotID'] = lotid_match.group(1)
    
    # Correctly parsing panel count from the <L [n]> structure
    panels_match = re.search(r"'LOTPANELS'\s*>\s*<L\s\[(\d+)\]", full_text, re.IGNORECASE)
    if panels_match: data['PanelCount'] = int(panels_match.group(1))

    return data

def parse_log_file(uploaded_file):
    """Main function to parse the uploaded log file line by line."""
    events = []
    if not uploaded_file: return events
    
    try: 
        lines = StringIO(uploaded_file.getvalue().decode("utf-8")).readlines()
    except UnicodeDecodeError: 
        lines = StringIO(uploaded_file.getvalue().decode("latin-1", errors='ignore')).readlines()
    
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line: 
            i += 1
            continue
            
        header_match = re.match(r"(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+),\[([^\]]+)\],(.*)", line)
        if not header_match: 
            i += 1
            continue
            
        timestamp, log_type, message_part = header_match.groups()
        
        msg_match = re.search(r"MessageName=(\w+)|Message=.*?:\'(\w+)\'", message_part)
        msg_name = (msg_match.group(1) or msg_match.group(2)) if msg_match else "N/A"
        
        event = {"timestamp": timestamp, "msg_name": msg_name}
        
        # Check if the line indicates a multi-line message block follows
        if ("Core:Send" in log_type or "Core:Receive" in log_type) and i + 1 < len(lines) and lines[i+1].strip().startswith('<'):
            j = i + 1
            block_lines = []
            while j < len(lines) and lines[j].strip() != '.':
                block_lines.append(lines[j])
                j += 1
            
            i = j # Move the main loop index past this block
            
            if block_lines:
                full_text = "".join(block_lines)
                details = {}
                if msg_name == 'S6F11': 
                    details = _parse_s6f11_report(full_text)
                elif msg_name == 'S2F49': 
                    details = _parse_s2f49_command(full_text)
                
                if details: 
                    event['details'] = details
        
        # Only add events that have successfully parsed details
        if 'details' in event and event['details']:
            events.append(event)
            
        i += 1
        
    return events
