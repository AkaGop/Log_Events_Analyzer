# log_parser.py
import re
from io import StringIO
from config import CEID_MAP, RPTID_MAP

def _parse_s6f11_report(full_text: str) -> dict:
    data = {}
    tokens = re.findall(r"<(?:A|U\d|B)\s\[\d+\]\s(?:'([^']*)'|(\d+))>", full_text)
    flat_values = [s if s else i for s, i in tokens]

    if len(flat_values) < 2: return {}
    try:
        data['DATAID'], data['CEID'] = int(flat_values[0]), int(flat_values[1])
    except (ValueError, IndexError): return {}

    payload = flat_values[2:]
    rptid, rptid_index = None, -1
    for i, val in enumerate(payload):
        if val.isdigit():
            rptid, rptid_index = int(val), i
            break
            
    if rptid in RPTID_MAP:
        data['RPTID'] = rptid
        data_payload = payload[rptid_index + 1:]
        
        if rptid == 101 and len(data_payload) > 1:
            data['AlarmID'] = data_payload[1]

        data_payload_filtered = [val for val in data_payload if not (len(val) >= 14 and val.isdigit())]
        for i, name in enumerate(RPTID_MAP.get(rptid, [])):
            if i < len(data_payload_filtered): data[name] = data_payload_filtered[i]
            
    elif data['CEID'] in [18, 113, 114]:
        data['AlarmID'] = data['CEID']

    return data

def _parse_s2f4p_command(full_text: str) -> dict:
    data = {}
    rcmd_match = re.search(r"<\s*A\s*\[\d+\]\s*'([A-Z_]{5,})'", full_text)
    if rcmd_match: data['RCMD'] = rcmd_match.group(1)
    
    tokens = re.findall(r"'([^']*)'", full_text)
    try:
        if 'LOTID' in tokens:
            lotid_index = tokens.index('LOTID')
            if lotid_index + 1 < len(tokens): data['LotID'] = tokens[lotid_index + 1]
    except (ValueError, IndexError): pass

    panels_match = re.search(r"'LOTPANELS'\s*>\s*<L\s\[(\d+)\]", full_text, re.IGNORECASE)
    if panels_match: data['PanelCount'] = int(panels_match.group(1))
    return data

def parse_log_file(uploaded_file):
    events = []
    if not uploaded_file: return events
    try: lines = StringIO(uploaded_file.getvalue().decode("utf-8")).readlines()
    except: lines = StringIO(uploaded_file.getvalue().decode("latin-1", errors='ignore')).readlines()
    
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line: i+= 1; continue
        header_match = re.match(r"(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+),\[([^\]]+)\],(.*)", line)
        if not header_match: i += 1; continue
        timestamp, log_type, message_part = header_match.groups()
        msg_match = re.search(r"MessageName=(\w+)|Message=.*?:\'(\w+)\'", message_part)
        msg_name = (msg_match.group(1) or msg_match.group(2)) if msg_match else "N/A"
        event = {"timestamp": timestamp, "msg_name": msg_name}
        if ("Core:Send" in log_type or "Core:Receive" in log_type) and i + 1 < len(lines) and lines[i+1].strip().startswith('<'):
            j = i + 1; block_lines = []
            while j < len(lines) and lines[j].strip() != '.':
                block_lines.append(lines[j]); j += 1
            i = j
            if block_lines:
                full_text = "".join(block_lines)
                details = {}
                if msg_name == 'S6F11': details = _parse_s6f11_report(full_text)
                elif msg_name == 'S2F49': details = _parse_s2f49_command(full_text)
                if details: event['details'] = details
        if 'details' in event and event['details']:
            events.append(event)
        i += 1
    return events
