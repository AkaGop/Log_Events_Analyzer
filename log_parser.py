# log_parser.py
import re
from io import StringIO
from config import CEID_MAP, RPTID_MAP

# --- START OF HIGHLIGHTED FIX ---
def _parse_s6f11_report(full_text: str) -> dict:
    """A robust parser that correctly extracts AlarmID from RPTID 101."""
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
        
        # This is the critical correction: If it's an alarm report (RPTID 101),
        # the AlarmID is the second value in the payload (after the clock).
        if rptid == 101 and len(data_payload) > 1:
            data['AlarmID'] = data_payload[1] # The true AlarmID

        data_payload_filtered = [val for val in data_payload if not (len(val) >= 14 and val.isdigit())]
        for i, name in enumerate(RPTID_MAP.get(rptid, [])):
            if i < len(data_payload_filtered): data[name] = data_payload_filtered[i]
            
    # Also assign AlarmID if the CEID itself is a direct alarm event
    elif data['CEID'] in [18, 113, 114]: # As defined in CEID_MAP
        data['AlarmID'] = data['CEID']

    return data
# --- END OF HIGHLIGHTED FIX ---

def _parse_s2f49_command(full_text: str) -> dict:
    # This function is correct and unchanged
    # ... (code from previous response)

def parse_log_file(uploaded_file):
    # This function is correct and unchanged
    # ... (code from previous response)
