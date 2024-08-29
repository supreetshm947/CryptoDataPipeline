from datetime import datetime

def convert_iso_to_datetime(iso_date_str):
    # Replace 'Z' with '+00:00' to make it compatible with datetime.fromisoformat()
    iso_format_string = iso_date_str.replace('Z', '+00:00')
    return datetime.fromisoformat(iso_format_string)