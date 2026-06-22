import re
from datetime import datetime, timedelta
import calendar

def find_effective_date(dump_text):
    match = re.search(r'EFFECTIVE\s+([A-Z]{3})(\d{1,2})', dump_text, re.IGNORECASE)
    if not match:
        return None
    month_str = match.group(1).upper()
    day = int(match.group(2))
    month_map = {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                 'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}
    month = month_map.get(month_str)
    if not month:
        return None
    today = datetime.today().date()
    current_year = today.year
    try:
        potential_date = datetime(current_year, month, day).date()
        if (potential_date - today).days > 180:
            return potential_date.replace(year=current_year - 1)
        elif (today - potential_date).days > 180:
            return potential_date.replace(year=current_year + 1)
        else:
            return potential_date
    except ValueError:
        return None

def parse_hhmm_time(time_str):
    if not isinstance(time_str, str):
        return None
    cleaned_str = time_str.replace(":", "").replace(".", "").strip()
    if not cleaned_str:
       return None
    padded_str = cleaned_str.zfill(4)
    if len(padded_str) == 4 and padded_str.isdigit():
        try:
            hh = int(padded_str[0:2])
            mm = int(padded_str[2:4])
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return datetime.strptime(f"{hh:02d}:{mm:02d}", "%H:%M").time()
            else:
                return None
        except ValueError:
            return None
    return None

def parse_time_str_to_float(time_str):
    if not time_str:
        return 0.0
    time_str = time_str.strip()
    if '.' in time_str:
        try:
            parts = time_str.split('.')
            h = int(parts[0])
            m_str = parts[1].ljust(2, '0')
            m = int(m_str[:2])
            return h + m / 60.0
        except:
             pass
    if ':' in time_str:
        try:
            parts = time_str.split(':')
            h = int(parts[0])
            m = int(parts[1])
            return h + m / 60.0
        except:
            pass
    cleaned_str = time_str.replace(':', '').replace('.', '')
    if cleaned_str.isdigit() and len(cleaned_str) <= 4:
        padded_str = cleaned_str.zfill(4)
        h = int(padded_str[:2])
        m = int(padded_str[2:])
        return h + m / 60.0
    try:
        return float(time_str)
    except:
        return 0.0

def get_date_for_day(start_date, day):
    if day < start_date.day:
        next_month_start = (start_date.replace(day=28) + timedelta(days=4)).replace(day=1)
        try:
            return next_month_start.replace(day=day)
        except ValueError:
            last_day_of_month = calendar.monthrange(next_month_start.year, next_month_start.month)[1]
            return next_month_start.replace(day=last_day_of_month)
    else:
        try:
            return start_date.replace(day=day)
        except ValueError:
            last_day_of_month = calendar.monthrange(start_date.year, start_date.month)[1]
            return start_date.replace(day=last_day_of_month)

def parse_trip_dump(dump_text, effective_date):
    start_date = effective_date
    flights = []
    report_times = {}
    header_report = None
    
    lines = [line.strip() for line in dump_text.split('\n') if line.strip()]
    
    for line in lines:
        if header_report is None and 'ACTUAL REPORT TIME' in line:
            match = re.search(r'ACTUAL REPORT TIME (\d{4})', line)
            if match:
                hhmm = match.group(1)
                h, m = divmod(int(hhmm), 100)
                header_report = f"{h:02d}:{m:02d}"
        
        match_report = re.search(r'PAY REPORT TIME (\d{4})/(\d+)', line)
        if match_report:
            hhmm = match_report.group(1)
            target_day = int(match_report.group(2))
            h, m = divmod(int(hhmm), 100)
            report_time = f"{h:02d}:{m:02d}"
            report_times[target_day] = report_time
    
    if header_report and start_date.day not in report_times:
        report_times[start_date.day] = header_report
        
    previous_date = start_date
    
    day_flight_re = re.compile(
        r'^(\d{1,2})\s+([A-Z]{1,2})?\s*(\d+)\s+([A-Z]{3})\s*\S*\s*(\d{2}:?\d{2})\s+([A-Z]{3})(?:.\s*)?(\d{2}:?\d{2})\s+([\d.:]+)'
    )
    sub_flight_re = re.compile(
        r'^(\d+)\s+([A-Z]{3})\s*\S*\s*(\d{2}:?\d{2})\s+([A-Z]{3})(?:.\s*)?(\d{2}:?\d{2})\s+([\d.:]+)'
    )
    current_day = None
    current_date = None
    i = 0
    while i < len(lines):
        line = lines[i]
        match_day_flight = day_flight_re.match(line)
        match_sub_flight = sub_flight_re.match(line)
        
        flight_data = None
        
        if match_day_flight:
            current_day = int(match_day_flight.group(1))
            
            if current_date is None:
                current_date = get_date_for_day(previous_date, current_day)
            else:
                current_date = get_date_for_day(current_date, current_day)
    
            previous_date = current_date
            
            flight_data = {
                'day': current_day,
                'flt': match_day_flight.group(3),
                'dep_apt': match_day_flight.group(4),
                'dep_time': match_day_flight.group(5).replace(':', ''),
                'arr_apt': match_day_flight.group(6),
                'arr_time': match_day_flight.group(7).replace(':', ''),
                'block_str': match_day_flight.group(8),
                'indicator': match_day_flight.group(2)
            }
        
        elif match_sub_flight and current_day is not None:
            flight_data = {
                'day': current_day,
                'flt': match_sub_flight.group(1),
                'dep_apt': match_sub_flight.group(2),
                'dep_time': match_sub_flight.group(3).replace(':', ''),
                'arr_apt': match_sub_flight.group(4),
                'arr_time': match_sub_flight.group(5).replace(':', ''),
                'block_str': match_sub_flight.group(6),
                'indicator': None
            }
        
        if flight_data:
            day = flight_data['day']
            date_str = current_date.strftime('%Y-%m-%d')
            
            dep_h, dep_m = divmod(int(flight_data['dep_time']), 100)
            arr_h, arr_m = divmod(int(flight_data['arr_time']), 100)
            local_dep = f"{dep_h:02d}:{dep_m:02d}"
            local_arr = f"{arr_h:02d}:{arr_m:02d}"
            dep_minutes = dep_h * 60 + dep_m
            arr_minutes = arr_h * 60 + arr_m
            arr_date = current_date
            
            if arr_minutes < dep_minutes:
                arr_date += timedelta(days=1)
            arr_date_str = arr_date.strftime('%Y-%m-%d')
            
            block = parse_time_str_to_float(flight_data['block_str'])
            if flight_data.get('indicator') in ['D', 'DD']:
                block = 0.0

            
            turn = 0.5
            
            report_local = report_times.get(day, None)
            if not report_local:
                report_local = header_report if day == start_date.day else 'MANUAL'
            report_date_str = date_str
            
            layover_duration = None
            hotel = None
            if i + 1 < len(lines):
                next_line = lines[i+1]
                layover_match = re.match(r'^\s*(\d+)?\s*([A-Z]{3})\s+([\d.]+)\/(.*)', next_line)
                if layover_match:
                    layover_apt_match = layover_match.group(2)
                    if layover_apt_match == flight_data['arr_apt']:
                        layover_duration = layover_match.group(3)
                        hotel = layover_match.group(4).strip()
                        i += 1
                        turn = 0.5
                        
            flights.append({
                'date': date_str,
                'dep': flight_data['dep_apt'],
                'dep_time': local_dep,
                'arr': flight_data['arr_apt'],
                'arr_time': local_arr,
                'arr_date': arr_date_str,
                'report_time': report_local,
                'report_date': report_date_str,
                'block': block,
                'turn': turn,
                'flt': flight_data['flt'],
                'layover_duration': layover_duration,
                'hotel': hotel
            })
        
        i += 1
        
    if flights:
        flights[-1]['turn'] = 0.5
        
    return flights