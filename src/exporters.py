from .utils import hours_to_hhmm
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import uuid
import streamlit as st
from ics import Calendar

def generate_civilian_export(rotation_dict, base_tz, base_tz_name):
    base_tz_short_name = base_tz_name.split(' ')[0]
    
    try:
        flights = json.loads(rotation_dict['data'])
        if not flights:
            return "No flight data for this rotation."
            
        rot_id = rotation_dict['rotation_id']
        start_date = datetime.strptime(flights[0]['date'], '%Y-%m-%d')
        end_date = datetime.strptime(flights[-1]['arr_date'], '%Y-%m-%d')
        
        start_str = start_date.strftime('%b %d')
        end_str = end_date.strftime('%d')
        if end_date.month != start_date.month:
            end_str = end_date.strftime('%b %d')
        
        route_parts = [flights[0]['dep']]
        for f in flights:
            route_parts.append(f['arr'])
            
        route_str_list = []
        for apt in route_parts:
            if not route_str_list or route_str_list[-1] != apt:
                route_str_list.append(apt)
        route_str = "-".join(route_str_list)
        
        output_lines = []
        output_lines.append(f"{rot_id} | {start_str} - {end_str} | {route_str}")
        
        header_left = "DAY FLT T DEPARTS ARRIVES BLK"
        header_right = f"Time zone: {base_tz_short_name}"
        padding_len = 75 - len(header_left) - len(header_right)
        padding = " " * max(10, padding_len)
        output_lines.append(f"{header_left}{padding}{header_right}")
        
        for i, flight in enumerate(flights):
            day = datetime.strptime(flight['date'], '%Y-%m-%d').day
            flt_num = flight.get('flt', '???')
            
            dep_apt = flight['dep']
            arr_apt = flight['arr']
            dep_hhmm = flight['dep_time'].replace(':', '')
            arr_hhmm = flight['arr_time'].replace(':', '')
            
            block_float = flight.get('block', 0.0)
            block_h = int(block_float)
            block_m = int((block_float - block_h) * 60)
            block_str = f"{block_h}.{block_m:02d}"
            
            try:
                if dep_apt not in AIRPORTS_TZ or arr_apt not in AIRPORTS_TZ:
                    return f"Error: Unknown airport timezone for {dep_apt} or {arr_apt}."
                dep_tz = ZoneInfo(AIRPORTS_TZ[dep_apt])
                arr_tz = ZoneInfo(AIRPORTS_TZ[arr_apt])
            except KeyError as e:
                return f"Error: Unknown airport timezone for {e}."
                
            dep_local = datetime.strptime(flight['date'] + ' ' + flight['dep_time'], '%Y-%m-%d %H:%M').replace(tzinfo=dep_tz)
            arr_local = datetime.strptime(flight['arr_date'] + ' ' + flight['arr_time'], '%Y-%m-%d %H:%M').replace(tzinfo=arr_tz)
            
            dep_base_tz = dep_local.astimezone(base_tz)
            arr_base_tz = arr_local.astimezone(base_tz)
            
            time_str_base = f"{dep_base_tz.strftime('%a %d %I:%M%p')} - {arr_base_tz.strftime('%a %d %I:%M%p')}".lower()
            
            line_left = f" {day:>3} {flt_num:>5} {dep_apt:>4} {dep_hhmm:>5} {arr_apt:>4} {arr_hhmm:>5} {block_str:>6}"
            padding_len = 75 - len(line_left) - len(time_str_base)
            padding = " " * max(10, padding_len)
            line = f"{line_left}{padding}{time_str_base}"
            output_lines.append(line)
            
            hotel = flight.get('hotel')
            layover_duration = flight.get('layover_duration')
            
            if hotel or layover_duration:
                layover_line = f" {arr_apt} "
                if layover_duration:
                     layover_line += f"{layover_duration}/"
                if hotel:
                     layover_line += hotel
                output_lines.append(layover_line)
            elif i < len(flights) - 1:
                next_flight_date = datetime.strptime(flights[i+1]['date'], '%Y-%m-%d').date()
                this_arr_date = arr_local.date()
                if next_flight_date > this_arr_date:
                    output_lines.append(f" -- Overnight in {arr_apt} --")
                    
        return "\n".join(output_lines)
        
    except Exception as e:
        st.exception(e)
        return f"Error generating export: {e}"

def generate_ical_export(processed_duties, calendar_blackouts):
    utc_tz = ZoneInfo('UTC')
    cal_lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//SkedCheckApp//EN",
    ]
    
    def dt_to_ical(dt):
        return dt.astimezone(utc_tz).strftime('%Y%m%dT%H%M%SZ')
        
    for duty in processed_duties:
        uid = str(uuid.uuid4())
        start_utc = duty['report_utc']
        end_utc = duty['release_utc']
        
        if duty['type'] == 'flight':
            fdp_flights = duty.get('flights', [duty.get('flight')])
            if not fdp_flights: continue
            first_flight = fdp_flights[0]
            last_flight = fdp_flights[-1]
            
            route = f"{first_flight['dep']}-{last_flight['arr']}"
            if len(fdp_flights) > 1:
                route = f"{first_flight['dep']}...{last_flight['arr']}"
            
            summary = f"✈️ {duty['rotation_id']} ({route})"
            
            description_parts = [
                f"Rotation: {duty['rotation_id']}",
                f"FDP: {hours_to_hhmm(duty['duty_hours'])}",
                f"Block: {hours_to_hhmm(duty['block'])}",
                "--- Flights ---"
            ]
            for f in fdp_flights:
                flt_num = f.get('flt', f.get('flt_num', ''))
                description_parts.append(
                    f" {flt_num} {f['dep']} {f['dep_time']} - {f['arr']} {f['arr_time']}"
                )
            
            description = "\\n".join(description_parts)
            location = f"{first_flight['dep']} to {last_flight['arr']}"
        elif duty['type'] == 'training':
             summary = f" 🧑‍✈️ TRAINING: {duty['rotation_id']}"
             description = "Training Duty"
             location = "Training Center"
        elif duty['type'] == 'reserve':
            summary = f" RES: {duty['rotation_id']}"
            description = "Reserve Duty"
            location = "Base"
            
        cal_lines.extend([
            "BEGIN:VEVENT",
            f"UID:{uid}",
            f"DTSTAMP:{dt_to_ical(datetime.now())}",
            f"DTSTART:{dt_to_ical(start_utc)}",
            f"DTEND:{dt_to_ical(end_utc)}",
            f"SUMMARY:{summary}",
            f"DESCRIPTION:{description}",
            f"LOCATION:{location}",
            "END:VEVENT"
        ])
        
    for event in calendar_blackouts:
        if event['type'] == 'vacation':
            uid = str(uuid.uuid4())
            start_utc = event['start_utc']
            end_utc = event['end_utc']
            
            cal_lines.extend([
                "BEGIN:VEVENT",
                f"UID:{uid}",
                f"DTSTAMP:{dt_to_ical(datetime.now())}",
                f"DTSTART;VALUE=DATE:{start_utc.strftime('%Y%m%d')}",
                f"DTEND;VALUE=DATE:{(end_utc + timedelta(days=1)).strftime('%Y%m%d')}",
                "SUMMARY:🌴 VACATION",
                "DESCRIPTION:Vacation Days",
                "END:VEVENT"
            ])
    cal_lines.append("END:VCALENDAR")
    return "\n".join(cal_lines)

def generate_json_backup():
    profile_data = {
        "rotations": st.session_state.get('rotations', []),
        "blackouts": st.session_state.get('blackouts', [])
    }
    return json.dumps(profile_data, indent=2)