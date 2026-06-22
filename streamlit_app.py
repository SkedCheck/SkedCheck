import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
import re
import calendar
import pytz
import os
import json
import uuid
from ics import Calendar
import streamlit.components.v1 as components

from src.database import *
from zoneinfo import ZoneInfo
from src.parsers import *
from src.utils import *
from src.calculator import *
from src.exporters import *
from src.ui.calendar_view import render_calendar_and_details

st.set_page_config(layout="wide", page_title="SkedCheck Schedule Viewer", page_icon="logo.png")

st.markdown("""
<style>
    .main-header {font-size: 3rem; color: #1f77b4; text-align: center; margin-bottom: 0rem;}
    .metric-header {font-size: 1.2rem; color: #ff7f0e;}
    .calendar-table {border-collapse: collapse; width: 100%; table-layout: fixed;}
    .calendar-cell {border: 1px solid #ddd; padding: 8px; text-align: center; min-height: 80px; background-color: #f9f9f9; vertical-align: top;}
    .calendar-cell:hover {background-color: #e6f3ff;}
    .rotation-id {font-weight: bold; color: #003268;}
    .vac-label {font-weight: bold; color: #003268;}
    .trng-label {font-weight: bold; color: #003268;}
    .res-label {font-weight: bold; color: #003268;}
    .blank-cell {background-color: #ffffff;}
    .block-hours {font-size: 0.8rem; color: #333;}
    .empty-cell {background-color: #fff; border: 1px solid #ddd; padding: 8px;}
    .conflict {background-color: #ffcccc; color: #cc0000;}
    .compliant {background-color: #e3f2fd; color: #01579b;} /* <-- 'Clear Skies' Blue */
    .warning {background-color: #ffffcc; color: #cc6600;}
    
    /* --- Make date clickable --- */
    .day-link {
        font-size: 1.1em;
        font-weight: bold;
        color: #1f77b4;
        text-decoration: none;
        cursor: pointer; 
    }
    .day-link:hover {
        text-decoration: underline;
    }

    /* --- Navigation Button Styling --- */
    div[data-testid="stHorizontalBlock"] > div[data-testid="column"] {
        text-align: center;
        align-items: center;
    }
    
    @media (max-width: 600px) {
        .calendar-table {font-size: 0.8rem;}
        .calendar-cell {min-height: 60px; padding: 4px;}
    }
</style>
""", unsafe_allow_html=True)

DB_FILE = "SkedCheck.db"

def save_setting(key, value):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    try:
        c.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', (key, value))
        conn.commit()
    except Exception as e:
        st.error(f"Error saving setting: {e}")
    finally:
        conn.close()

def load_setting(key, default=None):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    try:
        c.execute('SELECT value FROM settings WHERE key = ?', (key,))
        res = c.fetchone()
        if res:
            return res[0]
        else:
            return default
    except Exception as e:
        st.error(f"Error loading setting: {e}")
        return default
    finally:
        conn.close()

def load_profiles():
    conn = sqlite3.connect(DB_FILE)
    profiles = pd.read_sql_query("SELECT * FROM profiles ORDER BY name", conn)
    conn.close()
    return profiles.to_dict('records')

def create_profile(name, source_profile_id=None):
    conn = sqlite3.connect(DB_FILE)
    conn.execute('PRAGMA foreign_keys = ON;')
    c = conn.cursor()
    try:
        c.execute('INSERT INTO profiles (name) VALUES (?)', (name,))
        new_profile_id = c.lastrowid
        if source_profile_id:
            c.execute('''
                INSERT INTO rotations (profile_id, rotation_id, start_date, data, is_cancelled)
                SELECT ?, rotation_id, start_date, data, is_cancelled
                FROM rotations
                WHERE profile_id = ?
            ''', (new_profile_id, source_profile_id))
            c.execute('''
                INSERT INTO blackouts (profile_id, type, start_datetime_utc, end_datetime_utc, block_id)
                SELECT ?, type, start_datetime_utc, end_datetime_utc, block_id
                FROM blackouts
                WHERE profile_id = ?
            ''', (new_profile_id, source_profile_id))
        conn.commit()
        st.success(f"Profile '{name}' created!")
        return new_profile_id
    except sqlite3.IntegrityError:
        st.error(f"Profile name '{name}' already exists.")
        return None
    except Exception as e:
        st.error(f"Error creating profile: {e}")
        return None
    finally:
        conn.close()

def delete_profile(profile_id):
    conn = sqlite3.connect(DB_FILE)
    conn.execute('PRAGMA foreign_keys = ON;')
    c = conn.cursor()
    try:
        c.execute('DELETE FROM profiles WHERE id = ?', (profile_id,))
        conn.commit()
        st.success("Profile deleted.")
    except Exception as e:
        st.error(f"Error deleting profile: {e}")
    finally:
        conn.close()

def save_rotation(profile_id, rotation_id, start_date, parsed_data):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    data_str = json.dumps(parsed_data)
    
    start_date_str = start_date if isinstance(start_date, str) else start_date.strftime('%Y-%m-%d')
    
    try:
        c.execute('SELECT * FROM rotations WHERE profile_id = ? AND rotation_id = ? AND start_date = ?', (profile_id, rotation_id, start_date_str))
        existing = c.fetchone()
        if existing:
            c.execute('''
            UPDATE rotations SET data = ?, updated_at = CURRENT_TIMESTAMP, is_cancelled = 0
            WHERE profile_id = ? AND rotation_id = ? AND start_date = ?
            ''', (data_str, profile_id, rotation_id, start_date_str))
        else:
            c.execute('''
            INSERT INTO rotations (profile_id, rotation_id, start_date, data)
            VALUES (?, ?, ?, ?)
            ''', (profile_id, rotation_id, start_date_str, data_str))
        conn.commit()
    except Exception as e:
        st.error(f"Error saving rotation: {e}")
    conn.close()

def load_rotations(profile_id):
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT id, rotation_id, start_date, data, is_cancelled FROM rotations WHERE profile_id = ? ORDER BY id DESC", conn, params=(profile_id,))
    df = df[df['is_cancelled'] == 0]
    conn.close()
    unique_rot = {}
    for r in df.to_dict('records'):
        key = (r['rotation_id'], r['start_date'])
        if key not in unique_rot:
            unique_rot[key] = r
    loaded = list(unique_rot.values())
    return loaded

def save_blackout(profile_id, type_, start_dt, end_dt, block_id=None):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    if block_id is None:
        block_id = str(uuid.uuid4())
        
    if isinstance(start_dt, str):
        start_utc_str = start_dt
    else:
        start_utc_str = start_dt.astimezone(ZoneInfo('UTC')).isoformat()
        
    if isinstance(end_dt, str):
        end_utc_str = end_dt
    else:
        end_utc_str = end_dt.astimezone(ZoneInfo('UTC')).isoformat()
    try:
        c.execute('''
        INSERT INTO blackouts (profile_id, type, start_datetime_utc, end_datetime_utc, block_id)
        VALUES (?, ?, ?, ?, ?)
        ''', (profile_id, type_, start_utc_str, end_utc_str, block_id))
        conn.commit()
        return c.lastrowid
    except Exception as e:
        st.error(f"Error saving blackout: {e}")
        return None
    finally:
        conn.close()

def load_blackouts(profile_id):
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT * FROM blackouts WHERE profile_id = ? ORDER BY start_datetime_utc", conn, params=(profile_id,))
    conn.close()
    return df.to_dict('records') if not df.empty else []

def cancel_rotation(profile_id, rotation_id, start_date):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    try:
        c.execute('UPDATE rotations SET is_cancelled = 1 WHERE profile_id = ? AND rotation_id = ? AND start_date = ?', (profile_id, rotation_id, start_date))
        conn.commit()
    except Exception as e:
        st.error(f"Error cancelling rotation: {e}")
    conn.close()

def delete_rotation(profile_id, rotation_id, start_date):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    try:
        c.execute('''
            DELETE FROM rotations 
            WHERE profile_id = ? AND rotation_id = ? AND start_date = ?
        ''', (profile_id, rotation_id, start_date))
        conn.commit()
        st.success("Rotation permanently deleted. You can now paste and submit a corrected version.")
    except Exception as e:
        st.error(f"Error deleting rotation: {e}")
    finally:
        conn.close()

def delete_blackout(blackout_id):
    conn = sqlite3.connect(DB_FILE)
    conn.execute('PRAGMA foreign_keys = ON;')
    c = conn.cursor()
    try:
        c.execute('DELETE FROM blackouts WHERE id = ?', (blackout_id,))
        conn.commit()
    except Exception as e:
        st.error(f"Error deleting blackout: {e}")
    finally:
        conn.close()

def delete_blackout_block(block_id):
    conn = sqlite3.connect(DB_FILE)
    conn.execute('PRAGMA foreign_keys = ON;')
    c = conn.cursor()
    try:
        c.execute('DELETE FROM blackouts WHERE block_id = ?', (block_id,))
        conn.commit()
    except Exception as e:
        st.error(f"Error deleting blackout block: {e}")
    finally:
        conn.close()

def clear_profile_data(profile_id):
    conn = sqlite3.connect(DB_FILE)
    conn.execute('PRAGMA foreign_keys = ON;')
    c = conn.cursor()
    try:
        c.execute('DELETE FROM rotations WHERE profile_id = ?', (profile_id,))
        c.execute('DELETE FROM blackouts WHERE profile_id = ?', (profile_id,))
        conn.commit()
    except Exception as e:
        st.error(f"Error clearing profile data: {e}")
    finally:
        conn.close()

def change_rotation_start_date(rotation_db_id, new_start_date):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    try:
        c.execute("SELECT start_date, data FROM rotations WHERE id = ?", (rotation_db_id,))
        res = c.fetchone()
        if not res:
            st.error("Could not find rotation to move.")
            return
        
        old_start_date = datetime.strptime(res[0], '%Y-%m-%d').date()
        data = json.loads(res[1])
        delta = new_start_date - old_start_date
        
        new_data = []
        for f in data:
            new_f_date = (datetime.strptime(f['date'], '%Y-%m-%d').date() + delta).strftime('%Y-%m-%d')
            new_arr_date = (datetime.strptime(f['arr_date'], '%Y-%m-%d').date() + delta).strftime('%Y-%m-%d')
            new_report_date = (datetime.strptime(f['report_date'], '%Y-%m-%d').date() + delta).strftime('%Y-%m-%d')
            
            f['date'] = new_f_date
            f['arr_date'] = new_arr_date
            f['report_date'] = new_report_date
            new_data.append(f)
            
        new_data_str = json.dumps(new_data)
        new_start_date_str = new_start_date.strftime('%Y-%m-%d')
        
        c.execute("UPDATE rotations SET start_date = ?, data = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?", (new_start_date_str, new_data_str, rotation_db_id))
        conn.commit()
        st.success("Rotation moved successfully.")
    except Exception as e:
        st.error(f"Error moving rotation: {e}")
    finally:
        conn.close()

def change_blackout_start_date(blackout_id, new_start_date_local):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    try:
        c.execute("SELECT start_datetime_utc, end_datetime_utc FROM blackouts WHERE id = ?", (blackout_id,))
        res = c.fetchone()
        if not res:
            st.error("Could not find event to move.")
            return
            
        old_start_utc = datetime.fromisoformat(res[0])
        old_end_utc = datetime.fromisoformat(res[1])
        duration = old_end_utc - old_start_utc
        
        old_start_local = old_start_utc.astimezone(ZoneInfo(base_tz_str))
        new_start_time = old_start_local.time()
        
        new_start_local = datetime.combine(new_start_date_local, new_start_time, tzinfo=ZoneInfo(base_tz_str))
        new_end_local = new_start_local + duration
        
        new_start_utc_str = new_start_local.astimezone(ZoneInfo('UTC')).isoformat()
        new_end_utc_str = new_end_local.astimezone(ZoneInfo('UTC')).isoformat()
        
        c.execute("UPDATE blackouts SET start_datetime_utc = ?, end_datetime_utc = ? WHERE id = ?", (new_start_utc_str, new_end_utc_str, blackout_id))
        conn.commit()
        st.success("Event moved successfully.")
    except Exception as e:
        st.error(f"Error moving event: {e}")
    finally:
        conn.close()

def change_blackout_times(blackout_id, new_start_time, new_end_time):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    try:
        c.execute("SELECT start_datetime_utc FROM blackouts WHERE id = ?", (blackout_id,))
        res = c.fetchone()
        if not res:
            st.error("Could not find event to update.")
            return
            
        old_start_utc = datetime.fromisoformat(res[0])
        old_start_local = old_start_utc.astimezone(ZoneInfo(base_tz_str))
        event_date = old_start_local.date()
        
        new_start_local = datetime.combine(event_date, new_start_time, tzinfo=ZoneInfo(base_tz_str))
        new_end_local = datetime.combine(event_date, new_end_time, tzinfo=ZoneInfo(base_tz_str))
        
        if new_end_time < new_start_time:
            new_end_local += timedelta(days=1)
            
        new_start_utc_str = new_start_local.astimezone(ZoneInfo('UTC')).isoformat()
        new_end_utc_str = new_end_local.astimezone(ZoneInfo('UTC')).isoformat()
        
        c.execute("UPDATE blackouts SET start_datetime_utc = ?, end_datetime_utc = ? WHERE id = ?", (new_start_utc_str, new_end_utc_str, blackout_id))
        conn.commit()
        st.success("Event times updated successfully.")
    except Exception as e:
        st.error(f"Error updating event times: {e}")
    finally:
        conn.close()

def load_airports_tz():
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT * FROM airports", conn)
    conn.close()
    loaded = {row['code']: row['tz'] for row in df.to_dict('records')} if not df.empty else {}
    return loaded

def save_airport(code, tz):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    try:
        c.execute('INSERT OR REPLACE INTO airports (code, tz) VALUES (?, ?)', (code.upper(), tz))
        conn.commit()
    except Exception as e:
        st.error(f"Error saving airport: {e}")
    conn.close()

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

init_db()
AIRPORTS_TZ = load_airports_tz()
all_profiles = load_profiles()
profile_map = {p['name']: p['id'] for p in all_profiles}
profile_id_map = {p['id']: p['name'] for p in all_profiles}

if 'active_profile_id' not in st.session_state:
    st.session_state.active_profile_id = profile_map['Current Schedule']
elif st.session_state.active_profile_id not in profile_id_map:
    st.session_state.active_profile_id = profile_map['Current Schedule']
st.warning("""
    ⚠️ **BETA DATA WARNING:** This app resets if you leave it for too long! 
    **You must save your work** by downloading a JSON Backup (Tab 2) after every session.
""", icon="💾")
# --- NEW HEADER LAYOUT ---
col_logo, col_prof, col_tz, col_btn = st.columns([1.5, 2, 2, 1])

with col_logo:
    st.image("SkedCheckLogo.png", width=300) 

active_tab_key = "main_tabs_active"
if active_tab_key not in st.session_state:
    st.session_state[active_tab_key] = "Calendar & Details"

query_params = st.query_params
if "select_date" in query_params:
    st.session_state[active_tab_key] = "Calendar & Details"
    
    try:
        param = query_params["select_date"]
        if isinstance(param, list):
            date_str = param[0]
        else:
            date_str = param
            
        clicked_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        st.session_state.edit_event_date_picker = clicked_date
        
        # Adjust calendar view so the clicked date is visible
        st.session_state.calendar_start_date = clicked_date - timedelta(days=7)
        
        st.query_params.clear()
        st.rerun()
    except Exception:
        # Ignore bad params instead of resetting to today
        st.query_params.clear()
elif 'edit_event_date_picker' not in st.session_state:
    st.session_state.edit_event_date_picker = datetime.today().date()

def load_data_into_state(profile_id):
    st.session_state.rotations = load_rotations(profile_id)
    st.session_state.blackouts = load_blackouts(profile_id)
    st.session_state.data_loaded_for_profile = profile_id

if 'data_loaded_for_profile' not in st.session_state or st.session_state.data_loaded_for_profile != st.session_state.active_profile_id:
    load_data_into_state(st.session_state.active_profile_id)
    
with col_prof:
    profile_names = list(profile_map.keys())
    
    if st.session_state.active_profile_id not in profile_id_map:
        st.session_state.active_profile_id = profile_map['Current Schedule']
    
    active_profile_name = profile_id_map[st.session_state.active_profile_id]
    default_index = profile_names.index(active_profile_name)
 
    selected_profile_name = st.selectbox(
        "**Active Schedule Profile**",
        options=profile_names,
        index=default_index
    )
    
    if profile_map[selected_profile_name] != st.session_state.active_profile_id:
        st.session_state.active_profile_id = profile_map[selected_profile_name]
        load_data_into_state(st.session_state.active_profile_id)
        st.rerun()
      
active_profile_id = st.session_state.active_profile_id

tz_options = {
    "SEA (PST/PDT)": 'America/Los_Angeles', "SLC (MST/MDT)": 'America/Denver',
    "MSP (CST/CDT)": 'America/Chicago', "ATL (EST/EDT)": 'America/New_York'
}
tz_options_list = list(tz_options.keys())

if 'selected_tz_name' not in st.session_state:
    default_tz_name = load_setting('default_tz_name', tz_options_list[0])
    if default_tz_name not in tz_options_list:
        default_tz_name = tz_options_list[0]
    st.session_state.selected_tz_name = default_tz_name

with col_tz:
    st.selectbox(
        "Base Time Zone",
        tz_options_list,
        key="selected_tz_name",
    )
with col_btn:
    st.write(" ")
    st.write(" ")
    if st.button("Set as Default", help="Save the currently selected time zone as your default for future visits."):
        save_setting('default_tz_name', st.session_state.selected_tz_name)
        st.toast(f"Saved '{st.session_state.selected_tz_name}' as default.", icon="✅")

st.markdown("---")
# --- END NEW HEADER LAYOUT ---
# --- ALWAYS read from session state AFTER the widget has been rendered 
selected_tz_name = st.session_state.selected_tz_name
base_tz_str = tz_options[selected_tz_name]
base_tz = ZoneInfo(base_tz_str)
# --- END FIX ---
rotations = st.session_state.get('rotations', [])
blackouts = st.session_state.get('blackouts', [])

calendar_blackouts = []
processed_duties = []
utc_tz = ZoneInfo('UTC')
error_in_processing = False

vacation_events = []
training_events = []
reserve_events = []
for b in blackouts:
    start_utc = datetime.fromisoformat(b['start_datetime_utc'])
    end_utc = datetime.fromisoformat(b['end_datetime_utc'])
    
    # MODIFIED: Map types
    event_type_map = {'vacation': 'VAC', 'training': 'TRNG', 'reserve': 'RES'}
    event_id = event_type_map.get(b['type'], 'EVENT')
    
    event_obj = {
        'type': b['type'],
        'id': b['id'],
        'label': event_id,
        'start_utc': start_utc,
        'end_utc': end_utc,
        'block_id': b.get('block_id')
    }
    
    if b['type'] == 'vacation':
        vacation_events.append(event_obj)
    # UPDATED: Standard 'training' check
    elif b['type'] == 'training':
        training_events.append(event_obj)
    elif b['type'] == 'reserve':
        reserve_events.append(event_obj)
        
rotation_covered_dates = set()
for rot in rotations:
    try:
        flights = json.loads(rot['data'])
        if not flights:
            continue
        min_date = datetime.strptime(rot['start_date'], '%Y-%m-%d').date()
        max_date = max(datetime.strptime(f['arr_date'], '%Y-%m-%d').date() for f in flights)
        date = min_date
        
        while date <= max_date:
            rotation_covered_dates.add(date)
            date += timedelta(days=1)
    except:
        pass
        
    flights_by_day = {}
    
    for f in flights:
        if f['dep'] not in AIRPORTS_TZ or f['arr'] not in AIRPORTS_TZ:
            st.error(f"Error processing rotation {rot['rotation_id']}. Unknown airport: {f['dep']} or {f['arr']}. Please add it via the 'Add Airport Timezone' tool and re-submit this rotation.", icon="✈️")
            error_in_processing = True
            continue
            
        dep_tz = ZoneInfo(AIRPORTS_TZ[f['dep']])
        report_tz = dep_tz
        arr_tz = ZoneInfo(AIRPORTS_TZ[f['arr']])
        
        try:
            dep_local = datetime.strptime(f['date'] + ' ' + f['dep_time'], '%Y-%m-%d %H:%M').replace(tzinfo=dep_tz)
            arr_local = datetime.strptime(f['arr_date'] + ' ' + f['arr_time'], '%Y-%m-%d %H:%M').replace(tzinfo=arr_tz)
        except ValueError as e:
            st.error(f"Rotation {rot['rotation_id']} has invalid time data: {e}")
            error_in_processing = True
            continue
            
        if f['report_time'] and f['report_time'] != 'MANUAL':
            try:
                report_local = datetime.strptime(f['report_date'] + ' ' + f['report_time'], '%Y-%m-%d %H:%M').replace(tzinfo=report_tz)
            except ValueError:
                report_local = dep_local - timedelta(hours=1.5)
        else:
            report_local = dep_local - timedelta(hours=1.5)
            
        report_utc = report_local.astimezone(utc_tz)
        dep_utc = dep_local.astimezone(utc_tz)
        arr_utc = arr_local.astimezone(utc_tz)
        release_utc = arr_utc + timedelta(hours=f.get('turn', 0.5))
        
        f['report_utc'] = report_utc
        f['dep_utc'] = dep_utc
        f['arr_utc'] = arr_utc
        f['release_utc'] = release_utc
        
        report_date_str = f['report_date']
        if report_date_str not in flights_by_day:
            flights_by_day[report_date_str] = []
        flights_by_day[report_date_str].append(f)
        
    for report_date, flights_on_this_day in flights_by_day.items():
        if not flights_on_this_day:
            continue
            
        first_flight = flights_on_this_day[0]
        last_flight = flights_on_this_day[-1]
        
        fdp_obj = {
            'type': 'flight',
            'report_utc': first_flight['report_utc'],
            'dep_utc': first_flight['dep_utc'],
            'arr_utc': last_flight['arr_utc'],
            'release_utc': last_flight['release_utc'],
            'duty_hours': (last_flight['release_utc'] - first_flight['report_utc']).total_seconds() / 3600,
            'block': sum(fl.get('block', 0) for fl in flights_on_this_day),
            'rotation_id': rot['rotation_id'],
            'flights': list(flights_on_this_day),
            'flight': first_flight,
            'rotation_db_id': rot['id'],
            'rotation_start_date': rot['start_date']
        }
        processed_duties.append(fdp_obj)
        
calendar_blackouts.extend(vacation_events)

calendar_blackouts.extend(training_events)

for event in training_events:
    # UPDATED: Add training as 'training' type to processed_duties for rest calculation
    processed_duties.append({
        'type': 'training', 
        'report_utc': event['start_utc'],
        'dep_utc': event['start_utc'],
        'arr_utc': event['end_utc'],
        'release_utc': event['end_utc'],
        'duty_hours': (event['end_utc'] - event['start_utc']).total_seconds() / 3600,
        'block': 0.0,
        'rotation_id': event['label'],
        'flight': None,
        'flights': []
    })
    
for event in reserve_events:
    reserve_day = event['start_utc'].astimezone(base_tz).date()
    if reserve_day in rotation_covered_dates:
        continue
        
    is_overridden = False
    event_midpoint_utc = event['start_utc'] + (event['end_utc'] - event['start_utc']) / 2
            
    for vac in vacation_events:
        if vac['start_utc'] <= event_midpoint_utc <= vac['end_utc']:
            is_overridden = True
            break
    if is_overridden: continue
    
    # Check against training
    for trng in training_events:
        if trng['start_utc'] <= event_midpoint_utc <= trng['end_utc']:
            is_overridden = True
            break
    if is_overridden: continue
    
    calendar_blackouts.append(event)
    processed_duties.append({
        'type': 'reserve',
        'report_utc': event['start_utc'],
        'dep_utc': event['start_utc'],
        'arr_utc': event['end_utc'],
        'release_utc': event['end_utc'],
        'duty_hours': (event['end_utc'] - event['start_utc']).total_seconds() / 3600,
        'block': 0.0,
        'rotation_id': event['label'],
        'flight': None,
        'flights': []
    })
    
processed_duties.sort(key=lambda duty: duty['report_utc'])

# === Rotation Display Ranges for Calendar ===
rotation_display_ranges = {}
for rot in st.session_state.rotations:
    try:
        flights = json.loads(rot['data'])
        if not flights:
            continue
        min_date = datetime.strptime(rot['start_date'], '%Y-%m-%d').date()
        max_date = max(datetime.strptime(f['arr_date'], '%Y-%m-%d').date() for f in flights)
        rotation_display_ranges[rot['id']] = {
            'id': rot['rotation_id'],
            'start': min_date,
            'end': max_date,
            'db_id': rot['id'],
            'raw_data': rot
        }
    except:
        pass
# === End of added block ===
        
tab1, tab2, tab3 = st.tabs(["Calendar & Details", "Input & Manage", "Help & About"])

with tab1:
    # Keep the full calendar logic here for now
    # (your existing calendar code)
    selected_date = render_calendar_and_details(
        processed_duties, 
        calendar_blackouts, 
        base_tz, 
        rotation_display_ranges, 
        st.session_state.edit_event_date_picker,
        active_profile_id
    )

with tab2:
    st.session_state[active_tab_key] = "Input & Manage"
    
    col1, col2 = st.columns(2)
    with col1:
        with st.expander("Manage Schedule Profiles", expanded=False):
            st.subheader("Create New Profile")
            new_profile_name = st.text_input("New Profile Name", key="new_profile_name")
            clone_options = {"[Create New (Empty)]": None}
            for pid, pname in profile_id_map.items():
                clone_options[pname] = pid
            selected_clone_name = st.selectbox("Copy Data From", options=clone_options.keys())
            source_profile_id = clone_options[selected_clone_name]
            if st.button("Create Profile"):
                if new_profile_name:
                    new_pid = create_profile(new_profile_name, source_profile_id)
                    if new_pid:
                        st.session_state.active_profile_id = new_pid
                        st.session_state.data_loaded_for_profile = None
                        st.rerun()
                else:
                    st.warning("Please enter a profile name.")
            st.markdown("---")
            st.subheader("Delete Profile")
            delete_options = profile_id_map.copy()
            
            if len(delete_options) > 1:
                selected_delete_id = st.selectbox(
                    "Profile to Delete", options=delete_options.keys(),
                    format_func=lambda x: delete_options[x], key="delete_profile_select"
                )
                
                if st.button("Delete Selected Profile", type="primary"):
                    if selected_delete_id == active_profile_id:
                        st.session_state.active_profile_id = profile_map['Current Schedule']
                        
                    delete_profile(selected_delete_id)
                    st.session_state.data_loaded_for_profile = None
                    st.rerun()
            else:
                st.caption("Cannot delete the last remaining profile.")
    
    with col2:
        with st.expander("Import / Export Profile", expanded=False):
            st.subheader("Export")
            st.caption("Download your data to back it up or share it.")
            
            ical_data = generate_ical_export(processed_duties, calendar_blackouts)
            st.download_button(
                label="Export Full Calendar (iCal)",
                data=ical_data,
                file_name="SkedCheck_Schedule.ics",
                mime="text/calendar"
            )
            
            json_data = generate_json_backup()
            
            st.download_button(
                label="Backup Profile (JSON)",
                data=json_data,
                file_name=f"{profile_id_map[st.session_state.active_profile_id]}_backup.json",
                mime="application/json"
            )
            
            st.markdown("---")
            st.subheader("Import")
            st.caption("Upload a backup file or a calendar file from your airline.")
            
            uploaded_json = st.file_uploader("Restore Profile from JSON Backup", type=['json'])
            if uploaded_json is not None:
                file_contents = uploaded_json.getvalue().decode("utf-8")
                if parse_json_backup(file_contents, active_profile_id):
                    load_data_into_state(active_profile_id)
                    st.rerun()
                else:
                    st.error("Failed to import JSON file.")
                    
            uploaded_ical = st.file_uploader("Import from Airline Calendar (.ics)", type=['ics'])
            if uploaded_ical is not None:
                file_contents = uploaded_ical.getvalue().decode("utf-8")
                if parse_ical_import(file_contents, active_profile_id, base_tz):
                    load_data_into_state(active_profile_id)
                    st.rerun()
                else:
                    st.error("Failed to import iCal file.")
                    
    st.markdown("---")
    
    rotation_data = st.text_area("Rotation Data", height=200, placeholder="Paste your rotation schedule here...")
    if st.button("Parse and Submit Rotation"):
        if rotation_data:
            rotation_start = find_effective_date(rotation_data)
            if rotation_start is None:
                st.error("Could not find an 'EFFECTIVE MmmDD' date in the rotation data. Please check the format.")
            else:
                st.info(f"Detected rotation start date: {rotation_start.strftime('%Y-%m-%d')}")
                parsed_flights = parse_trip_dump(rotation_data, rotation_start)
                if parsed_flights:
                    df_parsed = pd.DataFrame(parsed_flights)
                    df_parsed['block'] = df_parsed['block'].apply(hours_to_hhmm)
                    st.subheader("Parsed Rotation Summary")
                    st.dataframe(df_parsed, use_container_width=True, hide_index=False)
                    rotation_id_match = re.search(r'([A-Z0-9]{1,4}(?:-\s*\d)?)\s+POS', rotation_data)
                    rotation_id = rotation_id_match.group(1).replace(' ', '') if rotation_id_match else f"R{int(datetime.now().timestamp())}"
                    save_rotation(active_profile_id, rotation_id, rotation_start, parsed_flights)
                    st.success(f"Saved rotation {rotation_id} to profile '{profile_id_map[active_profile_id]}'")
                    load_data_into_state(active_profile_id)
                    st.rerun()
                else:
                    st.error("No flights parsed. Check rotation format.")
        else:
            st.warning("Paste rotation data first.")
    
    with st.expander("✍️ Add Manual Rotation"):
        manual_rotation_id = st.text_input("Manual Rotation ID", key="manual_rot_id")
        manual_effective_date = st.date_input("Effective Date", value=datetime.today(), key="manual_effective_date")
        
        if 'temp_manual_flights' not in st.session_state:
            st.session_state.temp_manual_flights = []
            
        st.subheader("Add Flight Leg")
        with st.form(key="manual_flight_form"):
            manual_day = st.number_input("Day Number", min_value=1, step=1)
            manual_flight_num = st.text_input("Flight Number")
            manual_dep_apt = st.text_input("Departure Airport (e.g., SEA)")
            manual_dep_time_str = st.text_input("Departure Time (HHMM)")
            manual_arr_apt = st.text_input("Arrival Airport (e.g., LAX)")
            manual_arr_time_str = st.text_input("Arrival Time (HHMM)")
            manual_block_str = st.text_input("Block Time (HHMM or HH.MM)")
            manual_turn_str = st.text_input("Turn Time (HHMM or HH.MM)", value="0030")
            manual_report_time_str = st.text_input("Report Time (HHMM)", value="")
            manual_is_deadhead = st.checkbox("Deadhead Leg")
            
            add_flight_submitted = st.form_submit_button("Add Flight to List")
            if add_flight_submitted:
                dep_time = parse_hhmm_time(manual_dep_time_str)
                arr_time = parse_hhmm_time(manual_arr_time_str)
                if dep_time is None or arr_time is None:
                    st.error("Invalid departure or arrival time. Use HHMM format.")
                else:
                    block = parse_time_str_to_float(manual_block_str)
                    turn = parse_time_str_to_float(manual_turn_str)
                    report_local = parse_hhmm_time(manual_report_time_str)
                    report_time = report_local.strftime("%H:%M") if report_local else 'MANUAL'
                    if not manual_is_deadhead and block == 0.0:
                        st.error("Block time required for non-deadhead leg.")
                    else:
                        if manual_is_deadhead:
                            block = 0.0
                        flight = {
                            'day': manual_day,
                            'flt': manual_flight_num,
                            'dep': manual_dep_apt.upper(),
                            'dep_time': dep_time.strftime("%H:%M"),
                            'arr': manual_arr_apt.upper(),
                            'arr_time': arr_time.strftime("%H:%M"),
                            'block': block,
                            'turn': turn,
                            'report_time': report_time,
                            'indicator': 'DD' if manual_is_deadhead else ''
                        }
                        st.session_state.temp_manual_flights.append(flight)
                        st.success("Flight added to temporary list.")
                        st.rerun()
                        
        if st.session_state.temp_manual_flights:
            df_manual = pd.DataFrame(st.session_state.temp_manual_flights)
            st.subheader("Current Flights in Rotation")
            st.dataframe(df_manual, use_container_width=True, hide_index=False)
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Clear Flights"):
                    st.session_state.temp_manual_flights = []
                    st.rerun()
            with col2:
                if st.button("Submit Manual Rotation"):
                    if manual_rotation_id:
                        parsed_flights = []
                        sorted_flights = sorted(st.session_state.temp_manual_flights, key=lambda x: x['day'])
                        previous_date = None
                        for f in sorted_flights:
                            day = int(f['day'])
                            flight_date = get_date_for_day(manual_effective_date, day)
                            date_str = flight_date.strftime('%Y-%m-%d')
                            dep_h, dep_m = map(int, f['dep_time'].split(':'))
                            arr_h, arr_m = map(int, f['arr_time'].split(':'))
                            dep_minutes = dep_h * 60 + dep_m
                            arr_minutes = arr_h * 60 + arr_m
                            arr_date_str = date_str
                            
                            if arr_minutes < dep_minutes:
                                arr_date = flight_date + timedelta(days=1)
                                arr_date_str = arr_date.strftime('%Y-%m-%d')
                                
                            flight_entry = {
                                'date': date_str,
                                'dep': f['dep'],
                                'dep_time': f['dep_time'],
                                'arr': f['arr'],
                                'arr_time': f['arr_time'],
                                'arr_date': arr_date_str,
                                'report_time': f['report_time'],
                                'report_date': date_str,
                                'block': f['block'],
                                'turn': f['turn'],
                                'flt': f['flt']
                            }
                            parsed_flights.append(flight_entry)
                        save_rotation(active_profile_id, manual_rotation_id, manual_effective_date, parsed_flights)
                        st.success(f"Manual rotation {manual_rotation_id} saved.")
                        st.session_state.temp_manual_flights = []
                        load_data_into_state(active_profile_id)
                        st.rerun()
                    else:
                        st.error("Please provide Rotation ID.")
                        
    with st.expander("📄 Add Bid Package Rotation"):
        bid_start_date = st.date_input("Start Date", value=datetime.today(), key="bid_start")
        bid_data = st.text_area("Bid Package Rotation Data", height=200, placeholder="Paste your bid package rotation here...")
        if st.button("Parse and Submit Bid Rotation"):
            if bid_data:
                parsed_flights = parse_bid_dump(bid_data, bid_start_date)
                if parsed_flights:
                    df_parsed = pd.DataFrame(parsed_flights)
                    df_parsed['block'] = df_parsed['block'].apply(hours_to_hhmm)
                    st.subheader("Parsed Bid Rotation Summary")
                    st.dataframe(df_parsed, use_container_width=True, hide_index=False)
                    bid_id_match = re.search(r'#(\w+)', bid_data)
                    bid_id = bid_id_match.group(1) if bid_id_match else f"B{int(datetime.now().timestamp())}"
                    save_rotation(active_profile_id, bid_id, bid_start_date, parsed_flights)
                    st.success(f"Saved bid rotation {bid_id} to profile '{profile_id_map[active_profile_id]}'")
                    load_data_into_state(active_profile_id)
                    st.rerun()
                else:
                    st.error("No flights parsed. Check rotation format.")
            else:
                st.warning("Paste bid data first.")
                
    with st.expander("🗓️ Add Other Events"):
        st.subheader("Add Vacation (Full Days)")
        vac_start_date = st.date_input("Vacation Start", value=datetime.today(), key="vac_start")
        vac_end_date = st.date_input("Vacation End", value=datetime.today(), key="vac_end")
        if st.button("Add Vacation", key="add_vac_btn"):
            start_dt_local = datetime.combine(vac_start_date, time.min, tzinfo=base_tz)
            end_dt_local = datetime.combine(vac_end_date, time.max, tzinfo=base_tz)
            new_id = save_blackout(active_profile_id, "vacation", start_dt_local, end_dt_local)
            if new_id:
                load_data_into_state(active_profile_id)
                st.success("Vacation days added.")
                st.rerun()
                
        st.markdown("---")
        st.subheader("Add Training Duty")
        trng_start_date = st.date_input("Training Start Date", value=datetime.today(), key="trng_start_date")
        trng_start_time_str = st.text_input("Training Start Time (HHMM)", value="0800", key="trng_start_time_str")
        trng_end_date = st.date_input("Training End Date", value=datetime.today(), key="trng_end_date")
        trng_end_time_str = st.text_input("Training End Time (HHMM)", value="1700", key="trng_end_time_str")
        
        if st.button("Add Training", key="add_trng_btn"):
            trng_start_time = parse_hhmm_time(trng_start_time_str)
            trng_end_time = parse_hhmm_time(trng_end_time_str)
            if trng_start_time is None or trng_end_time is None:
                st.error("Invalid time format. Please use HHMM (e.g., 1340 or 0800).")
            else:
                start_dt_local = datetime.combine(trng_start_date, trng_start_time, tzinfo=base_tz)
                end_dt_local = datetime.combine(trng_end_date, trng_end_time, tzinfo=base_tz)
                if end_dt_local <= start_dt_local and trng_start_date == trng_end_date:
                    st.error("End time must be after start time for a single day event.")
                else:
                    block_id = str(uuid.uuid4())
                    current_date = trng_start_date
                    while current_date <= trng_end_date:
                        start_dt = datetime.combine(current_date, trng_start_time, tzinfo=base_tz)
                        end_dt = datetime.combine(current_date, trng_end_time, tzinfo=base_tz)
                        if trng_end_time < trng_start_time:
                            end_dt += timedelta(days=1)
                            
                        save_blackout(active_profile_id, "training", start_dt, end_dt, block_id=block_id)
                        current_date += timedelta(days=1)
                        
                    load_data_into_state(active_profile_id)
                    st.success("Training duty added.")
                    st.rerun()
                    
        st.markdown("---")
        st.subheader("Add Reserve Duty")
        res_start_date = st.date_input("Reserve Start Date", value=datetime.today(), key="res_start_date")
        res_start_time_str = st.text_input("Reserve Start Time (HHMM)", value="0000", key="res_start_time_str")
        res_end_date = st.date_input("Reserve End Date", value=datetime.today(), key="res_end_date")
        res_end_time_str = st.text_input("Reserve End Time (HHMM)", value="2359", key="res_end_time_str")
        
        if st.button("Add Reserve", key="add_res_btn"):
            res_start_time = parse_hhmm_time(res_start_time_str)
            res_end_time = parse_hhmm_time(res_end_time_str)
            if res_start_time is None or res_end_time is None:
                st.error("Invalid time format. Please use HHMM (e.g., 1340 or 0800).")
            else:
                block_id = str(uuid.uuid4())
                current_date = res_start_date
                while current_date <= res_end_date:
                    start_dt_local = datetime.combine(current_date, res_start_time, tzinfo=base_tz)
                    end_dt_local = datetime.combine(current_date, res_end_time, tzinfo=base_tz)
                    
                    if res_end_time < res_start_time:
                        end_dt_local += timedelta(days=1)
                        
                    save_blackout(active_profile_id, "reserve", start_dt_local, end_dt_local, block_id=block_id)
                    
                    if res_end_time > res_start_time:
                        current_date += timedelta(days=1)
                    else:
                        current_date += timedelta(days=1)
                        
                load_data_into_state(active_profile_id)
                st.success("Reserve duty added.")
                st.rerun()
                
    with st.expander("🌐 Add Airport Timezone (Shared)"):
        st.caption("Only use this if an airport was not found automatically.")
        airport_code = st.text_input("Airport Code (e.g., ZQN)")
        airport_tz = st.selectbox("Timezone", options=pytz.all_timezones)
        if st.button("Add Airport"):
            if airport_code:
                save_airport(airport_code, airport_tz)
                st.success(f"Added {airport_code}: {airport_tz}. You may need to re-submit your rotation.")
                st.rerun()
            else:
                st.warning("Enter airport code.")
                
    with st.expander("🗄️ Database Management"):
        st.subheader("Clear Data from a Profile")
        st.warning("Warning: This will permanently delete all rotations and events from the selected profile.")
        
        clear_profile_options = {p['name']: p['id'] for p in all_profiles}
        selected_profile_to_clear_name = st.selectbox(
            "Select profile to clear",
            options=clear_profile_options.keys()
        )
        
        if st.button(f"Clear All Data from '{selected_profile_to_clear_name}'", type="primary"):
            profile_id_to_clear = clear_profile_options[selected_profile_to_clear_name]
            
            clear_profile_data(profile_id_to_clear)
            st.success(f"All data cleared from profile '{selected_profile_to_clear_name}'.")
            
            if profile_id_to_clear == active_profile_id:
                load_data_into_state(active_profile_id)
            st.rerun()
            
    st.markdown("---")
    st.caption("Built with Streamlit | Data stored in SQLite DB | TZ via pytz")

with tab3:
    st.session_state[active_tab_key] = "Help & About"
    st.info("📧 **Feedback:** Found a bug or have a feature request? Email: SkedCheck411@gmail.com")
    
    with st.expander("How to Use SkedCheck (Quick Guide)", expanded=True):
        st.markdown("""
            ### 1. 🗓️ The Main View: `Calendar & Details` Tab
            This is the main screen of the application.

            * **Calendar Grid:** This shows an 12-week view of your schedule. The times on each date show the lowest available FDP and Block hours for that day. This is based on past flying and scheduled flying.
                * **Blue:** The day is legal and has sufficient rest.
                * **Yellow:** You are approaching a block or FDP limit.
                * **Red:** The day has a rest conflict, FDP violation, or block limit violation.
            * **Modify Input:** Select a date on the "Manage Date" picker to modify the event. You can also get a nicely formatted version of the rotation for sending.
            
            ### 2. ✍️ How to Add Your Schedule
            All inputs are in the **`Input & Manage`** tab.

            * **Best Way (iCrew / MiCrew):**
                1.  Copy your rotation data from iCrew or the emailed rotation text from MiCrew.
                2.  Paste it into the **"Rotation Data"** text area. **IMPORTANT FOR LCAs**: for line checks from a control seat, remove the "L" from the flight line to make sure the app counts the leg's block time.
                3.  Click **"Parse and Submit Rotation"**. The app finds the date automatically and enters each leg into the calendar.

            * **From Airline Calendar (.ics):**
                1.  In the **"Import / Export Profile"** expander, use the **"Import from Airline Calendar (.ics)"** uploader. This has been less robustly tested.
            
            * **From Bid Package:**
                1.  Open the **"Add Bid Package Rotation"** expander.
                2.  **Select the correct "Start Date"** for the rotation.
                3.  Paste your bid package text and submit.

            * **Add Vacation, Training, or Reserve:**
                1.  Open the **"Add Other Events"** expander.
                2.  For **Vacation**, just select start and end dates.
                3.  For **Training & Reserve**, select dates *and* times (e.g., 0800 to 1700). The time is set for the selected Base Time on the main page.

            ### 3. 🛠️ Managing Your Schedule
            
            * **To Edit/Delete an Event:**
                1.  In the `Calendar & Details` tab, select the date using the **"Manage Date"** picker.
                2.  An expander (e.g., "Rotation: R1234") will appear below the date pickers.
                3.  Open it to **Move**, **Cancel**, **Delete**, or **Update Times** for that event.

            * **Managing Profiles (Multiple Schedules):**
                1.  Switch schedules using the **"Active Schedule Profile"** dropdown at the top.
                2.  In the `Input & Manage` tab, open **"Manage Schedule Profiles"** to create, clone, or delete profiles.

            * **Backup and Export:**
                1.  In the `Input & Manage` tab, open **"Import / Export Profile"**.
                2.  **"Backup Profile (JSON)"** saves a file you can restore later.
                3.  **"Export Full Calendar (iCal)"** creates a file for Google Calendar, Outlook, etc.
        """)
    
    st.subheader("⚠️ Important Disclaimers")
    
    st.markdown("**Aviation Safety & Non-Official Status**")
    st.warning(
        """
        **DISCLAIMER:** This is an unofficial tool for FAR 117 reference. 
        Pilots rely on accurate scheduling—errors could have safety implications.
        
        **You must verify all calculations with official sources.** The developer is not liable for errors, damages, or regulatory violations. 
        This app interprets FAA FAR 117 rules and is not official advice.
        """
    )
    
    st.markdown("**No Warranty**")
    st.info(
        """
        This software is provided "AS-IS," without warranty of any kind, express or 
        implied, including but not limited to the warranties of merchantability, 
        fitness for a particular purpose, and non-infringement. In no event shall the 
        authors or copyright holders be liable for any claim, damages, or other 
        liability, whether in an action of contract, tort, or otherwise, arising from, 
        out of, or in connection with the software or the use or other dealings in the software.
        """
    )

    st.markdown("---")
    
    st.subheader("Privacy Policy")
    st.markdown(
        """
        This application is **100% local-only** after the first use (downloading and installing).
        * All schedule data is stored in a `SkedCheck.db` SQLite database file **on your local machine**.
        * The application does **not** transmit your data over the internet.
        * There are no servers, no tracking, and no data collection.
        """
    )
    
    st.markdown("---")

    st.subheader("License & Terms of Use")
    
    st.markdown("**Terms of Use**")
    st.markdown(
        """
        * This application is provided **free for personal use**.
        * Commercial resale is strictly prohibited.
        """
    )
    
    st.markdown("**Application License (MIT)**")
    st.code(
        """
        MIT License
        
        Copyright (c) 2025 Tim Hibbetts
        
        Permission is hereby granted, free of charge, to any person obtaining a copy
        of this software and associated documentation files (the "Software"), to deal
        in the Software without restriction, including without limitation the rights
        to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
        copies of the Software, and to permit persons to whom the Software is
        furnished to do so, subject to the following conditions:
        
        The above copyright notice and this permission notice shall be included in all
        copies or substantial portions of the Software.
        
        THE SOFTWARE IS PROVIDED "AS-IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
        IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
        FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
        AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
        LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
        OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
        SOFTWARE.
        """,
        language="text"
    )
    
    st.markdown("**Third-Party Dependencies**")
    st.markdown(
        """
        This application uses several open-source libraries, including:
        * Streamlit
        * Pandas
        * Altair
        * pytz
        * ics.py
        
        These dependencies are bundled with the application and are governed by their own respective licenses (e.g., MIT, BSD, Apache 2.0).
        """
    )

    st.markdown("---")
    st.subheader("Support This Project")
    st.markdown(
        """
        This application is free and open-source. If you find it useful, please consider supporting its development.
        
        [**Donate on Ko-fi (dogfood411)**](https://ko-fi.com/dogfood411)
        """
    )

st.markdown("---")
st.caption("© 2025 Tim Hibbetts. All rights reserved. | Built with Streamlit | Data stored in SQLite DB | TZ via pytz")