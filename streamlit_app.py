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

def init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute('PRAGMA foreign_keys = ON;')
    c = conn.cursor()
    
    c.execute('''
    CREATE TABLE IF NOT EXISTS profiles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL
    )
    ''')
    
    c.execute('''
    CREATE TABLE IF NOT EXISTS rotations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        profile_id INTEGER NOT NULL,
        rotation_id TEXT,
        start_date TEXT,
        data TEXT,
        is_cancelled BOOLEAN DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (profile_id) REFERENCES profiles (id) ON DELETE CASCADE,
        UNIQUE(profile_id, rotation_id, start_date)
    )
    ''')
    
    c.execute('''
    CREATE TABLE IF NOT EXISTS blackouts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        profile_id INTEGER NOT NULL,
        type TEXT,
        start_datetime_utc TEXT,
        end_datetime_utc TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        block_id TEXT,
        FOREIGN KEY (profile_id) REFERENCES profiles (id) ON DELETE CASCADE
    )
    ''')
    
    try:
        c.execute('SELECT block_id FROM blackouts LIMIT 1')
    except sqlite3.OperationalError:
        c.execute('ALTER TABLE blackouts ADD COLUMN block_id TEXT')
    
    c.execute('''
    CREATE TABLE IF NOT EXISTS airports (
        code TEXT PRIMARY KEY,
        tz TEXT
    )
    ''')
    
    c.execute('''
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    ''')
    
    initial_airports = [
        ('SEA', 'America/Los_Angeles'), ('LAX', 'America/Los_Angeles'), ('SFO', 'America/Los_Angeles'),
        ('PDX', 'America/Los_Angeles'), ('SAN', 'America/Los_Angeles'), ('GEG', 'America/Los_Angeles'),
        ('SLC', 'America/Denver'), ('DEN', 'America/Denver'), ('PHX', 'America/Phoenix'),
        ('MSP', 'America/Chicago'), ('ORD', 'America/Chicago'), ('DFW', 'America/Chicago'), ('IAH', 'America/Chicago'),
        ('ATL', 'America/New_York'), ('DTW', 'America/Detroit'), ('JFK', 'America/New_York'),
        ('LGA', 'America/New_York'), ('EWR', 'America/New_York'), ('BOS', 'America/New_York'),
        ('MIA', 'America/New_York'), ('CLT', 'America/New_York'), ('DCA', 'America/New_York'), ('PHL', 'America/New_York'),
        ('CVG', 'America/New_York'),
        ('CLE', 'America/New_York'), ('ATW', 'America/Chicago'), ('MEM', 'America/Chicago'),
        ('AMS', 'Europe/Amsterdam'),
        ('HNL', 'Pacific/Honolulu'), ('ANC', 'America/Anchorage'),
        ('YVR', 'America/Vancouver'), ('YYC', 'America/Denver'), ('YYZ', 'America/Toronto'), ('YUL', 'America/Toronto'),
        ('LHR', 'Europe/London'), ('CDG', 'Europe/Paris'), ('AMS', 'Europe/Amsterdam'),
        ('FRA', 'Europe/Berlin'), ('MUC', 'Europe/Berlin'), ('FCO', 'Europe/Rome'),
        ('BCN', 'Europe/Madrid'), ('MAD', 'Europe/Madrid'), ('DUB', 'Europe/Dublin'),
        ('ZRH', 'Europe/Zurich'), ('CPH', 'Europe/Copenhagen'), ('ARN', 'Europe/Stockholm'),
        ('HND', 'Asia/Tokyo'), ('NRT', 'Asia/Tokyo'), ('ICN', 'Asia/Seoul'),
        ('PEK', 'Asia/Shanghai'), ('PVG', 'Asia/Shanghai'), ('HKG', 'Asia/Hong_Kong'),
        ('TPE', 'Asia/Taipei'), ('SIN', 'Asia/Singapore'), ('BKK', 'Asia/Bangkok'), ('DXB', 'Asia/Dubai'),
        ('SYD', 'Australia/Sydney'), ('MEL', 'Australia/Sydney'), ('AKL', 'Pacific/Auckland'),
        ('MEX', 'America/Mexico_City'), ('BOG', 'America/Bogota'), ('GRU', 'America/Sao_Paulo'),
        ('EZE', 'America/Argentina/Buenos_Aires'), ('SCL', 'America/Santiago'), ('PTY', 'America/Panama'),
        ('CYFB', 'America/Iqaluit'), ('PASY', 'America/Adak'), ('EINN', 'Europe/Dublin'),
    ]
    for code, tz in initial_airports:
        c.execute('INSERT OR IGNORE INTO airports (code, tz) VALUES (?, ?)', (code, tz))
    
    c.execute('SELECT COUNT(*) FROM profiles')
    if c.fetchone()[0] == 0:
        c.execute('INSERT INTO profiles (name) VALUES (?)', ("Current Schedule",))
        
    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", ('default_tz_name', 'SEA (PST/PDT)'))
        
    conn.commit()
    conn.close()

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
                return time(hh, mm)
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
            if flight_data['indicator'] in ['D', 'DD', 'L']:
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

def parse_bid_dump(dump_text, start_date):
    flights = []
    day_offsets = {}
    current_day_letter = None
    current_date = None
    first_report_time = None
    first_report_date = None
    
    report_match = re.search(r'CHECK-IN AT (\d{2})\.(\d{2})', dump_text)
    if report_match:
        hh = report_match.group(1)
        mm = report_match.group(2)
        first_report_time = f"{hh}:{mm}"
        
    lines = [line for line in dump_text.split('\n') if line.strip()]
    
    flight_re = re.compile(
        r'^\s*([A-Z])?\s*(I)?\s*(DH|L)?\s*(\d+)\s+([A-Z]{3})\s+(\d{4})\s+([A-Z]{3})\s+(\d{4})\s+([\d.]+)\s*([\d.]+)?'
    )
    i = 0
    while i < len(lines):
        line = lines[i]
        match = flight_re.search(line)
        if match:
            day_letter, intl_indicator, is_dh, flt_num, dep_apt, dep_time, arr_apt, arr_time, block_str, turn_str = match.groups()
            
            if turn_str and '.' not in turn_str:
                turn_str = None
                
            if day_letter:
                current_day_letter = day_letter.upper()
                offset = ord(current_day_letter) - ord('A')
                day_offsets[current_day_letter] = offset
                current_date = start_date + timedelta(days=offset)
            if current_date is None:
                if not flights:
                    i += 1
                    continue
                last_flight_date_str = flights[-1]['date']
                current_date = datetime.strptime(last_flight_date_str, '%Y-%m-%d').date()
            
            date_str = current_date.strftime('%Y-%m-%d')
            
            report_time_str = 'MANUAL'
            report_date_str = date_str
            
            if current_day_letter and day_offsets.get(current_day_letter) == 0 and first_report_time:
                is_first_flight_of_day = True
                for f in flights:
                    if f['date'] == date_str:
                        is_first_flight_of_day = False
                        break
                        
                if is_first_flight_of_day:
                    report_time_str = first_report_time
                    
                if first_report_date is None:
                        first_report_date = current_date
                        
            dep_h, dep_m = int(dep_time[:2]), int(dep_time[2:])
            arr_h, arr_m = int(arr_time[:2]), int(arr_time[2:])
            
            local_dep = f"{dep_h:02d}:{dep_m:02d}"
            local_arr = f"{arr_h:02d}:{arr_m:02d}"
            dep_minutes = dep_h * 60 + dep_m
            arr_minutes = arr_h * 60 + arr_m
            arr_date = current_date
            if arr_minutes < dep_minutes:
                arr_date += timedelta(days=1)
            arr_date_str = arr_date.strftime('%Y-%m-%d')
            
            block = parse_time_str_to_float(block_str)
            
            if is_dh_or_l:
                block = 0.0
                
            turn = parse_time_str_to_float(turn_str)
            if turn == 0.0:
                turn = 0.5
                
            layover_duration = None
            hotel = None
            if i + 1 < len(lines):
                next_line = lines[i+1]
                layover_match = re.match(r'^\s*(\d+)?\s*([A-Z]{3})\s+([\d.]+)\/(.*)', next_line)
                if layover_match:
                    layover_apt_match = layover_match.group(2)
                    if layover_apt_match == arr_apt:
                        layover_duration = layover_match.group(3)
                        hotel = layover_match.group(4).strip()
                        i += 1
                        turn = 0.5
                        
            flights.append({
                'date': date_str,
                'dep': dep_apt,
                'dep_time': local_dep,
                'arr': arr_apt,
                'arr_time': local_arr,
                'arr_date': arr_date_str,
                'report_time': report_time_str,
                'report_date': report_date_str,
                'block': block,
                'turn': turn,
                'flt': flt_num,
                'layover_duration': layover_duration,
                'hotel': hotel
            })
            
        i += 1
        
    if flights:
        flights[-1]['turn'] = 0.5
        
    return flights

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

def generate_json_backup():
    profile_data = {
        "rotations": st.session_state.get('rotations', []),
        "blackouts": st.session_state.get('blackouts', [])
    }
    return json.dumps(profile_data, indent=2)

def parse_json_backup(file_contents, profile_id):
    try:
        data = json.loads(file_contents)
        rotations = data.get('rotations', [])
        blackouts = data.get('blackouts', [])
        
        rot_count = 0
        for rot in rotations:
            save_rotation(
                profile_id,
                rot['rotation_id'],
                rot['start_date'],
                json.loads(rot['data'])
            )
            rot_count += 1
        
        blackout_count = 0
        for b in blackouts:
            save_blackout(
                profile_id,
                b['type'],
                b['start_datetime_utc'],
                b['end_datetime_utc'],
                b.get('block_id')
            )
            blackout_count += 1
            
        st.success(f"Successfully imported {rot_count} rotations and {blackout_count} events.")
        return True
        
    except json.JSONDecodeError:
        st.error("Error: This does not appear to be a valid JSON backup file.")
        return False
    except Exception as e:
        st.error(f"An error occurred during import: {e}")
        return False

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

def parse_ical_import(file_contents, profile_id, base_tz):
    try:
        cal = Calendar(file_contents)
        events = sorted(cal.events, key=lambda e: e.begin)
        
        parsed_flights = []
        flight_re = re.compile(r'(\w{2,3})\s*(\d+)\s*([A-Z]{3})-([A-Z]{3})')
        simple_flight_re = re.compile(r'Flight\s*(\d+)')
        
        for event in events:
            summary = event.name or ""
            description = event.description or ""
            event_text = f"{summary} {description}"
            
            match = flight_re.search(event_text)
            
            if match:
                flt_num = match.group(2)
                dep_apt = match.group(3)
                arr_apt = match.group(4)
                
                dep_utc = event.begin.datetime
                arr_utc = event.end.datetime
                
                if dep_apt not in AIRPORTS_TZ or arr_apt not in AIRPORTS_TZ:
                    st.warning(f"Skipping flight {flt_num} ({dep_apt}-{arr_apt}) on {dep_utc.date()}: Unknown airport code. Please add it manually.")
                    continue
                    
                dep_local = dep_utc.astimezone(ZoneInfo(AIRPORTS_TZ[dep_apt]))
                arr_local = arr_utc.astimezone(ZoneInfo(AIRPORTS_TZ[arr_apt]))
                
                block_hours = (arr_utc - dep_utc).total_seconds() / 3600
                
                flight_data = {
                    'date': dep_local.strftime('%Y-%m-%d'),
                    'dep': dep_apt,
                    'dep_time': dep_local.strftime('%H:%M'),
                    'arr': arr_apt,
                    'arr_time': arr_local.strftime('%H:%M'),
                    'arr_date': arr_local.strftime('%Y-%m-%d'),
                    'report_time': 'MANUAL',
                    'report_date': dep_local.strftime('%Y-%m-%d'),
                    'block': block_hours,
                    'turn': 0.5,
                    'flt': flt_num,
                    'dep_utc': dep_utc,
                    'arr_utc': arr_utc
                }
                parsed_flights.append(flight_data)
        
        if not parsed_flights:
            st.error("No valid flight data found in iCal file. Check if flights are in 'FLT 123 AAA-BBB' format.")
            return False
            
        rotations_to_save = []
        current_rotation = []
        last_flight_time = None
        
        for flight in sorted(parsed_flights, key=lambda x: x['dep_utc']):
            if not last_flight_time or (flight['dep_utc'] - last_flight_time).total_seconds() < 36 * 3600:
                current_rotation.append(flight)
            else:
                rotations_to_save.append(current_rotation)
                current_rotation = [flight]
            last_flight_time = flight['arr_utc'].astimezone(ZoneInfo('UTC'))
            
        if current_rotation:
            rotations_to_save.append(current_rotation)
            
        saved_count = 0
        for rot in rotations_to_save:
            if rot:
                start_date = rot[0]['date']
                rot_id = f"iCal-{start_date}"
                for f in rot:
                    f.pop('dep_utc', None)
                    f.pop('arr_utc', None)
                
                save_rotation(profile_id, rot_id, start_date, rot)
                saved_count += 1
        
        st.success(f"Successfully imported {saved_count} rotations from iCal file.")
        return True
    except Exception as e:
        st.error(f"An error occurred during iCal import: {e}")
        return False

class FAR117Calculator:
    def __init__(self):
        self.duties = []
        self.last_release_utc = None
        self.acclimated = True
        self.unacclimated_until = None
        self.last_offset = None
        
    def check_30_in_168(self, start_time):
        window_start = start_time - timedelta(hours=168)
        relevant_duties = [d for d in self.duties if d['release_utc'] > window_start and d['report_utc'] < start_time]
        if not relevant_duties:
            return True
        relevant_duties = sorted(relevant_duties, key=lambda d: d['report_utc'])
        rests = []
        prev_end = window_start
        for d in relevant_duties:
            if d['report_utc'] > prev_end:
                rest = (d['report_utc'] - prev_end).total_seconds() / 3600
                rests.append(rest)
            prev_end = max(prev_end, d['release_utc'])
        if start_time > prev_end:
            rest = (start_time - prev_end).total_seconds() / 3600
            rests.append(rest)
        max_rest = max(rests) if rests else 168.0
        return max_rest >= 30
    
    def add_generic_duty(self, report_utc, release_utc, is_flight_duty=False):
        if is_flight_duty:
            if not self.check_30_in_168(report_utc):
                return False
                
            if self.last_release_utc:
                rest_hours = (report_utc - self.last_release_utc).total_seconds() / 3600
                if rest_hours < 10:
                    return False
        
        if self.last_release_utc and report_utc < self.last_release_utc:
             return False
             
        self.last_release_utc = release_utc
        self.duties.append({'report_utc': report_utc, 'release_utc': release_utc})
        return True
    
    def add_flight(self, date_str, dep_airport, local_dep_time, arr_airport, local_arr_time, arr_date_str=None, report_time=None, report_date_str=None, block=None, turn=0.0):
        pass

def get_daily_remaining_range(day_data, processed_duties, base_tz):
    # 1. Setup Timestamps for "Today"
    # We define the reference point 't' as the end of the selected day in UTC.
    day_start = datetime(day_data.year, day_data.month, day_data.day, 0, 0, 0, tzinfo=base_tz)
    day_end = datetime(day_data.year, day_data.month, day_data.day, 23, 59, 59, tzinfo=base_tz)
    day_start_utc = day_start.astimezone(ZoneInfo('UTC'))
    day_end_utc = day_end.astimezone(ZoneInfo('UTC'))
    
    t_now = day_end_utc

    # 2. Check: Am I legal RIGHT NOW based on the past? (The Backward Look)
    # ---------------------------------------------------------
    
    # --- 672 Block Check (Backward) ---
    used_block_672_backward = 0.0
    window_start_672 = t_now - timedelta(hours=672)
    
    for duty in processed_duties:
        if duty['type'] == 'flight' and duty['block'] > 0:
            flights_in_duty = duty.get('flights', [])
            if not flights_in_duty and duty.get('flight'):
                 flights_in_duty = [duty['flight']]
            
            for flight in flights_in_duty:
                if not flight: continue
                dep_utc = flight.get('dep_utc')
                arr_utc = flight.get('arr_utc')
                block = flight.get('block', 0)
                
                if not dep_utc or not arr_utc or block == 0: continue
                
                # Check intersection with the 672h window
                overlap_start = max(dep_utc, window_start_672)
                overlap_end = min(arr_utc, t_now)
                
                if overlap_end > overlap_start:
                    fraction = (overlap_end - overlap_start).total_seconds() / (arr_utc - dep_utc).total_seconds()
                    used_block_672_backward += fraction * block

    # --- 168 FDP Check (Backward) ---
    used_fdp_168_backward = 0.0
    window_start_168 = t_now - timedelta(hours=168)
    
    for duty in processed_duties:
        if duty['type'] == 'flight':
            report_utc = duty['report_utc']
            release_utc = duty['release_utc']
            
            overlap_start = max(report_utc, window_start_168)
            overlap_end = min(release_utc, t_now)
            
            if overlap_end > overlap_start:
                duration = (overlap_end - overlap_start).total_seconds() / 3600
                used_fdp_168_backward += duration

    # 3. Check: Will flying today break a FUTURE trip? (The Forward Constraint)
    # ---------------------------------------------------------
    # We assume 'Today' adds to the bucket. We must find the smallest 'slack' 
    # in any future window that overlaps with 'Today'.

    min_future_block_slack = 100.0
    min_future_fdp_slack = 60.0

    # Filter for duties that start AFTER today
    future_duties = [d for d in processed_duties if d['report_utc'] > t_now]

    for future_duty in future_duties:
        # FUTURE CHECK: BLOCK (672h)
        if future_duty['type'] == 'flight':
            # The critical moment is the END of this future flight leg/duty
            # (Strictly speaking, legality is checked at report, but the 
            # limits are rolling. We check at the future duty report time 
            # to see if 'today' is inside its lookback).
            
            future_check_point = future_duty['report_utc'] 
            
            # If the future trip is more than 672 hours away, 
            # today's flying won't affect it.
            if (future_check_point - t_now).total_seconds() / 3600 > 672:
                continue
                
            # Calculate how much block is ALREADY scheduled in that future window
            # (excluding today, because we are trying to find today's room)
            future_window_start = future_check_point - timedelta(hours=672)
            used_in_future_window = 0.0
            
            for d in processed_duties:
                # We skip duties that happen on "Today" (between day_start_utc and day_end_utc)
                # because that is the 'variable' we are solving for.
                # We only count fixed past flying and fixed future flying.
                if d['report_utc'] >= day_start_utc and d['release_utc'] <= day_end_utc:
                    continue
                
                if d['type'] == 'flight' and d['block'] > 0:
                     flights_in_duty = d.get('flights', []) or ([d['flight']] if d.get('flight') else [])
                     for flt in flights_in_duty:
                        if not flt: continue
                        dep = flt.get('dep_utc')
                        arr = flt.get('arr_utc')
                        blk = flt.get('block', 0)
                        if not dep or not arr: continue
                        
                        overlap_start = max(dep, future_window_start)
                        overlap_end = min(arr, future_check_point)
                        
                        if overlap_end > overlap_start:
                             fraction = (overlap_end - overlap_start).total_seconds() / (arr - dep).total_seconds()
                             used_in_future_window += fraction * blk
            
            slack = 100.0 - used_in_future_window
            if slack < min_future_block_slack:
                min_future_block_slack = slack

        # FUTURE CHECK: FDP (168h)
        # Note: 168h is much shorter. Future constraints only apply if the future trip 
        # is within 168h (7 days) of today.
        if future_duty['type'] == 'flight':
            future_report = future_duty['report_utc']
            
            if (future_report - t_now).total_seconds() / 3600 <= 168:
                future_window_start = future_report - timedelta(hours=168)
                used_in_future_window = 0.0
                
                for d in processed_duties:
                    # Skip "Today"
                    if d['report_utc'] >= day_start_utc and d['release_utc'] <= day_end_utc:
                        continue
                        
                    if d['type'] == 'flight':
                         overlap_start = max(d['report_utc'], future_window_start)
                         overlap_end = min(d['release_utc'], future_report)
                         
                         if overlap_end > overlap_start:
                             duration = (overlap_end - overlap_start).total_seconds() / 3600
                             used_in_future_window += duration
                             
                slack = 60.0 - used_in_future_window
                if slack < min_future_fdp_slack:
                    min_future_fdp_slack = slack

    # 4. Final Calculation
    # ---------------------------------------------------------
    remaining_block_backward = max(0.0, 100.0 - used_block_672_backward)
    remaining_fdp_backward = max(0.0, 60.0 - used_fdp_168_backward)
    
    # The actual remaining is the MINIMUM of what history allows 
    # and what the future schedule permits.
    final_remaining_block = min(remaining_block_backward, min_future_block_slack)
    final_remaining_fdp = min(remaining_fdp_backward, min_future_fdp_slack)

    # 5. Rest Calculation (Standard Backward check for 30 in 168)
    # Note: 30-in-168 is a binary "Go/No-Go" status check, usually not a "bucket" of time.
    # We keep the original logic here but ensure it checks strictly backward from now.
    max_rest = 0.0
    window_start_168_rest = t_now - timedelta(hours=168)
    
    relevant_duties = [d for d in processed_duties 
                       if d['type'] in ['flight', 'training'] 
                       and d['release_utc'] > window_start_168_rest 
                       and d['report_utc'] < t_now]
                       
    if not relevant_duties:
         max_rest = 168.0
    else:
        relevant_duties = sorted(relevant_duties, key=lambda d: d['report_utc'])
        rests = []
        prev_end = window_start_168_rest
        
        for duty in relevant_duties:
            if duty['report_utc'] > prev_end:
                rest_duration = (duty['report_utc'] - prev_end).total_seconds() / 3600
                rests.append(rest_duration)
            prev_end = max(prev_end, duty['release_utc'])
        
        if t_now > prev_end:
            final_rest = (t_now - prev_end).total_seconds() / 3600
            rests.append(final_rest)
            
        max_rest = max(rests) if rests else 168.0
        
    has_flight_duty_today = any(
        d['type'] == 'flight' and
        d['report_utc'] >= day_start_utc and
        d['report_utc'] <= day_end_utc
        for d in processed_duties
    )
    
    fdp_exceeded = used_fdp_168_backward > 60
    has_30h_conflict = max_rest < 30
    rest_conflict = has_flight_duty_today and has_30h_conflict

    return {
        'min_block': max(0.0, final_remaining_block),
        'max_block': max(0.0, final_remaining_block), # Max/Min logic can be expanded if 'Today' is variable, currently they are same
        'min_fdp': max(0.0, final_remaining_fdp),
        'max_fdp': max(0.0, final_remaining_fdp),
        'rest_conflict': rest_conflict,
        'fdp_exceeded': (has_flight_duty_today and fdp_exceeded)
    }

def hours_to_hhmm(hours):
    if hours <= 0:
        return "00:00"
    h = int(hours)
    m_float = (hours - h) * 60
    m = int(round(m_float))
    
    if m == 60:
        h += 1
        m = 0
        
    return f"{h:02d}:{m:02d}"

def copy_to_clipboard_js(text_to_copy, button_id):
    js = f"""
    <script>
    (function() {{
        const textToCopy = {json.dumps(text_to_copy)};
        const allButtons = Array.from(window.parent.document.querySelectorAll('button[data-testid="stButton"]'));
        const button = allButtons.find(btn => btn.innerText && btn.innerText.includes("{button_id}"));
        
        const textArea = document.createElement("textarea");
        textArea.value = textToCopy;
        textArea.style.position = "fixed";
        textArea.style.left = "-9999px";
        document.body.appendChild(textArea);
        textArea.focus();
        textArea.select();
        try {{
            const successful = document.execCommand('copy');
            if (successful) {{
                if (button) {{
                    const originalText = button.innerText;
                    button.innerText = 'Copied!';
                    setTimeout(() => {{
                        if (button) {{
                            button.innerText = originalText;
                        }}
                    }}, 2000);
                }}
            }} else {{
                if (button) {{
                    button.innerText = 'Copy Failed';
                }}
            }}
        }} catch (err) {{
            console.error('Fallback: Oops, unable to copy', err);
            if (button) {{
                button.innerText = 'Copy Failed';
            }}
        }}
        document.body.removeChild(textArea);
    }})();
    </script>
    """
    return js

init_db()
AIRPORTS_TZ = load_airports_tz()
all_profiles = load_profiles()
profile_map = {p['name']: p['id'] for p in all_profiles}
profile_id_map = {p['id']: p['name'] for p in all_profiles}

if 'active_profile_id' not in st.session_state:
    st.session_state.active_profile_id = profile_map['Current Schedule']
elif st.session_state.active_profile_id not in profile_id_map:
    st.session_state.active_profile_id = profile_map['Current Schedule']

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
        selected_manage_date = datetime.strptime(query_params["select_date"][0], '%Y-%m-%d').date()
        st.session_state.edit_event_date_picker = selected_manage_date
        st.query_params.clear()
    except (ValueError, TypeError):
        if 'edit_event_date_picker' not in st.session_state:
            st.session_state.edit_event_date_picker = datetime.today().date()
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

calc = FAR117Calculator()
if not error_in_processing:
    for duty in processed_duties:
        is_flight_duty = duty['type'] == 'flight' # UPDATED: Only flight counts for main FDP calc
        calc.add_generic_duty(duty['report_utc'], duty['release_utc'], is_flight_duty=is_flight_duty)
        
tab1, tab2, tab3 = st.tabs(["Calendar & Details", "Input & Manage", "Help & About"])

with tab1:
    st.session_state[active_tab_key] = "Calendar & Details"
    
    today = datetime.now(tz=base_tz).date()
    
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
        except (json.JSONDecodeError, ValueError, TypeError):
            st.error(f"Could not display rotation {rot.get('rotation_id', 'Unknown')}. Data may be corrupt or incomplete.")
            
    weekday = today.weekday()
    current_week_start = today - timedelta(days=(weekday + 1) % 7)
    current_default = current_week_start - timedelta(weeks=2)
    
    default_start = current_default
    
    if 'calendar_start_date' not in st.session_state:
        st.session_state.calendar_start_date = default_start
        
    calendar_start = st.session_state.calendar_start_date
    
    # --- NEW LAYOUT: Use nested columns for labels ---
    col_start, col_manage = st.columns(2)
    
    with col_start:
        lbl_col, inp_col = st.columns([1, 2]) # 1:2 ratio for label/widget
        with lbl_col:
            st.markdown("**Calendar Start**") # Aligns to the top
        with inp_col:
            def update_calendar_start_callback():
                st.session_state.calendar_start_date = st.session_state.date_input_key
            st.date_input(
                "Calendar Start", # Hidden label
                value=st.session_state.calendar_start_date,
                key="date_input_key",
                on_change=update_calendar_start_callback,
                help="Set the starting week for the calendar view",
                label_visibility="collapsed"
            )
            
    with col_manage:
        lbl_col, inp_col = st.columns([1, 2]) # 1:2 ratio for label/widget
        with lbl_col:
            st.markdown("**Manage Date**") # Aligns to the top
        with inp_col:
            st.date_input(
                "Manage Date", # Hidden label
                key='edit_event_date_picker',
                value=st.session_state.edit_event_date_picker,
                help="View/edit events for this date",
                label_visibility="collapsed"
            )
    # --- END NEW LAYOUT ---
            
    selected_date = st.session_state.edit_event_date_picker
    events_found = False
    
    selected_date_str = selected_date.strftime('%Y-%m-%d')
    for rot in st.session_state.rotations:
        rot_info = rotation_display_ranges.get(rot['id'])
        if rot_info and (rot_info['start'] <= selected_date <= rot_info['end']):
            events_found = True
            with st.expander(f"Rotation: {rot['rotation_id']} (Started {rot['start_date']})"):
                
                st.markdown("**Civilian Format Export**")
                civilian_text = generate_civilian_export(rot_info['raw_data'], base_tz, selected_tz_name)
                st.text_area("Civilian-Readable Schedule", civilian_text, height=150, key=f"civ_text_{rot['id']}")
                
                button_label = f"Copy {rot['rotation_id']} to Clipboard"
                if st.button(button_label, key=f"copy_btn_{rot['id']}"):
                    js = copy_to_clipboard_js(civilian_text, button_label)
                    components.html(js, height=0)
                    
                st.markdown("---")
                
                st.markdown("**Move Event Date**")
                new_start = st.date_input("New Start Date", value=rot_info['start'], key=f"new_start_rot_{rot['id']}")
                if st.button("Move Rotation", key=f"move_rot_{rot['id']}"):
                    change_rotation_start_date(rot['id'], new_start)
                    load_data_into_state(active_profile_id)
                    st.rerun()
                st.markdown("---")
                
                st.markdown("**Cancel Event**")
                if st.button("Cancel This Rotation", key=f"cancel_rot_{rot['id']}", type="primary"):
                    cancel_rotation(active_profile_id, rot['rotation_id'], rot['start_date'])
                    st.session_state.rotations = [r for r in st.session_state.rotations if r['id'] != rot['id']]
                    st.success("Rotation cancelled.")
                    st.rerun()
                    
    day_start_local = datetime.combine(selected_date, time.min, tzinfo=base_tz)
    day_end_local = datetime.combine(selected_date, time.max, tzinfo=base_tz)
    day_start_utc = day_start_local.astimezone(utc_tz)
    day_end_utc = day_end_local.astimezone(utc_tz)
    
    for event in calendar_blackouts:
        if (event['start_utc'] <= day_end_utc) and (event['end_utc'] >= day_start_utc):
            events_found = True
            
            start_local_str = event['start_utc'].astimezone(base_tz).strftime('%m/%d %H:%M')
            end_local_str = event['end_utc'].astimezone(base_tz).strftime('%m/%d %H:%M')
            
            with st.expander(f"{event['label'].capitalize()}: {start_local_str} to {end_local_str}"):
                
                st.markdown("**Move Event Date**")
                event_start_date = event['start_utc'].astimezone(base_tz).date()
                new_start_blk = st.date_input("New Start Date", value=event_start_date, key=f"new_start_blk_{event['id']}")
                if st.button("Move Event", key=f"move_blk_{event['id']}"):
                    change_blackout_start_date(event['id'], new_start_blk)
                    load_data_into_state(active_profile_id)
                    st.rerun()
                    
                if event['type'] in ['training', 'reserve']: # UPDATED: Simple type check
                    st.markdown("---")
                    st.markdown("**Update Event Times (for this day only)**")
                    
                    event_start_time_local = event['start_utc'].astimezone(base_tz).time()
                    event_end_time_local = event['end_utc'].astimezone(base_tz).time()
                    
                    new_start_time_str = st.text_input("New Start Time (HHMM)", value=event_start_time_local.strftime('%H%M'), key=f"new_start_time_{event['id']}")
                    new_end_time_str = st.text_input("New End Time (HHMM)", value=event_end_time_local.strftime('%H%M'), key=f"new_end_time_{event['id']}")
                    
                    if st.button("Update Times", key=f"update_times_{event['id']}"):
                        new_start_t = parse_hhmm_time(new_start_time_str)
                        new_end_t = parse_hhmm_time(new_end_time_str)
                        if new_start_t and new_end_t:
                            change_blackout_times(event['id'], new_start_t, new_end_t)
                            load_data_into_state(active_profile_id)
                            st.rerun()
                        else:
                            st.error("Invalid time format. Please use HHMM.")
                            
                st.markdown("---")
                
                st.markdown("**Delete Event**")
                if st.button(f"Delete This {event['label']}", key=f"delete_blackout_{event['id']}", type="primary"):
                    delete_blackout(event['id'])
                    st.session_state.blackouts = [b for b in st.session_state.blackouts if b['id'] != event['id']]
                    st.success(f"{event['label']} deleted.")
                    st.rerun()
                    
                if event['type'] in ['training', 'reserve'] and event.get('block_id'): # UPDATED
                    if st.button(f"Delete ENTIRE Block of {event['label']}s", key=f"delete_block_{event['id']}", type="primary"):
                        delete_blackout_block(event['block_id'])
                        load_data_into_state(active_profile_id)
                        st.success(f"Entire {event['label']} block deleted.")
                        st.rerun()
                        
    if not events_found:
        st.caption("No manageable events found on this date.")
    
    # --- st.markdown("---") was DELETED from here ---
    
    weekday_offset = (calendar_start.weekday() + 1) % 7
    week_start = calendar_start - timedelta(days=weekday_offset)
    days_of_week = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
    
    chart_dates = []
    chart_avg_blocks = []
    chart_conflicts = 0
    
    html = '<table class="calendar-table">'
    html += '<tr>' + ''.join(f'<th>{day}</th>' for day in days_of_week) + '</tr>'
    
    for week in range(12):
        html += '<tr>'
        for day_idx in range(7):
            day_data = week_start + timedelta(days=week*7 + day_idx)
            day_type = ''
            class_name = ''
            
            day_start_local = datetime.combine(day_data, time.min, tzinfo=base_tz)
            day_end_local = datetime.combine(day_data, time.max, tzinfo=base_tz)
            day_start_utc = day_start_local.astimezone(utc_tz)
            day_end_utc = day_end_local.astimezone(utc_tz)
            
            for rot_db_id, rot_info in rotation_display_ranges.items():
                if rot_info['start'] <= day_data <= rot_info['end']:
                    day_type = rot_info['id']
                    class_name = 'rotation-id'
                    break
                    
            if not day_type:
                for event in calendar_blackouts:
                    if (event['start_utc'] <= day_end_utc) and (event['end_utc'] >= day_start_utc):
                        day_type = event['label']
                        class_name_map = {'vacation': 'vac-label', 'training': 'trng-label', 'reserve': 'res-label'}
                        class_name = class_name_map.get(event['type'], 'rotation-id')
                        break
                        
            summary = get_daily_remaining_range(day_data, processed_duties, base_tz)
            
            cell_class = 'calendar-cell'
            if summary['min_block'] <= 0 or summary['min_fdp'] <= 0 or summary['rest_conflict'] or summary['fdp_exceeded']:
                cell_class += ' conflict'
                chart_conflicts += 1
            elif summary['min_block'] < 10 or summary['min_fdp'] < 10:
                cell_class += ' warning'
            else:
                cell_class += ' compliant'
            if class_name:
                cell_class += f' {class_name}'
                
            tooltip = (
                f"Block Remaining: {hours_to_hhmm(summary['min_block'])}\n"
                f"FDP Remaining: {hours_to_hhmm(summary['min_fdp'])}\n"
                f"Rest Conflict: {'Yes' if summary['rest_conflict'] else 'No'}\n"
                f"FDP Exceeded: {'Yes' if summary['fdp_exceeded'] else 'No'}"
            )
            
            html += f'<td class="{cell_class}" title="{tooltip}">'
            
            date_str = day_data.strftime('%Y-%m-%d')
            html += f'<div><a href="?select_date={date_str}" target="_self" class="day-link">{day_data.month}/{day_data.day}</a></div>'
            
            if day_type:
                html += f'<span class="{class_name}">{day_type}</span><br>'
                
            html += f'<div class="block-hours">Block: {hours_to_hhmm(summary["min_block"])}</div>'
            html += f'<div class="block-hours">FDP: {hours_to_hhmm(summary["min_fdp"])}</div>'
            html += '</td>'
            
            avg_block = (summary['min_block'] + summary['max_block']) / 2
            chart_dates.append(day_data)
            chart_avg_blocks.append(avg_block)
            
        html += '</tr>'
    html += '</table>'
    
    calendar_container = st.container()
    
    with calendar_container:
        st.markdown('<div id="calendar-wrapper">', unsafe_allow_html=True)
        st.markdown(html, unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
        
    scroll_result = None

    # Trend Chart Code Removed

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