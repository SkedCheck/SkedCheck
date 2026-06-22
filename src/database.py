import sqlite3
import pandas as pd
from datetime import datetime
import uuid
import streamlit as st
import json

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
        ('CVG', 'America/New_York'), ('CLE', 'America/New_York'), ('ATW', 'America/Chicago'), ('MEM', 'America/Chicago'),
        ('AMS', 'Europe/Amsterdam'), ('HNL', 'Pacific/Honolulu'), ('ANC', 'America/Anchorage'),
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

# === All other database functions ===

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
                FROM rotations WHERE profile_id = ?
            ''', (new_profile_id, source_profile_id))
            c.execute('''
                INSERT INTO blackouts (profile_id, type, start_datetime_utc, end_datetime_utc, block_id)
                SELECT ?, type, start_datetime_utc, end_datetime_utc, block_id
                FROM blackouts WHERE profile_id = ?
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
        c.execute('SELECT * FROM rotations WHERE profile_id = ? AND rotation_id = ? AND start_date = ?', 
                 (profile_id, rotation_id, start_date_str))
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
    return list(unique_rot.values())

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
        c.execute('UPDATE rotations SET is_cancelled = 1 WHERE profile_id = ? AND rotation_id = ? AND start_date = ?', 
                 (profile_id, rotation_id, start_date))
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
    # (Keep the full function from original - it's long, but paste it here)
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
        
        c.execute("UPDATE rotations SET start_date = ?, data = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?", 
                 (new_start_date_str, new_data_str, rotation_db_id))
        conn.commit()
        st.success("Rotation moved successfully.")
    except Exception as e:
        st.error(f"Error moving rotation: {e}")
    finally:
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
        st.success("Rotation deleted.")
    except Exception as e:
        st.error(f"Error deleting rotation: {e}")
    finally:
        conn.close()

# Add the other change functions similarly (change_blackout_start_date, change_blackout_times, etc.)
# For brevity, I'll ask you to copy them in the next message if needed.