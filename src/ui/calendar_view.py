import streamlit as st
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
import streamlit.components.v1 as components
from src.utils import hours_to_hhmm, copy_to_clipboard_js
from src.calculator import get_daily_remaining_range
from src.ui.rotation_editor import render_rotation_editor


def render_calendar_and_details(processed_duties, calendar_blackouts, base_tz, rotation_display_ranges, selected_date, active_profile_id):
    """Full calendar view with HTML table."""
    
    if 'main_tabs_active' not in st.session_state:
        st.session_state.main_tabs_active = "Calendar & Details"
    
    st.subheader("Calendar & Details")
    
        # Date pickers
    col_start, col_manage = st.columns(2)
    with col_start:
        lbl_col, inp_col = st.columns([1, 2])
        with lbl_col:
            st.markdown("**Calendar Start**")
        with inp_col:
            if 'calendar_start_date' not in st.session_state:
                st.session_state.calendar_start_date = datetime.today().date() - timedelta(days=14)
            st.date_input("Calendar Start", key="calendar_start_date", label_visibility="collapsed")

    with col_manage:
        lbl_col, inp_col = st.columns([1, 2])
        with lbl_col:
            st.markdown("**Manage Date**")
        with inp_col:
            st.date_input("Manage Date", key='edit_event_date_picker', label_visibility="collapsed")

    selected_date = st.session_state.edit_event_date_picker

    # === FULL CALENDAR HTML TABLE ===
    weekday_offset = (st.session_state.get('calendar_start_date', datetime.today().date()).weekday() + 1) % 7
    week_start = st.session_state.get('calendar_start_date', datetime.today().date()) - timedelta(days=weekday_offset)
    days_of_week = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
    
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
            day_start_utc = day_start_local.astimezone(ZoneInfo('UTC'))
            day_end_utc = day_end_local.astimezone(ZoneInfo('UTC'))
            
            # Find rotation or blackout
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
            if summary['min_block'] <= 0 or summary['min_fdp'] <= 0 or summary.get('rest_conflict', False) or summary.get('fdp_exceeded', False):
                cell_class += ' conflict'
            elif summary['min_block'] < 10 or summary['min_fdp'] < 10:
                cell_class += ' warning'
            else:
                cell_class += ' compliant'
            if class_name:
                cell_class += f' {class_name}'
                
            tooltip = f"Block Remaining: {hours_to_hhmm(summary['min_block'])}\nFDP Remaining: {hours_to_hhmm(summary['min_fdp'])}"
            
            html += f'<td class="{cell_class}" title="{tooltip}">'
            date_str = day_data.strftime('%Y-%m-%d')
            html += f'<div><a href="?select_date={date_str}" target="_self" class="day-link">{day_data.month}/{day_data.day}</a></div>'
            
            if day_type:
                html += f'<span class="{class_name}">{day_type}</span><br>'
                
            html += f'<div class="block-hours">Block: {hours_to_hhmm(summary["min_block"])}</div>'
            html += f'<div class="block-hours">FDP: {hours_to_hhmm(summary["min_fdp"])}</div>'
            html += '</td>'
            
        html += '</tr>'
    html += '</table>'
    
    st.markdown(html, unsafe_allow_html=True)
    
    st.info("✅ Full calendar is now rendered from the UI module.")

    # === ROTATION EDITOR ===
    from src.ui.rotation_editor import render_rotation_editor
    render_rotation_editor(selected_date, active_profile_id)


    return selected_date
