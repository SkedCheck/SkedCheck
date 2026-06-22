import streamlit as st
import json
from datetime import datetime
from src.database import (
    load_rotations, cancel_rotation, delete_rotation, change_rotation_start_date
)

def find_rotations_on_date(selected_date, profile_id):
    """Load rotations fresh from DB and find ones covering the date."""
    rotations = load_rotations(profile_id)
    
    if not rotations:
        return []

    active = []
    for rot in rotations:
        try:
            flights = json.loads(rot['data'])
            if not flights:
                continue

            flight_dates = []
            for f in flights:
                try:
                    d1 = datetime.strptime(f['date'], '%Y-%m-%d').date()
                    d2 = datetime.strptime(f['arr_date'], '%Y-%m-%d').date()
                    flight_dates.append(d1)
                    flight_dates.append(d2)
                except:
                    continue

            if not flight_dates:
                continue

            rot_start = min(flight_dates)
            rot_end = max(flight_dates)

            check_date = selected_date
            if isinstance(check_date, datetime):
                check_date = check_date.date()

            if rot_start <= check_date <= rot_end:
                active.append({
                    'db_id': rot['id'],
                    'rotation_id': rot['rotation_id'],
                    'start_date': rot['start_date'],
                    'flights': flights
                })
        except:
            continue

    return active


def render_rotation_editor(selected_date, active_profile_id):
    if isinstance(selected_date, datetime):
        selected_date = selected_date.date()

    active_rots = find_rotations_on_date(selected_date, active_profile_id)

    if not active_rots:
        st.caption("No active rotations found on this date.")
        return

    for rot in active_rots:
        with st.expander(f"✈️ {rot['rotation_id']} ({rot['start_date']})", expanded=True):
            st.write(f"**Covers:** {rot['flights'][0]['date']} → {rot['flights'][-1]['arr_date']}")
            st.caption(f"Total legs: {len(rot['flights'])}")

            col1, col2, col3 = st.columns(3)

            with col1:
                if st.button("Move Rotation", key=f"move_{rot['db_id']}"):
                    st.session_state[f"show_move_{rot['db_id']}"] = True

            with col2:
                if st.button("Delete Rotation", key=f"delete_{rot['db_id']}", type="primary"):
                    st.session_state[f"show_delete_{rot['db_id']}"] = True

            with col3:
                if st.button("Cancel (Soft Delete)", key=f"cancel_{rot['db_id']}"):
                    cancel_rotation(active_profile_id, rot['rotation_id'], rot['start_date'])
                    st.rerun()

            # Move Rotation
            if st.session_state.get(f"show_move_{rot['db_id']}", False):
                new_date = st.date_input(
                    "New Start Date",
                    value=datetime.strptime(rot['start_date'], '%Y-%m-%d').date(),
                    key=f"new_date_{rot['db_id']}"
                )
                if st.button("Confirm Move", key=f"confirm_move_{rot['db_id']}"):
                    change_rotation_start_date(rot['db_id'], new_date)
                    st.session_state[f"show_move_{rot['db_id']}"] = False
                    st.rerun()

            # Delete Confirmation
            if st.session_state.get(f"show_delete_{rot['db_id']}", False):
                st.warning("**This will permanently delete the rotation.** You can add a corrected version afterward.")
                col_a, col_b = st.columns(2)
                with col_a:
                    if st.button("Yes, Delete Permanently", key=f"confirm_delete_{rot['db_id']}"):
                        delete_rotation(active_profile_id, rot['rotation_id'], rot['start_date'])
                        st.session_state.data_loaded_for_profile = None
                        st.session_state[f"show_delete_{rot['db_id']}"] = False
                        st.rerun()
                with col_b:
                    if st.button("Cancel", key=f"cancel_delete_{rot['db_id']}"):
                        st.session_state[f"show_delete_{rot['db_id']}"] = False
                        st.rerun()
