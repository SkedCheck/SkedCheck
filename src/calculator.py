from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def get_daily_remaining_range(day_data, processed_duties, base_tz):
    """
    Calculates remaining Block and FDP hours for a specific day.
    
    - Block: Max 100 hours in any 28-day (672 hour) window
    - FDP: Max 60 hours in any 7-day (168 hour) window
    """
    # Create timestamps for the day we're checking
    day_start = datetime.combine(day_data, datetime.min.time(), tzinfo=base_tz)
    day_end = datetime.combine(day_data, datetime.max.time(), tzinfo=base_tz)
    day_start_utc = day_start.astimezone(ZoneInfo('UTC'))
    day_end_utc = day_end.astimezone(ZoneInfo('UTC'))
    
    t_now = day_end_utc   # We check from the end of the day

    # === 1. Backward Block Calculation (Past 28 days) ===
    used_block = 0.0
    window_start = t_now - timedelta(hours=672)   # 28 days ago
    
    for duty in processed_duties:
        if duty['type'] != 'flight' or duty.get('block', 0) <= 0:
            continue
        flights = duty.get('flights', []) or ([duty.get('flight')] if duty.get('flight') else [])
        for f in flights:
            if not f or not f.get('dep_utc') or not f.get('arr_utc'):
                continue
            dep = f['dep_utc']
            arr = f['arr_utc']
            blk = f.get('block', 0)
            
            overlap_start = max(dep, window_start)
            overlap_end = min(arr, t_now)
            if overlap_end > overlap_start:
                fraction = (overlap_end - overlap_start).total_seconds() / (arr - dep).total_seconds()
                used_block += fraction * blk

    # === 2. Backward FDP Calculation (Past 7 days) ===
    used_fdp = 0.0
    window_start = t_now - timedelta(hours=168)   # 7 days ago
    for duty in processed_duties:
        if duty['type'] == 'flight':
            overlap_start = max(duty['report_utc'], window_start)
            overlap_end = min(duty['release_utc'], t_now)
            if overlap_end > overlap_start:
                used_fdp += (overlap_end - overlap_start).total_seconds() / 3600

    remaining_block = max(0.0, 100.0 - used_block)
    remaining_fdp = max(0.0, 60.0 - used_fdp)

    return {
        'min_block': remaining_block,
        'max_block': remaining_block,
        'min_fdp': remaining_fdp,
        'max_fdp': remaining_fdp,
        'rest_conflict': False,
        'fdp_exceeded': False
    }