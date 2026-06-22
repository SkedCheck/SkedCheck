from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def get_daily_remaining_range(day_data, processed_duties, base_tz):
    """
    Returns the lowest remaining Block (100h in 672h) and FDP (60h in 168h)
    availability during the given calendar day.
    """
    day_start_local = datetime.combine(day_data, datetime.min.time(), tzinfo=base_tz)
    day_end_local   = datetime.combine(day_data, datetime.max.time(), tzinfo=base_tz)

    day_start_utc = day_start_local.astimezone(ZoneInfo('UTC'))
    day_end_utc   = day_end_local.astimezone(ZoneInfo('UTC'))

    # Collect all flight segments
    segments = []
    for duty in processed_duties:
        if duty['type'] != 'flight':
            continue
        flights = duty.get('flights') or ([duty.get('flight')] if duty.get('flight') else [])
        for f in flights:
            if f and f.get('dep_utc') and f.get('arr_utc') and f.get('block', 0) > 0:
                segments.append({
                    'start': f['dep_utc'],
                    'end': f['arr_utc'],
                    'block': f['block'],
                    'counts_for_block': f.get('counts_for_block', True)
                })

    if not segments:
        return {
            'min_block': 100.0, 'max_block': 100.0,
            'min_fdp': 60.0, 'max_fdp': 60.0,
            'rest_conflict': False, 'fdp_exceeded': False
        }

    # === BLOCK HOURS: 672-hour lookback ===
    min_remaining_block = 100.0
    current = day_start_utc

    while current <= day_end_utc:
        window_start = current - timedelta(hours=672)
        used_block = 0.0

        for seg in segments:
            if not seg.get('counts_for_block', True):
                continue

            overlap_start = max(seg['start'], window_start)
            overlap_end = min(seg['end'], current)
            if overlap_end > overlap_start:
                total_dur = (seg['end'] - seg['start']).total_seconds()
                overlap_dur = (overlap_end - overlap_start).total_seconds()
                used_block += (overlap_dur / total_dur) * seg['block']

        remaining = 100.0 - used_block
        if remaining < min_remaining_block:
            min_remaining_block = remaining

        current += timedelta(hours=1)

    # === FDP: 168-hour lookback ===
    min_remaining_fdp = 60.0
    current = day_start_utc

    while current <= day_end_utc:
        window_start = current - timedelta(hours=168)
        used_fdp = 0.0

        for duty in processed_duties:
            if duty['type'] == 'flight':
                overlap_start = max(duty['report_utc'], window_start)
                overlap_end = min(duty['release_utc'], current)
                if overlap_end > overlap_start:
                    used_fdp += (overlap_end - overlap_start).total_seconds() / 3600

        remaining_fdp = 60.0 - used_fdp
        if remaining_fdp < min_remaining_fdp:
            min_remaining_fdp = remaining_fdp

        current += timedelta(hours=1)

    return {
        'min_block': max(0.0, round(min_remaining_block, 1)),
        'max_block': max(0.0, round(min_remaining_block, 1)),
        'min_fdp':   max(0.0, round(min_remaining_fdp, 1)),
        'max_fdp':   max(0.0, round(min_remaining_fdp, 1)),
        'rest_conflict': False,
        'fdp_exceeded': False
    }
