import numpy as np
import pandas as pd
from datetime import datetime, timedelta

N_USERS = 5000
CORE_EVENTS = ["app_session_start","project_created","daily_status_update"]
GOLDEN_PATH = ["task_assigned","slack_integration_used"]
RETENTION_CUTOFF = 90 
ACTIVATION_WINDOW = 14
MAX_GENERATION_SPAN = 180
START_DATE = datetime(2025, 1, 1)
OUTPUT_LIST = []

def rand_timedelta(days_min,days_max):
    """Generates a random timedelta between min and max days"""
    return timedelta(days=np.random.randint(days_min,days_max) + np.random.rand())

def add_event(user_id, timestamp, event_name):
    """Adds a single event record to the output list."""
    OUTPUT_LIST.append({
        'user_id': f'user_{user_id}',
        'timestamp': timestamp,
        'event_name': event_name
    })

for i in range(1, N_USERS + 1): # Loop through all 5000 users
    user_id = i
    is_retained = np.random.rand() < 0.30  # 30% retention rate

    # 1. Define Sign-Up Date: Random date within the MAX_GENERATION_SPAN
    sign_up_date = START_DATE + timedelta(days=np.random.randint(0, MAX_GENERATION_SPAN))

    # 2. Define Last Event Date based on Retention Status
    if is_retained:
        # Retained users stay active well past the 90-day cutoff
        # e.g., active for 100 to 180 days total
        total_duration = np.random.randint(RETENTION_CUTOFF + 10, MAX_GENERATION_SPAN) 
        last_event_date = sign_up_date + timedelta(days=total_duration)
    else:
        # Churned users stop well before the 90-day cutoff
        # e.g., active for 3 to 60 days max
        total_duration = np.random.randint(3, RETENTION_CUTOFF - 30) 
        last_event_date = sign_up_date + timedelta(days=total_duration)

    # 3. Generate Events within the User's Lifecycle
    current_date = sign_up_date
    
    while current_date <= last_event_date:
        # A. Mandatory 'app_session_start' (one per day/session)
        add_event(user_id, current_date, 'app_session_start')

        # B. Golden Path Bias (CRITICAL for the HMM insight)
        is_in_activation_window = (current_date - sign_up_date).days < ACTIVATION_WINDOW
        
        if is_retained and is_in_activation_window:
            # 80% chance for Retained users to complete the critical sequence early
            if np.random.rand() < 0.80: 
                # Add 'task_assigned' and then 'slack_integration_used' close together
                add_event(user_id, current_date + timedelta(minutes=np.random.randint(10, 60)), GOLDEN_PATH[0])
                add_event(user_id, current_date + timedelta(minutes=np.random.randint(61, 120)), GOLDEN_PATH[1])
        
        # C. Add Core/Noise Events (Happens for all users)
        for event in CORE_EVENTS:
            if np.random.rand() < 0.5: # 50% chance of using a core feature
                add_event(user_id, current_date + rand_timedelta(0, 1), event)

        # D. Advance Time for Next Session (simulate the next login 1-3 days later)
        current_date += rand_timedelta(1, 3) 

# --- Finalization ---
final_df = pd.DataFrame(OUTPUT_LIST)

# Ensure data is sorted for the next step (Data Pipeline)
final_df = final_df.sort_values(by=['user_id', 'timestamp']).reset_index(drop=True)

# Save the raw data artifact
final_df.to_csv('data/raw_events.csv', index=False)
print(f"Generated {len(final_df)} events for {N_USERS} users and saved to data/raw_events.csv")