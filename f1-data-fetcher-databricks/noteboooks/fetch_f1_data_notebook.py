import requests
import pandas as pd
import numpy as np
import time
from datetime import timedelta
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = dbutils.secrets.get(scope="my-secret-scope", key="mongodb-uri")
mongo_client = MongoClient(uri, server_api=ServerApi('1'))

try:
    mongo_client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)
    exit(1)
collection = ""
table = ""
mongo_db = mongo_client["collection"]
mongo_collection = mongo_db["table"]

def fetch_json(url):
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as http_err:
        if resp.status_code == 422:
            print(f"422 Error: Too much data requested or invalid query: {url}")
            return None
        else:
            print(f"HTTP error occurred: {http_err} for URL: {url}")
            return None
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

def convert_date_columns(df, columns):
    for c in columns:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True, errors='coerce')

def format_time(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

session_key = "9940"
driver_number = 44

print("Fetching static data...")

driver_data = fetch_json(f"https://api.openf1.org/v1/drivers?session_key={session_key}&driver_number={driver_number}") or []
driver_name = driver_data[0].get('full_name', 'Unknown') if driver_data else 'Unknown'
print(f"Driver name: {driver_name}")

session_data = fetch_json(f"https://api.openf1.org/v1/sessions?session_key={session_key}") or []
session_name = session_data[0].get('session_name', session_key) if session_data else session_key
print(f"Session name: {session_name}")

position_data = fetch_json(f"https://api.openf1.org/v1/position?session_key={session_key}&driver_number={driver_number}") or []
position_df = pd.DataFrame(position_data)
convert_date_columns(position_df, ['date'])

intervals_data = fetch_json(f"https://api.openf1.org/v1/intervals?session_key={session_key}&driver_number={driver_number}") or []
intervals_df = pd.DataFrame(intervals_data)

for possible_col in ['date', 'session_time', 'date_time', 'time']:
    if possible_col in intervals_df.columns:
        intervals_df[possible_col] = pd.to_datetime(intervals_df[possible_col], utc=True, errors='coerce')
        intervals_df.rename(columns={possible_col: 'date'}, inplace=True)
        break
else:
    print("Warning: No datetime column found in intervals_df")
    intervals_df['date'] = pd.NaT

laps_data = fetch_json(f"https://api.openf1.org/v1/laps?session_key={session_key}&driver_number={driver_number}") or []
laps_df = pd.DataFrame(laps_data)
convert_date_columns(laps_df, ['date_start'])

stints_data = fetch_json(f"https://api.openf1.org/v1/stints?session_key={session_key}&driver_number={driver_number}") or []
stints_df = pd.DataFrame(stints_data)

if laps_df.empty:
    raise ValueError("No lap data found to define replay window.")

start_time = laps_df['date_start'].min()
end_time = laps_df['date_start'].max() + timedelta(minutes=5)

print(f"Starting real-time replay from {start_time} to {end_time}")

current_time = start_time
step = timedelta(seconds=1)

while current_time < end_time:
    next_time = current_time + step

    url = (
        f"https://api.openf1.org/v1/car_data?"
        f"session_key={session_key}&driver_number={driver_number}"
        f"&date>{format_time(current_time)}&date<{format_time(next_time)}"
    )

    print(f"Fetching data for time window {format_time(current_time)} to {format_time(next_time)}")
    car_data = fetch_json(url) or []

    if not car_data:
        print(f"Fetched 0 records for time window {format_time(current_time)} to {format_time(next_time)}")
        current_time = next_time
        time.sleep(0.1)
        continue

    car_df = pd.DataFrame(car_data)

    if 'date' in car_df.columns:
        convert_date_columns(car_df, ['date'])
    else:
        print("Warning: 'date' column missing in fetched data.")
        current_time = next_time
        time.sleep(0.1)
        continue

    lap_numbers = []
    for row_time in car_df['date']:
        lap_match = laps_df[laps_df['date_start'] <= row_time].sort_values('date_start', ascending=False).head(1)
        lap_num = lap_match['lap_number'].values[0] if not lap_match.empty else None
        lap_numbers.append(lap_num)
    car_df['lap_number'] = lap_numbers

    car_df = car_df.dropna(subset=['lap_number'])
    car_df['lap_number'] = car_df['lap_number'].astype(int)

    car_df['time_from_lap_start'] = car_df.groupby('lap_number')['date'].transform(lambda x: x - x.min())

    for _, row in car_df.iterrows():
        row_time = row['date']
        lap_number = row['lap_number']

        lap_match = laps_df[laps_df['lap_number'] == lap_number]
        st_speed = lap_match['st_speed'].values[0] if not lap_match.empty else None

        pos_match = position_df[position_df['date'] <= row_time].sort_values('date', ascending=False).head(1)
        position = pos_match['position'].values[0] if not pos_match.empty else None

        if intervals_df['date'].dt.tz is None:
            intervals_df['date'] = intervals_df['date'].dt.tz_localize('UTC')

        int_match = intervals_df[intervals_df['date'] <= row_time].sort_values('date', ascending=False).head(1)
        gap_to_leader = int_match['gap_to_leader'].values[0] if not int_match.empty else None

        if not stints_df.empty:
            stint_info = stints_df[(stints_df['lap_start'] <= lap_number) & (stints_df['lap_end'] >= lap_number)]
            if not stint_info.empty:
                stint = stint_info.iloc[0]
                compound = stint.get('compound')
                tyre_age_at_start = stint.get('tyre_age_at_start')
                stint_number = stint.get('stint_number')
            else:
                compound = tyre_age_at_start = stint_number = None
        else:
            compound = tyre_age_at_start = stint_number = None

        output = {
            "date": row_time,
            "speed": row.get('speed'),
            "n_gear": row.get('n_gear'),
            "throttle": row.get('throttle'),
            "brake": row.get('brake'),
            "drs": row.get('drs'),
            "driver_name": driver_name,
            "session_name": session_name,
            "gap_to_leader": gap_to_leader,
            "lap_number": lap_number,
            "st_speed": st_speed,
            "position": position,
            "compound": compound,
            "tyre_age_at_start": tyre_age_at_start,
            "stint_number": stint_number,
            "time_from_lap_start": row['time_from_lap_start'].total_seconds()
        }

        print(output)

        clean_output = {
            k: (
                v.to_pydatetime() if isinstance(v, pd.Timestamp) else
                int(v) if isinstance(v, (np.integer,)) else
                float(v) if isinstance(v, (np.floating, timedelta)) else
                v
            )
            for k, v in output.items()
        }

        mongo_collection.insert_one(clean_output)

    current_time = next_time
    time.sleep(0.1)
