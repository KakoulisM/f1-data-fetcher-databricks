import requests
import pandas as pd

def fetch_json(url):
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

sessions_url = "https://api.openf1.org/v1/sessions"
sessions_data = fetch_json(sessions_url)

if sessions_data:
    df = pd.DataFrame(sessions_data)

    df = df[[
        'session_key',
        'session_name',
        'circuit_short_name',
        'country_name',
        'date_start'
    ]].rename(columns={
        'session_key': 'session_id',
        'session_name': 'session_type',
        'circuit_short_name': 'grand_prix',
        'date_start': 'start_time'
    })

    df = df.drop_duplicates(subset=['session_id']).sort_values(by='start_time', ascending=False)

    df['start_time'] = pd.to_datetime(df['start_time'], utc=True, errors='coerce')

    display(df)
else:
    print("No session data available.")
