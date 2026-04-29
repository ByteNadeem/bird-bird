import sqlite3
import pandas as pd
from datetime import datetime
import os
from meteostat import daily, stations, Point

db_path = 'backend/database/migration.db'
output_path = 'docs/climate_daily_story5_1.csv'

# 1) Read data from migration.db
conn = sqlite3.connect(db_path)
query = "SELECT MIN(date(event_timestamp)), MAX(date(event_timestamp)), AVG(latitude), AVG(longitude) FROM observations"
date_min_str, date_max_str, avg_lat, avg_lon = conn.execute(query).fetchone()
conn.close()

date_min = datetime.strptime(date_min_str, '%Y-%m-%d')
date_max = datetime.strptime(date_max_str, '%Y-%m-%d')

# 2) Fetch weather using Meteostat
location = Point(avg_lat, avg_lon)
nearby = stations.nearby(location).head(1)
if nearby.empty:
    print("No nearby Meteostat station found.")
    raise SystemExit(1)

station_id = nearby.index[0]
station_name = nearby.iloc[0]["name"]

df = daily(station_id, date_min, date_max).fetch()

if df is None or df.empty:
    print("No data found.")
    exit(1)

# 3) Build CSV columns
df = df.reset_index()[["time", "temp", "prcp"]]
df.columns = ['date', 'tmean_c', 'precip_mm']
df['date'] = df['date'].dt.strftime('%Y-%m-%d')

# 4) Save to CSV
os.makedirs(os.path.dirname(output_path), exist_ok=True)
df.to_csv(output_path, index=False)

# 5) Print diagnostics
rows = len(df)
tmean_cov = df['tmean_c'].notnull().mean()
precip_cov = df['precip_mm'].notnull().mean()

print(f"Source: Point({avg_lat}, {avg_lon})")
print(f"Station: {station_id} ({station_name})")
print(f"Rows: {rows}")
print(f"Date Range: {date_min_str} to {date_max_str}")
print(f"tmean_c coverage: {tmean_cov:.2%}")
print(f"precip_mm coverage: {precip_cov:.2%}")
