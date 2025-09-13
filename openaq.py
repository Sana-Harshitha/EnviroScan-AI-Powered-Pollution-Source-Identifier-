import pandas as pd
from openaq_utils.openaq_getvalues import get_values

# List of locations (lat, lon)
locations = [
    (28.6139, 77.2090),   # Delhi
    (19.0760, 72.8777),   # Mumbai
    (12.9716, 77.5946),   # Bangalore
]

# Fetch values for each location
rows = []
for lat, lon in locations:
    row = get_values(lat, lon)
    if row:
        rows.append(row)

# Convert to DataFrame
df = pd.DataFrame(rows)
print(df)
