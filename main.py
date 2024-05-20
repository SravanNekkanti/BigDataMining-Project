import json
import requests
import pandas as pd
from haversine import haversine, Unit
from datetime import datetime
import pytz  # For timezone conversion

# Load dataset
accidents_df = pd.read_csv('/Users/francispagulayan/Desktop/Book1.csv')
with open('/Users/francispagulayan/Downloads/response_1700772760284.json', 'r') as file:
    data = json.load(file)

# Extract station data
stations = []
for feature in data['features']:
    station_id = feature['properties']['stationIdentifier']
    latitude = feature['geometry']['coordinates'][1]
    longitude = feature['geometry']['coordinates'][0]
    stations.append({'id': station_id, 'latitude': latitude, 'longitude': longitude})


# Function to find the nearest weather station
def find_nearest_station(lat, lon, stations):
    min_distance = float('inf')
    nearest_station_id = None
    for station in stations:
        distance = haversine((lat, lon), (station['latitude'], station['longitude']), unit=Unit.MILES)
        if distance < min_distance:
            min_distance = distance
            nearest_station_id = station['id']
    return nearest_station_id



# Function to fetch weather data from the NWS API
def fetch_weather_data(station_id, date_time):
    try:
        local_time = pytz.timezone('America/New_York').localize(datetime.strptime(date_time, '%m/%d/%y %H:%M'))
        # Convert to UTC and format properly
        utc_time = local_time.astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%S') + 'Z'  # Add 'Z' to indicate UTC
    except ValueError as e:
        print(f"Error parsing date: {date_time}. Error: {e}")
        return None

    url = f"https://api.weather.gov/stations/{station_id}/observations/{utc_time}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            properties = data.get('properties', {})
            temperature = properties.get('temperature', {}).get('value', None)
            return temperature
        else:
            print(f"API request failed. Status Code: {response.status_code}, URL: {url}")
            return None
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None



# Iterate through the DataFrame
for index, row in accidents_df.iterrows():
    # Check if the temperature data is missing
    if pd.isna(row['Temperature(F)']):
        nearest_station_id = find_nearest_station(row['Start_Lat'], row['Start_Lng'], stations)
        temperature = fetch_weather_data(nearest_station_id, row['Start_Time'])
        if temperature is not None:
            accidents_df.at[index, 'Temperature(F)'] = temperature

# Save the updated dataset
accidents_df.to_csv('/Users/francispagulayan/Desktop/untitled folder/updated_accident_data.csv', index=False)
