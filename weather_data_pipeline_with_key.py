import requests
import pandas as pd
import sqlite3
from collections import Counter

# List of cities in Germany to fetch weather data for
cities = ["Berlin", "Munich", "Hamburg", "Cologne", "Frankfurt", "Dresden", "Dortmund"]

# Step 1: Fetch Data for Each City
def fetch_data_for_city(city):
    try:
        # Fetch current weather
        url_current = "https://weatherapi-com.p.rapidapi.com/current.json"
        querystring_current = {"q": city}
        headers = {
            "x-rapidapi-key": "07eb7a0222msha97703020e46948p197074jsnb776b55d3751",
            "x-rapidapi-host": "weatherapi-com.p.rapidapi.com"
        }
        response_current = requests.get(url_current, headers=headers, params=querystring_current)
        response_current.raise_for_status()
        current_data = response_current.json()
        
        # Fetch forecast data
        url_forecast = "https://weatherapi-com.p.rapidapi.com/forecast.json"
        querystring_forecast = {"q": city, "days": "2"}  # Fetch for the next 2 days
        response_forecast = requests.get(url_forecast, headers=headers, params=querystring_forecast)
        response_forecast.raise_for_status()
        forecast_data = response_forecast.json()

        return current_data, forecast_data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {city}: {e}")
        return None, None

# Step 2: Process and Transform Data
def process_data(current_data, forecast_data):
    if current_data is None or forecast_data is None:
        return None

    try:
        # Extract current weather data
        location = current_data["location"]["name"]
        region = current_data["location"]["region"]
        country = current_data["location"]["country"]
        temp_c = current_data["current"]["temp_c"]
        condition = current_data["current"]["condition"]["text"]
        wind_kph = current_data["current"]["wind_kph"]
        humidity = current_data["current"]["humidity"]
        feelslike_c = current_data["current"]["feelslike_c"]
        last_updated = current_data["current"]["last_updated"]

        # Extract forecast data for the next day
        forecast_day = forecast_data["forecast"]["forecastday"][1]

        max_temp_c = forecast_day["day"]["maxtemp_c"]
        min_temp_c = forecast_day["day"]["mintemp_c"]
        avg_temp_c = forecast_day["day"]["avgtemp_c"]
        avg_wind_kph = forecast_day["day"]["maxwind_kph"]
        avg_humidity = forecast_day["day"]["avghumidity"]
        avg_feelslike_c = round(sum(hour["feelslike_c"] for hour in forecast_day["hour"]) / len(forecast_day["hour"]), 1)
        
        # Find the times of max and min temperatures
        max_temp_time = min_temp_time = None
        for hour in forecast_day["hour"]:
            if hour["temp_c"] == max_temp_c:
                max_temp_time = hour["time"]
            if hour["temp_c"] == min_temp_c:
                min_temp_time = hour["time"]

        # Get the most frequent condition during the day and the time range it covers
        conditions = [hour["condition"]["text"] for hour in forecast_day["hour"]]
        most_common_condition = Counter(conditions).most_common(1)[0][0]

        # Get time range for the most common condition
        times_for_most_common_condition = [hour["time"] for hour in forecast_day["hour"] if hour["condition"]["text"] == most_common_condition]
        most_common_condition_time_range = f"{times_for_most_common_condition[0]} to {times_for_most_common_condition[-1]}"

        # Concatenate the most common condition with its time range
        most_common_condition_with_time_range = f"{most_common_condition} from {times_for_most_common_condition[0]} to {times_for_most_common_condition[-1]}"

        # Create a dictionary of the extracted data
        data = {
            "location": location,
            "region": region,
            "country": country,
            "current_temp_c": temp_c,
            "current_condition": condition,
            "current_wind_kph": wind_kph,
            "current_humidity": humidity,
            "current_feelslike_c": feelslike_c,
            "last_updated": last_updated,
            "forecast_max_temp_c": max_temp_c,
            "forecast_max_temp_time": max_temp_time,
            "forecast_min_temp_c": min_temp_c,
            "forecast_min_temp_time": min_temp_time,
            "forecast_avg_temp_c": avg_temp_c,
            "forecast_avg_wind_kph": avg_wind_kph,
            "forecast_avg_humidity": avg_humidity,
            "forecast_avg_feelslike_c": avg_feelslike_c,
            "most_common_condition_with_time_range": most_common_condition_with_time_range
        }

        return data
    except KeyError as e:
        print(f"Error processing data: Missing key {e}")
        return None

# Step 3: Load Data into SQLite Database
def load_to_db(data_list, db_name="germany_weather.db"):
    if not data_list:
        print("No data to load into the database.")
        return

    try:
        # Connect to SQLite database (or create it)
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Drop the table if it already exists to avoid schema mismatch
        cursor.execute('DROP TABLE IF EXISTS weather_data')

        # Create the table with the correct schema
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location TEXT,
            region TEXT,
            country TEXT,
            current_temp_c REAL,
            current_condition TEXT,
            current_wind_kph REAL,
            current_humidity INTEGER,
            current_feelslike_c REAL,
            last_updated TEXT,
            forecast_max_temp_c REAL,
            forecast_max_temp_time TEXT,
            forecast_min_temp_c REAL,
            forecast_min_temp_time TEXT,
            forecast_avg_temp_c REAL,
            forecast_avg_wind_kph REAL,
            forecast_avg_humidity REAL,
            forecast_avg_feelslike_c REAL,
            most_common_condition_with_time_range TEXT
        )
        ''')

        # Insert data into the table
        df = pd.DataFrame(data_list)
        df.to_sql('weather_data', conn, if_exists='append', index=False)

        # Commit and close connection
        conn.commit()
        conn.close()
        print("Data successfully loaded into the database.")
    except sqlite3.Error as e:
        print(f"Database error: {e}")

# Step 4: Execute ETL Process
def etl_process():
    data_list = []

    for city in cities:
        current_data, forecast_data = fetch_data_for_city(city)
        city_data = process_data(current_data, forecast_data)
        if city_data:
            data_list.append(city_data)

    load_to_db(data_list)

# Run the ETL process
etl_process()
