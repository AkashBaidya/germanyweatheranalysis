
# 🌤️ Germany Weather Data Engineering Project

This project is a data engineering pipeline that fetches weather data for multiple cities in Germany, processes the data to extract key insights, and stores the results in a SQLite database. The focus is on gathering the most frequent weather condition during the forecasted day and its associated time range.

## 🚀 Project Overview

### 🛠️ Features
- **Fetch Current and Forecast Weather Data:** Get the current weather and the next day's forecast for multiple German cities.
- **Data Processing:** Calculate key metrics like max/min temperature, average temperature, wind speed, humidity, and feels-like temperature.
- **Identify Dominant Weather Condition:** Determine the most frequent weather condition for the day and the time range it covers.
- **Store Data in SQLite Database:** The processed data is stored in a SQLite database for easy access and analysis.

### 📋 Requirements

To run this project, you'll need:
- Python 3.7+
- The following Python packages:
  - `requests`
  - `pandas`
  - `sqlite3` (comes with Python by default)

Install the required packages with:
```bash
pip install requests pandas
```

### 🏗️ Project Structure

- **`fetch_data_for_city(city):`** Fetches current and forecasted weather data for a specified city.
- **`process_data(current_data, forecast_data):`** Processes the raw data, extracts key metrics, and identifies the most frequent weather condition along with its time range.
- **`load_to_db(data_list, db_name="germany_weather.db"):`** Loads the processed data into a SQLite database.
- **`etl_process():`** Orchestrates the ETL (Extract, Transform, Load) process for multiple cities.

## 🛠️ Setup Instructions

### 1. 🧑‍💻 Clone the Repository
First, clone the repository to your local machine:
```bash
git clone https://github.com/yourusername/germany-weather-data-engineering.git
cd germany-weather-data-engineering
```

### 2. 🐍 Set Up the Environment
Create and activate a Python virtual environment (optional but recommended):
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows use `venv\Scriptsctivate`
```

Install the required Python packages:
```bash
pip install requests pandas
```

### 3. 🚀 Run the ETL Process
Execute the ETL process to fetch, process, and store the weather data:
```bash
python etl_process.py
```

### 4. 🗄️ Explore the Database
After running the script, a SQLite database named `germany_weather.db` will be created. You can explore it using any SQLite database viewer (e.g., DB Browser for SQLite).

### 5. 📊 Analyze the Data
The database contains the following columns:
- **`location:`** The city name.
- **`region:`** The region or state within Germany.
- **`country:`** The country (Germany).
- **`current_temp_c:`** Current temperature in Celsius.
- **`current_condition:`** Current weather condition.
- **`current_wind_kph:`** Current wind speed in kilometers per hour.
- **`current_humidity:`** Current humidity percentage.
- **`current_feelslike_c:`** Current feels-like temperature in Celsius.
- **`last_updated:`** Last updated time for the current weather data.
- **`forecast_max_temp_c:`** Maximum forecasted temperature for the next day.
- **`forecast_max_temp_time:`** Time at which the maximum temperature is expected.
- **`forecast_min_temp_c:`** Minimum forecasted temperature for the next day.
- **`forecast_min_temp_time:`** Time at which the minimum temperature is expected.
- **`forecast_avg_temp_c:`** Average forecasted temperature for the next day.
- **`forecast_avg_wind_kph:`** Average wind speed for the next day.
- **`forecast_avg_humidity:`** Average humidity for the next day.
- **`forecast_avg_feelslike_c:`** Average feels-like temperature for the next day.
- **`most_common_condition_with_time_range:`** The most frequent weather condition during the forecasted day, along with the time range it will occur.

## 🎉 Conclusion

This project provides a streamlined approach to gather, process, and store weather data for German cities. It's an excellent starting point for building more complex data pipelines or conducting further analysis on weather patterns. Feel free to extend the project with additional cities, metrics, or even integrate it with more advanced databases or cloud storage solutions!

### 🤝 Contributing
If you find any issues or have suggestions for improvements, feel free to open an issue or submit a pull request. Contributions are welcome!

### 📄 License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

*Happy Coding!* ✨
