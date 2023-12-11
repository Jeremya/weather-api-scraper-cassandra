import requests
import logging
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import time
import uuid

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Function to get the forecast URL for a given latitude and longitude
def get_forecast_url(latitude, longitude):
    try:
        point_url = f"https://api.weather.gov/points/{latitude},{longitude}"
        response = requests.get(point_url)
        data = response.json()
        return data['properties']['forecastHourly']
    except Exception as e:
        logger.error(f"Error fetching forecast URL: {e}")
        return None

# Function to fetch hourly weather data from the National Weather Service API
def fetch_weather_data(forecast_url):
    try:
        response = requests.get(forecast_url)
        forecast_data = response.json()['properties']['periods']

        weather_data = []
        for period in forecast_data[:1]:  # Get only the latest hour data
            weather_data.append({
                "temperature": period['temperature'],
                "wind_speed": int(period['windSpeed'].split()[0]),  # Assuming format 'XX mph'
                "short_forecast": period['shortForecast'],
                "datetime": period['startTime']  # ISO format datetime string
            })
        return weather_data
    except Exception as e:
        logger.error(f"Error fetching weather data: {e}")
        return []

# Connect to Cassandra
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('weatherdata')

# Function to insert weather data into Cassandra
def insert_weather_data(city, state, country, datetime_str, temperature, wind_speed, short_forecast):
    try:
        datetime_obj = datetime.fromisoformat(datetime_str)
        entry_id = uuid.uuid1()  # Generate a time-based UUID
        query = SimpleStatement("""
            INSERT INTO weatherdata.city_weather (city, state, country, datetime, temperature, wind_speed, short_forecast, entry_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """)
        session.execute(query, (city, state, country, datetime_obj, temperature, wind_speed, short_forecast, entry_id))
        logger.info(f"Weather data inserted for {city}, {state}, {country} at {datetime_str}")
    except Exception as e:
        logger.error(f"Error inserting weather data into Cassandra: {e}")

# List of 10 biggest US cities to track
cities = [
    {"city": "Los Angeles", "state": "CA", "country": "USA", "latitude": 34.0522, "longitude": -118.2437},
    {"city": "Chicago", "state": "IL", "country": "USA", "latitude": 41.8781, "longitude": -87.6298},
    {"city": "Houston", "state": "TX", "country": "USA", "latitude": 29.7604, "longitude": -95.3698},
    {"city": "Phoenix", "state": "AZ", "country": "USA", "latitude": 33.4484, "longitude": -112.0740},
    {"city": "Philadelphia", "state": "PA", "country": "USA", "latitude": 39.9526, "longitude": -75.1652},
    {"city": "San Antonio", "state": "TX", "country": "USA", "latitude": 29.4241, "longitude": -98.4936},
    {"city": "San Diego", "state": "CA", "country": "USA", "latitude": 32.7157, "longitude": -117.1611},
    {"city": "Dallas", "state": "TX", "country": "USA", "latitude": 32.7767, "longitude": -96.7970},
    {"city": "San Jose", "state": "CA", "country": "USA", "latitude": 37.3382, "longitude": -121.8863}
]

logger.info("Weather data fetching and storage script started.")

try:
    while True:
        for city_info in cities:
            logger.info(f"Fetching weather data for {city_info['city']}, {city_info['state']}, {city_info['country']}")
            forecast_url = get_forecast_url(city_info["latitude"], city_info["longitude"])
            if forecast_url:
                weather_data = fetch_weather_data(forecast_url)
                for data_point in weather_data:
                    insert_weather_data(
                        city_info["city"],
                        city_info["state"],
                        city_info["country"],
                        data_point["datetime"],
                        data_point["temperature"],
                        data_point["wind_speed"],
                        data_point["short_forecast"]
                    )
        time.sleep(20)  # Wait for 20 seconds before next API call
except KeyboardInterrupt:
    logger.info("Weather data fetching and storage script stopped.")
    cluster.shutdown()
