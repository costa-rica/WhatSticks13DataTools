import json
import requests
from datetime import datetime, timedelta
import os
import pandas as pd
import time
from ws_models import engine, DatabaseSession, Users, WeatherHistory, Locations, UserLocationDay
from ..common.config_and_logger import config, logger_ws_utilities
from ..common.utilities import wrap_up_session
from timezonefinder import TimezoneFinder
from ..scheduler.main import add_weather_history
from ..visual_crossing_api.vc_api_requests import request_visual_crossing_for_last_30days

def convert_string_to_datetime(date_string):
    # List of formats to try
    formats = ['%Y%m%d-%H%M', '%Y-%m-%d %H:%M:%S', '%Y/%m/%d']

    for fmt in formats:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue
    # If none of the formats work, return None or raise an exception
    return None

def convert_lat_lon_to_timezone_string(latitude, longitude):
    latitude = float(latitude)
    longitude = float(longitude)
    # Note: latitude and longitude must be float
    tf = TimezoneFinder()
    try:
        # Find the timezone
        timezone_str = tf.timezone_at(lat=latitude, lng=longitude)
    except Exception as e:
        logger_ws_utilities.info(f"-- Timezone threw Exception, e: {e} \n\n setting to timezone: Etc/GMT --")
        timezone_str = "Etc/GMT"

    # Check if the timezone is found
    if timezone_str:
        logger_ws_utilities.info(f"-- found timezone: {timezone_str} --")
        return timezone_str
    else:
        logger_ws_utilities.info(f"-- Timezone could not be determined, timezone_str: {timezone_str} --")
        # return "Timezone could not be determined"
        return "Etc/GMT"

def convert_lat_lon_to_city_country(latitude, longitude):
    logger_ws_utilities.info(f"- in convert_lat_lon_to_city_country")
    # url = f"https://nominatim.openstreetmap.org/reverse?lat={latitude}&lon={longitude}&format=json"
    # url = f"{current_app.config.get('NOMINATIM_API_URL')}/reverse?lat={latitude}&lon={longitude}&format=json"
    url = f"{config.NOMINATIM_API_URL}/reverse?lat={latitude}&lon={longitude}&format=json"
    logger_ws_utilities.info(f"sending request to : {url}")
    # Send the request
    response = requests.get(url, headers={"User-Agent": "What Sticks"})
    logger_ws_utilities.info(f"response.status_code: {response.status_code}")
    # Parse the JSON response
    data = response.json()

    # Extract city and country
    city = data.get('address', {}).get('city', 'Not found')
    country = data.get('address', {}).get('country', 'Not found')
    state = data.get('address', {}).get('state', 'Not found')
    boundingbox = data.get('boundingbox', 'Not found')
    lat = data.get('lat', 'Not found')
    lon = data.get('lon', 'Not found')

    location_dict = {
        "city": city, "country":country, "state":state,"boundingbox":boundingbox,
        "lat":lat,"lon":lon
    }

    return location_dict

def find_user_location(db_session, user_latitude, user_longitude) -> str:
    logger_ws_utilities.info("bp_users/utils find_user_location --> Searching for location_id")
    # db_session = DatabaseSession()

    # Query all locations from the database
    locations = db_session.query(Locations).all()
    
    user_latitude = float(user_latitude)
    user_longitude = float(user_longitude)
    for location in locations:
        print(f"Checking {location.city}")
        # Assuming boundingbox format is [min_lat, max_lat, min_lon, max_lon]
        boundingbox = location.boundingbox
        min_lat, max_lat, min_lon, max_lon = boundingbox
        
        # add buffer
        min_lat = min_lat - 0.15
        max_lat = max_lat + 0.15
        min_lon = min_lon - 0.25
        max_lon = max_lon + 0.25
        # NOTE: Buffer magnitude in kilometers:
        # - 0.15 lat is approx 16.5km
        # - 0.25 lon is approx 20km
        
        
        if location.city == "San Francisco":
            print("---------------------")
            if min_lat <= user_latitude <= max_lat:
                print("*** found latitude! *")
            else:
                print(f"min_lat: {min_lat}")
                print(f"user_latitude: {user_latitude}")
                print(f"max_lat: {max_lat}")
            if min_lon <= user_longitude <= max_lon:
                print("*** found user_longitude! *")
            else:
                print(f"min_lon: {min_lon}")
                print(f"user_longitude: {user_longitude}")
                print(f"max_lon: {max_lon}")

            print("---------------------")

        # Check if user's coordinates are within the bounding box
        if min_lat <= user_latitude <= max_lat and min_lon <= user_longitude <= max_lon:
            logger_ws_utilities.info(f"- Found coords in location: {location.city}")
            # return str(location.id)  # Return the location ID if within the bounding box
            return location.id  # Return the location ID if within the bounding box
    logger_ws_utilities.info(f"- Did NOT fined coords in location")
    # wrap_up_session(db_session)
    return "no_location_found"  # Return this if no location matches the user's coordinates

def add_user_loc_day_process(db_session, user_id, latitude, longitude, formatted_datetime_utc):
    logger_ws_utilities.info(f"- accessed:  ws_utilities -- add_user_loc_day_process - ")
    # db_session = DatabaseSession()
    logger_ws_utilities.info(f"- received db_session id: {id(db_session)} -")

    location_id = find_user_location(db_session, latitude, longitude)

    if location_id == "no_location_found":
        logger_ws_utilities.info(f"- location not found adding it to Locations Table- ")
        timezone_str = convert_lat_lon_to_timezone_string(latitude, longitude)
        location_dict = convert_lat_lon_to_city_country(latitude, longitude)

        logger_ws_utilities.info(f"- location_dict: {location_dict} -")

        city = location_dict.get('city', 'Not found')
        country = location_dict.get('country', 'Not found')
        state = location_dict.get('state', 'Not found')
        boundingbox = location_dict.get('boundingbox', 'Not found')
        boundingbox_float_afied = [float(i) for i in boundingbox]

        lat = location_dict.get('lat', latitude)
        lon = location_dict.get('lon', longitude)

        new_location_db_obj = Locations(city=city, state=state,
                        country=country, boundingbox=boundingbox_float_afied,
                        lat=lat, lon=lon,
                        tz_id=timezone_str)
        
        db_session.add(new_location_db_obj)
        # wrap_up_session(db_session)
        db_session.flush()
        location_id = new_location_db_obj.id

        response_30day_hist = request_visual_crossing_for_last_30days(new_location_db_obj)
        # for day in response_30dayHist.json().get('days'):
        # add_weather_history(db_session, location.id, response_30day_hist)
        add_weather_history(db_session, new_location_db_obj.id, response_30day_hist)

        logger_ws_utilities.info(f"- successfully added 30 days for new_location_db_obj: {new_location_db_obj.id} - {new_location_db_obj.city} ")

    else:
        logger_ws_utilities.info(f"- location_id found in Locations Table --- > {location_id} - ")
    
    datetime_utc_obj = convert_string_to_datetime(formatted_datetime_utc)
    new_user_location_day = UserLocationDay(user_id=user_id,location_id=location_id,date_time_utc_user_check_in=datetime_utc_obj,
                                            date_utc_user_check_in=datetime_utc_obj)

    logger_ws_utilities.info(f"- STEP HERE: new_user_location_day: {new_user_location_day} - ")

    # Convert datetime object to date object
    date_only_obj = datetime_utc_obj.date()
    
    user_id_AND_date_utc_user_check_in_Exists = db_session.query(UserLocationDay).filter_by(user_id=user_id, date_utc_user_check_in=date_only_obj).first()
    logger_ws_utilities.info(f"what is user_id_AND_date_utc_user_check_in_Exists: {user_id_AND_date_utc_user_check_in_Exists}")
    if user_id_AND_date_utc_user_check_in_Exists is None:
        # db_session = DatabaseSession()
        db_session.add(new_user_location_day)
        # wrap_up_session(db_session)
    else:
        logger_ws_utilities.info("User already has an entry for this day")
    
    
    return location_id


# def add_weather_history_30_days(db_session, location_id):
#     # logger_ws_utilities.info("- accessed: ws_utlitiles/api/users.py add_weather_history_30_days ")
#     # # db_session = DatabaseSession()
#     # logger_ws_utilities.info(f"- received db_session id: {id(db_session)} -")
#     # logger_ws_utilities.info(f"- searching location_id: {location_id} -")

#     # location = db_session.get(Locations, location_id)

#     # api_token = config.VISUAL_CROSSING_TOKEN
#     # vc_base_url = config.VISUAL_CROSSING_BASE_URL

#     # # Calculate the date range for the last 30 days
#     # end_date = datetime.now()
#     # start_date = end_date - timedelta(days=30)

#     # # Format dates in YYYY-MM-DD format
#     # start_date_str = start_date.strftime('%Y-%m-%d')
#     # end_date_str = end_date.strftime('%Y-%m-%d')

#     # api_url = f"{vc_base_url}/{location.lat},{location.lon}/{start_date_str}/{end_date_str}"
#     # params = {
#     #     'unitGroup': 'us',  # or 'metric' based on your preference
#     #     'key': api_token,
#     #     'include': 'days'  # Adjust based on the details you need
#     # }

#     # # Make the API request
#     # response_30dayHist = requests.get(api_url, params=params)

#     # for day in response_30dayHist.json().get('days'):
#     add_weather_history(db_session, location.id, response_30dayHist.json())

#     logger_ws_utilities.info(f"- successfully added 30 days for location: {location.id} - {location.city} ")
#     # wrap_up_session(db_session)

