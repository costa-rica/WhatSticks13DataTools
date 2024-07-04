# from ws_models import DatabaseSession, Users, WeatherHistory, Locations, UserLocationDay
from ..common.config_and_logger import config, logger_ws_utilities
import json
import requests
from datetime import datetime, timedelta

# def request_visual_crossing_yesterday_weather(location_db_obj, date_1_start):
def request_visual_crossing_for_one_day(location_db_obj, date_1_start):
    logger_ws_utilities.info(f"-- in call_visual_crossing_yesterday_weather")
    logger_ws_utilities.info(f"- searching location_id: {location_db_obj.id}, city: {location_db_obj.city}, date: {date_1_start} -")

    unitgroup = config.VISUAL_CROSSING_UNIT_GROUP
    api_token = config.VISUAL_CROSSING_TOKEN
    vc_base_url = config.VISUAL_CROSSING_BASE_URL
    lat = location_db_obj.lat
    lon = location_db_obj.lon

    vc_weather_history_api_call_url = f"{vc_base_url}/{str(lat)},{str(lon)}/{date_1_start}?unitGroup={unitgroup}&key={api_token}&include=days"
    request_vc_weather_history = requests.get(vc_weather_history_api_call_url)

    if request_vc_weather_history.status_code == 200:
        # weather_data = request_vc_weather_history.json()
        logger_ws_utilities.info(f"- Successfully requested Weather History for: location.id: {location_db_obj.id} for date: {date_1_start} -")
        return request_vc_weather_history.json()
    else:
        return {}
    

# def request_visual_crossing_30days_weather(location_db_obj):
def request_visual_crossing_for_last_30days(location_db_obj):
    logger_ws_utilities.info(f"-- in request_visual_crossing_for_last_30days")

    logger_ws_utilities.info(f"- searching location_id: {location_db_obj.id}, city: {location_db_obj.city} -")

    # location = db_session.get(Locations, location_id)

    vc_base_url = config.VISUAL_CROSSING_BASE_URL

    # Calculate the date range for the last 30 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    # Format dates in YYYY-MM-DD format
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    api_url = f"{vc_base_url}/{location_db_obj.lat},{location_db_obj.lon}/{start_date_str}/{end_date_str}"
    params = {
        'unitGroup': config.VISUAL_CROSSING_UNIT_GROUP,  # or 'metric' based on your preference
        'key': config.VISUAL_CROSSING_TOKEN,
        'include': 'days'  # Adjust based on the details you need
    }

    # Make the API request
    request_vc_weather_30day_history = requests.get(api_url, params=params)
    if request_vc_weather_30day_history.status_code == 200:
        weather_data = request_vc_weather_30day_history.json()
        long_f_string = (
            f"- Successfully request for 30 day Weather History for: location.id:" +
            f" {location_db_obj.id} for start_date_str: {start_date_str} to end_date_str: {end_date_str} -"
        )
        logger_ws_utilities.info(long_f_string)
        return request_vc_weather_30day_history.json()
    else:
        return {}