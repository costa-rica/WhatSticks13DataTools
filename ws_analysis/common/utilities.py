from .config_and_logger import config, logger_ws_analysis
import pandas as pd
from ws_models import engine
from datetime import datetime
import pytz
import os
from ws_models import engine, DatabaseSession, Users, UserLocationDay, Locations


# Function to convert date from UTC to Paris time
# def convert_to_paris_time(utc_str):
def convert_to_user_tz(utc_str, user_tz_str):
    utc_time = datetime.strptime(utc_str, '%Y-%m-%d %H:%M:%S %z')
    # paris_tz = pytz.timezone('Europe/Paris')
    user_tz = pytz.timezone(user_tz_str)
    user_time = utc_time.astimezone(user_tz)
    return user_time

# # Function to determine the dateUserTz_3pm
# # if the user's time of sleep is before 3pm we count it as the prior day's sleep
# def get_dateUserTz_3pm_obe(row):
#     if row['startDateUserTz'].time() >= pd.Timestamp('15:00:00').time():
#         return row['dateUserTz']
#     else:
#         return row['dateUserTz'] - pd.Timedelta(days=1)

# Function to determine the dateUserTz_3pm
# if the user's time of sleep is before 3pm we count it as the prior day's sleep
def get_startDate_3pm(row):
    if row['startDate'].time() >= pd.Timestamp('15:00:00').time():
        return pd.to_datetime(row['startDate_dateOnly']) + pd.Timedelta(days=1)
    else:
        return pd.to_datetime(row['startDate_dateOnly']) 

# Function to calculate the duration in hours as a float
def calculate_duration_in_hours(start, end):
    duration = end - start
    hours = duration.total_seconds() / 3600
    return hours

def create_pickle_apple_qty_cat_path_and_name(user_id_str):
    # user's existing data in pickle dataframe
    user_apple_health_dataframe_pickle_file_name = f"user_{int(user_id_str):04}_apple_health_dataframe.pkl"

    #pickle filename and path
    pickle_apple_qty_cat_path_and_name = os.path.join(config.DATAFRAME_FILES_DIR, user_apple_health_dataframe_pickle_file_name)
    return pickle_apple_qty_cat_path_and_name

def create_pickle_apple_workouts_path_and_name(user_id_str):
    # user's existing data in pickle dataframe
    user_apple_workouts_dataframe_pickle_file_name = f"user_{int(user_id_str):04}_apple_workouts_dataframe.pkl"

    #pickle filename and path
    pickle_apple_workouts_path_and_name = os.path.join(config.DATAFRAME_FILES_DIR, user_apple_workouts_dataframe_pickle_file_name)
    return pickle_apple_workouts_path_and_name


### NEW WS 11 Analysis Package ###
# def adjust_timezone(start_or_end_date, user_tz_str):
#     # Parse the date string to a datetime object
#     # Assuming the input datetime strings are naive and meant to be in UTC
#     date_naive = datetime.strptime(start_or_end_date, '%Y-%m-%d %H:%M:%S')
    
#     # Assume the naive datetime is in UTC
#     date_with_tz = date_naive.replace(tzinfo=pytz.utc)

#     # Get the target timezone from the user_tz_str
#     target_tz = pytz.timezone(user_tz_str)
    
#     # Convert the date to the target timezone
#     date_user_tz = date_with_tz.astimezone(target_tz)
    
#     # Make the datetime object timezone-naive
#     date_user_tz_naive = date_user_tz.replace(tzinfo=None)
    
#     return date_user_tz_naive


def add_timezones_from_UserLocationDay(user_id, df):
    ##############################################################################################################
    # Note this function maps the UserLocationDay timezones from each day to each corresponding row in the df
    # This function matches on the columns:
    # - df.date_utc AND
    # - query_user_loc_day_for_user_id.date_utc_user_check_in_str
    ##############################################################################################################

    db_session = DatabaseSession()

    # get timezones from UserLocationDay (backref tz_id) and update user_tz_str --- 
    # NOTE: UserLocationDay does not contain timezone it has relation to Locations table which does have timezone
    query_user_loc_day_for_user_id = db_session.query(UserLocationDay, Locations.tz_id).join(Locations).\
        filter(UserLocationDay.user_id == user_id)
    wrap_up_session(db_session)
    df_user_loc_day = pd.read_sql(query_user_loc_day_for_user_id.statement, engine)

    # Convert date_utc_user_check_in from datetime.date to string to match df['date_utc'] format
    # If date_utc_user_check_in is already datetime.date, first ensure it's converted to datetime then to string
    df_user_loc_day['date_utc_user_check_in_str'] = pd.to_datetime(df_user_loc_day['date_utc_user_check_in']).dt.strftime('%Y-%m-%d')

    ## Update timezone in df with UserLocationDay timezone -> if exits a row in UserLocationDay
    ##### -> i.e. only matching rows between df['date_utc'] and df_user_loc_day['date_utc_user_check_in_str']
    # First, ensure that the 'date_utc' column in both DataFrames is of the same data type
    # df['date_utc'] = df['date_utc'].astype(str)
    df['startDate_dateOnly'] = df['startDate_dateOnly'].astype(str)
    df_user_loc_day['date_utc_user_check_in_str'] = df_user_loc_day['date_utc_user_check_in_str'].astype(str)

    # Merge df with df_user_loc_day to only get matching 'date_utc' with 'date_utc_user_check_in_str'
    # and select the 'tz_id' for those matches.
    # temp_df = df[['date_utc']].merge(df_user_loc_day[['date_utc_user_check_in_str', 'tz_id']], 
    #                                 left_on='date_utc', 
    #                                 right_on='date_utc_user_check_in_str', 
    #                                 how='left')
    temp_df = df[['startDate_dateOnly']].merge(df_user_loc_day[['date_utc_user_check_in_str', 'tz_id']], 
                                    left_on='startDate_dateOnly', 
                                    right_on='date_utc_user_check_in_str', 
                                    how='left')

    # Ensure the index aligns with the original df for proper updating
    temp_df.index = df.index

    # Update the 'user_tz_str' column in df with the 'tz_id' values from the merge where there's a match
    df['user_tz_str'].update(temp_df['tz_id'])

    return df

def wrap_up_session(db_session):
    logger_ws_analysis.info("- accessed wrap_up_session -")
    try:
        # perform some database operations
        db_session.commit()
        logger_ws_analysis.info("- perfomed: db_session.commit() -")
    except Exception as e:
        logger_ws_analysis.info(f"{type(e).__name__}: {e}")
        db_session.rollback()  # Roll back the transaction on error
        logger_ws_analysis.info("- perfomed: db_session.rollback() -")
        raise
    finally:
        db_session.close()  # Ensure the session is closed in any case
        logger_ws_analysis.info("- perfomed: db_session.close() -")

