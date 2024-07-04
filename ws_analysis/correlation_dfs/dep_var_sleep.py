import pandas as pd;import numpy as np
from ..common.config_and_logger import config, logger_ws_analysis
import os
from ..daily_dfs.sleep_time import create_df_daily_sleep
    
from ..daily_dfs.steps import create_df_daily_steps
from ..daily_dfs.heart_rate import create_df_daily_heart_rate
from ..daily_dfs.workouts import create_df_daily_workout_duration, \
    create_df_daily_workout_duration_dummies
from ..common.create_user_df import create_user_location_date_df
from ..daily_dfs.weather import create_df_weather_history
from ..daily_dfs.user_location_day import create_df_daily_user_location_consecutive

### NOTE: this would cause an circular reference error:
# from ws_utilities import create_df_from_db_table_name

#############################################################################################################################
# NOTE: Since these functions are never used here ALL sleep correlations are assuming sleep is the depenedent variable.
# i.e. things affect sleep, not sleep affecting heart rate, steps, etc.,
# create_df_n_minus1_daily_sleep
# create_df_n_minus1_daily_steps
# create_df_n_minus1_daily_heart_rate
#############################################################################################################################

#sleep changed
# df here would come from create_user_df create_user_qty_cat_df
def corr_sleep_steps(df):
    logger_ws_analysis.info("- in corr_sleep_steps")
    user_id = df['user_id'].iloc[0]

    df_daily_sleep = create_df_daily_sleep(df)
    if len(df_daily_sleep) == 0:
        return "insufficient data", "insufficient data"
    # df_daily_sleep.rename(columns=({'dateUserTz_3pm':'dateUserTz'}),inplace=True)
    df_daily_sleep.rename(columns=({'startDate_dateOnly_sleep_adj':'startDate_dateOnly'}),inplace=True)

    # if 'HKCategoryTypeIdentifierSleepAnalysis' in list_of_user_data:
    df_daily_steps = create_df_daily_steps(df)
    try:
        if len(df_daily_steps) > 5:# arbitrary minimum

            # This will keep only the rows that have matching 'startDate_dateOnly' values in both dataframes
            df_daily_sleep_steps = pd.merge(df_daily_sleep,df_daily_steps, on='startDate_dateOnly')


            # save csv file for user
            csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_sleep_steps.csv")
            df_daily_sleep_steps.to_csv(csv_path_and_filename)
            # Calculate the correlation between step_count and sleep_duration
            correlation = df_daily_sleep_steps['step_count'].corr(df_daily_sleep_steps['sleep_duration'])
            obs_count = len(df_daily_sleep_steps)
            # logger_ws_analysis.info(f"correlation: {correlation}, corr type: {correlation}")
            logger_ws_analysis.info(f"df_daily_sleep_steps correlation: {correlation}, corr type: {type(correlation)}")
            return correlation, obs_count
        else:
            return "insufficient data", "insufficient data"
    except Exception as e:
        logger_ws_analysis.info(f"error in corr_sleep_steps: {e}")
        return "insufficient data", "insufficient data"
#sleep changed
def corr_sleep_heart_rate(df):
    logger_ws_analysis.info("- in corr_sleep_heart_rate")
    user_id = df['user_id'].iloc[0]

    df_daily_sleep = create_df_daily_sleep(df)
    if len(df_daily_sleep) == 0:
        return "insufficient data", "insufficient data"
    # df_daily_sleep.rename(columns=({'dateUserTz_3pm':'dateUserTz'}),inplace=True)
    df_daily_sleep.rename(columns=({'startDate_dateOnly_sleep_adj':'startDate_dateOnly'}),inplace=True)

    df_daily_heart_rate = create_df_daily_heart_rate(df)

    try:
        logger_ws_analysis.info("- try corr_sleep_heart_rate")
        if len(df_daily_heart_rate) > 5:# arbitrary minimum
            logger_ws_analysis.info("- if len(df_daily_heart_rate) > 5")

            # This will keep only the rows that have matching 'startDate_dateOnly' values in both dataframes
            df_daily_sleep_heart_rate = pd.merge(df_daily_sleep,df_daily_heart_rate, on='startDate_dateOnly')

            # save csv file for user
            csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_sleep_heart_rate.csv")
            df_daily_sleep_heart_rate.to_csv(csv_path_and_filename)

            # Calculate the correlation between step_count and sleep_duration
            correlation = df_daily_sleep_heart_rate['heart_rate_avg'].corr(df_daily_sleep_heart_rate['sleep_duration'])
            obs_count = len(df_daily_sleep_heart_rate)
            logger_ws_analysis.info(f"df_daily_sleep_heart_rate correlation: {correlation}, corr type: {type(correlation)}")
            return correlation, obs_count
        else:
            return "insufficient data", "insufficient data"
    except Exception as e:
        logger_ws_analysis.info(f"error in corr_sleep_heart_rate: {e}")
        return "insufficient data", "insufficient data"
#sleep changed
def corr_sleep_workouts(df_qty_cat, df_workouts):

    logger_ws_analysis.info("- in corr_sleep_workouts")
    user_id = df_qty_cat['user_id'].iloc[0]
    df_daily_sleep = create_df_daily_sleep(df_qty_cat)
    if len(df_daily_sleep) == 0:
        return "insufficient data", "insufficient data"
    df_daily_sleep.rename(columns=({'startDate_dateOnly_sleep_adj':'startDate_dateOnly'}),inplace=True)

    df_daily_workout_duration = create_df_daily_workout_duration(df_workouts)
    # df_daily_workout_duration_csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_workout_duration.csv")
    # df_daily_workout_duration.to_csv(df_daily_workout_duration_csv_path_and_filename)
    try:
        if len(df_daily_workout_duration) > 5:# arbitrary minimum

            # This will keep only the rows that have matching 'startDate_dateOnly' values in both dataframes
            df_daily_sleep_workout_duration = pd.merge(df_daily_sleep,df_daily_workout_duration, on='startDate_dateOnly')
            # # save csv file for user
            csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_sleep_workout_duration.csv")
            df_daily_sleep_workout_duration.to_csv(csv_path_and_filename)
            # Calculate the correlation between step_count and sleep_duration
            correlation = df_daily_sleep_workout_duration['duration'].corr(df_daily_sleep_workout_duration['sleep_duration'])
            obs_count = len(df_daily_sleep_workout_duration)
            # logger_ws_analysis.info(f"correlation: {correlation}, corr type: {correlation}")
            logger_ws_analysis.info(f"df_daily_sleep_workout_duration correlation: {correlation}, corr type: {type(correlation)}")
            return correlation, obs_count
        else:
            return "insufficient data", "insufficient data"
    except Exception as e:
        logger_ws_analysis.info(f"error in corr_sleep_workouts: {e}")
        return "insufficient data", "insufficient data"
#sleep changed
def corr_sleep_workout_dummies(df_qty_cat, df_workouts):

    logger_ws_analysis.info("- in corr_sleep_workout_dummies")
    user_id = df_qty_cat['user_id'].iloc[0]
    df_daily_sleep = create_df_daily_sleep(df_qty_cat)
    if len(df_daily_sleep) == 0:
        return "insufficient data", "insufficient data"
    df_daily_sleep.rename(columns=({'startDate_dateOnly_sleep_adj':'startDate_dateOnly'}),inplace=True)

    df_daily_workout_duration_dummies = create_df_daily_workout_duration_dummies(df_workouts)
    try:
        if len(df_daily_workout_duration_dummies) > 5:# arbitrary minimum
            # This will keep only the rows that have matching 'startDate_dateOnly' values in both dataframes
            df_daily_sleep_workout_duration = pd.merge(df_daily_sleep,df_daily_workout_duration_dummies, on='startDate_dateOnly')
            # save csv file for user
            csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_sleep_workout_duration_dummies.csv")
            df_daily_sleep_workout_duration.to_csv(csv_path_and_filename)

            # List to store the tuples of column name and correlation
            col_names_and_correlations_tuple_list = []

            # Iterate over the columns to calculate correlation
            for col in df_daily_sleep_workout_duration.columns:
                if col.startswith('dur_') and col.endswith('_dummy'):
                    # Calculate the correlation
                    corr_value = df_daily_sleep_workout_duration['sleep_duration'].corr(df_daily_sleep_workout_duration[col])
                    
                    # Append the tuple (column name, correlation value) to the list
                    col_names_and_correlations_tuple_list.append((col, corr_value))

            obs_count = len(df_daily_sleep_workout_duration)

            return col_names_and_correlations_tuple_list, obs_count
        else:
            return "insufficient data", "insufficient data"
    except Exception as e:
        logger_ws_analysis.info(f"error in corr_sleep_workouts: {e}")
        return "insufficient data", "insufficient data"
#sleep changed
# def corr_sleep_cloudiness(df_qty_cat, df_user_locations_day, df_weather_history):
def corr_sleep_cloudiness(df_qty_cat):
    logger_ws_analysis.info("- in corr_sleep_cloudiness")
    user_id = df_qty_cat['user_id'].iloc[0]

    df_sleep_time = create_df_daily_sleep(df_qty_cat)
    df_sleep_time.rename(columns=({'startDate_dateOnly_sleep_adj':'startDate_dateOnly'}),inplace=True)

    # df_user_locations_day = create_user_location_date_df(user_id)
    df_user_locations_day = create_df_daily_user_location_consecutive(user_id)
    # logger_ws_analysis.info(f"---------> df_user_locations_day columns: ")
    # logger_ws_analysis.info(f"{df_user_locations_day.columns}")
    # logger_ws_analysis.info(f"---------> df_user_locations_day columns: ")
    # logger_ws_analysis.info(f"---------> *********************** ")
    if len(df_user_locations_day) == 0:
        logger_ws_analysis.info("- User has no user location day ")
        return "insufficient data", "insufficient data"
    df_weather_history = create_df_weather_history()
    if len(df_weather_history) == 0:
        logger_ws_analysis.info("- NO Weather Data exists ")
        return "insufficient data", "insufficient data"
    # Step 2: Perform a left join to merge while keeping all rows from user_locations_day_df
    df_daily_cloudcover = pd.merge(df_user_locations_day, df_weather_history[['date', 'location_id', 'cloudcover']],
                        on=['date', 'location_id'], how='left')
    
    df_daily_cloudcover.rename(columns=({'date':'startDate_dateOnly'}),inplace=True)
    df_daily_cloudcover['startDate_dateOnly']=pd.to_datetime(df_daily_cloudcover['startDate_dateOnly'])

    df_daily_sleep_time_cloudcover = pd.merge(df_sleep_time, df_daily_cloudcover[['startDate_dateOnly','cloudcover']],
                                        on=['startDate_dateOnly'],how='left')

    df_daily_sleep_time_cloudcover.dropna(inplace=True)
    df_daily_sleep_time_cloudcover.reset_index(inplace=True)
    df_daily_sleep_time_cloudcover.drop(columns=['index'], inplace=True)
    obs_count = len(df_daily_sleep_time_cloudcover)
    logger_ws_analysis.info(f"- obs_count: {obs_count}")
    try:
        # if obs_count > 5:# arbitrary minimum

        # save csv file for user
        csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_sleep_time_cloudcover.csv")
        df_daily_sleep_time_cloudcover.to_csv(csv_path_and_filename)

        correlation = df_daily_sleep_time_cloudcover['cloudcover'].corr(df_daily_sleep_time_cloudcover['sleep_duration'])
        logger_ws_analysis.info(f"- correlation: {correlation}")
        return correlation, obs_count
        # else:
        #     return "insufficient data", "insufficient data"
    except Exception as e:
        logger_ws_analysis.info(f"error in corr_sleep_heart_rate: {e}")
        return "insufficient data", "insufficient data"
#sleep changed
def corr_sleep_temperature(df_qty_cat):
    logger_ws_analysis.info("- in corr_sleep_temperature")
    user_id = df_qty_cat['user_id'].iloc[0]
    df_sleep_time = create_df_daily_sleep(df_qty_cat)
    df_sleep_time.rename(columns=({'startDate_dateOnly_sleep_adj':'startDate_dateOnly'}),inplace=True)
    df_user_locations_day = create_df_daily_user_location_consecutive(user_id)
    df_weather_history = create_df_weather_history()

    # Step 2: Perform a left join to merge while keeping all rows from user_locations_day_df
    df_daily_temperature = pd.merge(df_user_locations_day, df_weather_history[['date', 'location_id', 'temp']],
                    on=['date', 'location_id'], how='left')
    
    df_daily_temperature.rename(columns=({'date':'startDate_dateOnly'}),inplace=True)
    df_daily_temperature['startDate_dateOnly']=pd.to_datetime(df_daily_temperature['startDate_dateOnly'])

    df_daily_sleep_time_temperature = pd.merge(df_sleep_time, df_daily_temperature[['startDate_dateOnly','temp']],
                                        on=['startDate_dateOnly'],how='left')

    df_daily_sleep_time_temperature.dropna(inplace=True)
    df_daily_sleep_time_temperature.reset_index(inplace=True)
    df_daily_sleep_time_temperature.drop(columns=['index'], inplace=True)
    obs_count = len(df_daily_sleep_time_temperature)
    logger_ws_analysis.info(f"- obs_count: {obs_count}")
    try:
        # save csv file for user
        csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_sleep_time_temperature.csv")
        df_daily_sleep_time_temperature.to_csv(csv_path_and_filename)

        correlation = df_daily_sleep_time_temperature['temp'].corr(df_daily_sleep_time_temperature['sleep_duration'])
        logger_ws_analysis.info(f"- correlation: {correlation}")
        return correlation, obs_count

    except Exception as e:
        logger_ws_analysis.info(f"error in corr_sleep_heart_rate: {e}")
        return "insufficient data", "insufficient data"

