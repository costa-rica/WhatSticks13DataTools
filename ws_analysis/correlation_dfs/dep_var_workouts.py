import pandas as pd;import numpy as np
from ..common.config_and_logger import config, logger_ws_analysis
import os
from ..daily_dfs.sleep_time import create_df_daily_sleep, \
    create_df_n_minus1_daily_sleep
from ..daily_dfs.steps import create_df_daily_steps, \
    create_df_n_minus1_daily_steps
from ..daily_dfs.heart_rate import create_df_daily_heart_rate, \
    create_df_n_minus1_daily_heart_rate
from ..daily_dfs.workouts import create_df_daily_workout_duration, \
    create_df_daily_workout_duration_dummies
from ..daily_dfs.weather import create_df_weather_history
from ..daily_dfs.user_location_day import create_df_daily_user_location_consecutive


#######################################
## Daily Workouts Dependent Variable ##
#######################################

#reverted timezone change
def corr_workouts_sleep(df_workouts, df_qty_cat):

    logger_ws_analysis.info("- in corr_workouts_sleep")
    user_id = df_qty_cat['user_id'].iloc[0]
    df_daily_sleep = create_df_daily_sleep(df_qty_cat)# create daily sleep
    if len(df_daily_sleep) == 0:
        return "insufficient data", "insufficient data"
    df_daily_sleep.rename(columns=({'startDate_dateOnly_sleep_adj':'startDate_dateOnly'}),inplace=True)

    df_n_minus1_daily_sleep = create_df_n_minus1_daily_sleep(user_id, df_daily_sleep)

    df_daily_workout_duration = create_df_daily_workout_duration(df_workouts)

    csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_workout_duration.csv")
    df_daily_workout_duration.to_csv(csv_path_and_filename)

    try:
        if len(df_daily_workout_duration) > 5:# arbitrary minimum

            # This will keep only the rows that have matching 'startDate_dateOnly' values in both dataframes
            df_daily_workout_duration_sleep_n_minus1 = pd.merge(df_n_minus1_daily_sleep,df_daily_workout_duration, on='startDate_dateOnly')
            df_daily_workout_duration_sleep_n_minus1['startDate_dateOnly'] = df_daily_workout_duration_sleep_n_minus1['startDate_dateOnly'].dt.strftime('%Y-%m-%d')
            # save csv file for user
            csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_workout_sleep_n_minus1.csv")
            df_daily_workout_duration_sleep_n_minus1.to_csv(csv_path_and_filename)
            # Calculate the correlation between step_count and sleep_duration
            correlation = df_daily_workout_duration_sleep_n_minus1['duration'].corr(df_daily_workout_duration_sleep_n_minus1['sleep_duration'])
            obs_count = len(df_daily_workout_duration_sleep_n_minus1)
            # logger_ws_analysis.info(f"correlation: {correlation}, corr type: {correlation}")
            logger_ws_analysis.info(f"df_daily_workout_duration_sleep_n_minus1 correlation: {correlation}, corr type: {type(correlation)}")
            return correlation, obs_count
        else:
            return "insufficient data", "insufficient data"
    except Exception as e:
        logger_ws_analysis.info(f"error in corr_workouts_sleep: {e}")
        return "insufficient data", "insufficient data"

#reverted timezone change
def corr_workouts_steps(df_workouts, df_qty_cat):

    logger_ws_analysis.info("- in corr_workouts_steps")
    user_id = df_qty_cat['user_id'].iloc[0]
    df_daily_steps = create_df_daily_steps(df_qty_cat)# create daily steps
    if len(df_daily_steps) == 0:
        return "insufficient data", "insufficient data"

    df_n_minus1_daily_steps = create_df_n_minus1_daily_steps(df_daily_steps)

    csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_n_minus1_daily_steps.csv")
    df_n_minus1_daily_steps.to_csv(csv_path_and_filename)

    df_daily_workout_duration = create_df_daily_workout_duration(df_workouts)
    # df_daily_workout_duration['startDate_dateOnly']=pd.to_datetime(df_daily_workout_duration['startDate_dateOnly'])

    csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_workout_duration.csv")
    df_daily_workout_duration.to_csv(csv_path_and_filename)

    try:
        if len(df_daily_workout_duration) > 5:# arbitrary minimum

            # This will keep only the rows that have matching 'startDate_dateOnly' values in both dataframes
            # df_daily_workout_duration_sleep_n_minus1 = pd.merge(df_n_minus1_daily_sleep,df_daily_workout_duration, on='startDate_dateOnly')
            df_daily_workout_duration_steps_n_minus1 = pd.merge(df_n_minus1_daily_steps,df_daily_workout_duration, on='startDate_dateOnly')
            df_daily_workout_duration_steps_n_minus1['startDate_dateOnly'] = df_daily_workout_duration_steps_n_minus1['startDate_dateOnly'].dt.strftime('%Y-%m-%d')
            # save csv file for user
            csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_workout_steps_n_minus1.csv")
            df_daily_workout_duration_steps_n_minus1.to_csv(csv_path_and_filename)
            logger_ws_analysis.info("--- df_daily_workout_duration_steps_n_minus1 ----")
            logger_ws_analysis.info(df_daily_workout_duration_steps_n_minus1.columns)
            logger_ws_analysis.info(df_daily_workout_duration_steps_n_minus1.head(2))
            # Calculate the correlation between step_count and sleepTimeUserTz
            # correlation = df_daily_workout_duration_sleep_n_minus1['duration'].corr(df_daily_workout_duration_sleep_n_minus1['sleepTimeUserTz'])
            correlation = df_daily_workout_duration_steps_n_minus1['duration'].corr(df_daily_workout_duration_steps_n_minus1['step_count'])
            obs_count = len(df_daily_workout_duration_steps_n_minus1)
            # logger_ws_analysis.info(f"correlation: {correlation}, corr type: {correlation}")
            logger_ws_analysis.info(f"df_daily_workout_duration_steps_n_minus1 correlation: {correlation}, corr type: {type(correlation)}")
            return correlation, obs_count
        else:
            return "insufficient data", "insufficient data"
    except Exception as e:
        logger_ws_analysis.info(f"error in corr_workouts_sleep: {e}")
        return "insufficient data", "insufficient data"

#reverted timezone change
def corr_workouts_heart_rate(df_workouts, df_qty_cat):

    logger_ws_analysis.info("- in corr_workouts_heart_rate")
    user_id = df_qty_cat['user_id'].iloc[0]
    # df_daily_steps = create_df_daily_steps(df_qty_cat)# create daily steps
    df_daily_heart_rate = create_df_daily_heart_rate(df_qty_cat)# create daily steps
    if len(df_daily_heart_rate) == 0:
        return "insufficient data", "insufficient data"
    # df_n_minus1_daily_steps = create_df_n_minus1_daily_steps(df_daily_steps)
    df_n_minus1_daily_heart_rate = create_df_n_minus1_daily_heart_rate(df_daily_heart_rate)
    # df_n_minus1_daily_heart_rate['startDate_dateOnly']=pd.to_datetime(df_n_minus1_daily_heart_rate['startDate_dateOnly'])

    csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_n_minus1_daily_heart_rate.csv")
    df_n_minus1_daily_heart_rate.to_csv(csv_path_and_filename)

    df_daily_workout_duration = create_df_daily_workout_duration(df_workouts)
    # df_daily_workout_duration['startDate_dateOnly']=pd.to_datetime(df_daily_workout_duration['startDate_dateOnly'])

    csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_workout_duration.csv")
    df_daily_workout_duration.to_csv(csv_path_and_filename)

    try:
        if len(df_daily_workout_duration) > 5:# arbitrary minimum

            # This will keep only the rows that have matching 'startDate_dateOnly' values in both dataframes
            df_daily_workout_duration_heart_rate_n_minus1 = pd.merge(df_n_minus1_daily_heart_rate,df_daily_workout_duration, on='startDate_dateOnly')
            df_daily_workout_duration_heart_rate_n_minus1['startDate_dateOnly'] = df_daily_workout_duration_heart_rate_n_minus1['startDate_dateOnly'].dt.strftime('%Y-%m-%d')
            # save csv file for user
            csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_workout_duration_heart_rate_n_minus1.csv")
            df_daily_workout_duration_heart_rate_n_minus1.to_csv(csv_path_and_filename)
            # logger_ws_analysis.info("--- df_daily_workout_duration_steps_n_minus1 ----")
            # logger_ws_analysis.info(df_daily_workout_duration_steps_n_minus1.columns)
            # logger_ws_analysis.info(df_daily_workout_duration_steps_n_minus1.head(2))
            # Calculate the correlation between step_count and sleep_duration
            # correlation = df_daily_workout_duration_sleep_n_minus1['duration'].corr(df_daily_workout_duration_sleep_n_minus1['sleep_duration'])
            correlation = df_daily_workout_duration_heart_rate_n_minus1['duration'].corr(df_daily_workout_duration_heart_rate_n_minus1['heart_rate_avg'])
            obs_count = len(df_daily_workout_duration_heart_rate_n_minus1)
            # logger_ws_analysis.info(f"correlation: {correlation}, corr type: {correlation}")
            logger_ws_analysis.info(f"df_daily_workout_duration_heart_rate_n_minus1 correlation: {correlation}, corr type: {type(correlation)}")
            return correlation, obs_count
        else:
            return "insufficient data", "insufficient data"
    except Exception as e:
        logger_ws_analysis.info(f"error in corr_workouts_sleep: {e}")
        return "insufficient data", "insufficient data"


#reverted timezone change
def corr_workouts_cloudiness(df_workouts):

    logger_ws_analysis.info("- in corr_workouts_heart_rate")
    user_id = df_workouts['user_id'].iloc[0]

    df_user_locations_day = create_df_daily_user_location_consecutive(user_id)
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

    # Step 3: create the workouts dataframe
    df_daily_workout_duration = create_df_daily_workout_duration(df_workouts)
    df_daily_workout_duration['startDate_dateOnly']=pd.to_datetime(df_daily_workout_duration['startDate_dateOnly'])
    
    df_daily_workout_duration_cloudcover = pd.merge(df_daily_cloudcover,df_daily_workout_duration, on='startDate_dateOnly')
    df_daily_workout_duration_cloudcover['startDate_dateOnly'] = df_daily_workout_duration_cloudcover['startDate_dateOnly'].dt.strftime('%Y-%m-%d')

    # save csv file for user
    csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_workout_duration_cloudcover.csv")
    df_daily_workout_duration_cloudcover.to_csv(csv_path_and_filename)

    correlation = df_daily_workout_duration_cloudcover['duration'].corr(df_daily_workout_duration_cloudcover['cloudcover'])
    obs_count = len(df_daily_workout_duration_cloudcover)
    
    return correlation, obs_count


def corr_workouts_temperature(df_workouts):

    logger_ws_analysis.info("- in corr_workouts_temperature")
    user_id = df_workouts['user_id'].iloc[0]

    df_user_locations_day = create_df_daily_user_location_consecutive(user_id)
    if len(df_user_locations_day) == 0:
        logger_ws_analysis.info("- User has no user location day ")
        return "insufficient data", "insufficient data"
    df_weather_history = create_df_weather_history()
    if len(df_weather_history) == 0:
        logger_ws_analysis.info("- NO Weather Data exists ")
        return "insufficient data", "insufficient data"

    # Step 2: Perform a left join to merge while keeping all rows from user_locations_day_df
    df_daily_temperature = pd.merge(df_user_locations_day, df_weather_history[['date', 'location_id', 'temp']],
                        on=['date', 'location_id'], how='left')
    
    df_daily_temperature.rename(columns=({'date':'startDate_dateOnly'}),inplace=True)
    df_daily_temperature['startDate_dateOnly']=pd.to_datetime(df_daily_temperature['startDate_dateOnly'])

    # Step 3: create the workouts dataframe
    df_daily_workout_duration = create_df_daily_workout_duration(df_workouts)
    df_daily_workout_duration['startDate_dateOnly']=pd.to_datetime(df_daily_workout_duration['startDate_dateOnly'])
    
    df_daily_workout_duration_temperature = pd.merge(df_daily_temperature,df_daily_workout_duration, on='startDate_dateOnly')
    df_daily_workout_duration_temperature['startDate_dateOnly'] = df_daily_workout_duration_temperature['startDate_dateOnly'].dt.strftime('%Y-%m-%d')

    # save csv file for user
    csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_workout_duration_temperature.csv")
    df_daily_workout_duration_temperature.to_csv(csv_path_and_filename)

    correlation = df_daily_workout_duration_temperature['duration'].corr(df_daily_workout_duration_temperature['temp'])
    obs_count = len(df_daily_workout_duration_temperature)
    
    return correlation, obs_count




