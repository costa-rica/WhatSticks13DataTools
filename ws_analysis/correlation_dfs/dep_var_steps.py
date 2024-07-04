import pandas as pd;import numpy as np
from ..common.config_and_logger import config, logger_ws_analysis
import os
from ..daily_dfs.sleep_time import create_df_daily_sleep, create_df_n_minus1_daily_sleep
from ..daily_dfs.steps import  create_df_daily_steps
from ..daily_dfs.heart_rate import create_df_daily_heart_rate, create_df_n_minus1_daily_heart_rate
from ..daily_dfs.weather import create_df_weather_history
from ..daily_dfs.user_location_day import create_df_daily_user_location_consecutive

#sleep changed
def corr_steps_sleep(df_qty_cat):
    logger_ws_analysis.info("- in corr_steps_sleep")

    # df_qty_cat = create_user_qty_cat_df(1)
    user_id = df_qty_cat['user_id'].iloc[0]
    # Step 1: Create daily steps dataframe
    df_daily_steps = create_df_daily_steps(df_qty_cat)
    if len(df_daily_steps) == 0:
        logger_ws_analysis.info("- if len(df_daily_steps) == 0:")
        return "insufficient data", "insufficient data"
    # df_daily_steps['startDate_dateOnly']=pd.to_datetime(df_daily_steps['startDate_dateOnly'])
    # Step 2: Create daily sleep n-1 df
    df_daily_sleep = create_df_daily_sleep(df_qty_cat)# create daily sleep
    if len(df_daily_sleep) == 0:
        logger_ws_analysis.info("- if len(df_daily_sleep) == 0:")
        return "insufficient data", "insufficient data"
    
    logger_ws_analysis.info("- in corr_steps_sleep ---> has enough df_daily_steps and df_daily_sleep")

    df_daily_sleep.rename(columns=({'startDate_dateOnly_sleep_adj':'startDate_dateOnly'}),inplace=True)

    df_n_minus1_daily_sleep = create_df_n_minus1_daily_sleep(user_id, df_daily_sleep)
    df_n_minus1_daily_sleep['startDate_dateOnly']=pd.to_datetime(df_n_minus1_daily_sleep['startDate_dateOnly'])

    # This will keep only the rows that have matching 'startDate_dateOnly' values in both dataframes
    df_daily_steps_sleep = pd.merge(df_daily_steps,df_n_minus1_daily_sleep, on='startDate_dateOnly')
    df_daily_steps_sleep.dropna(inplace=True)

    if len(df_daily_steps_sleep) > 0:
        logger_ws_analysis.info("- if len(df_daily_steps_sleep) > 0:")

        try:
            # save csv file for user
            csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_steps_sleep.csv")
            df_daily_steps_sleep.to_csv(csv_path_and_filename)

            correlation = df_daily_steps_sleep['step_count'].corr(df_daily_steps_sleep['sleep_duration'])
            obs_count = len(df_daily_steps_sleep)
            # logger_ws_analysis.info(f"correlation: {correlation}, corr type: {correlation}")
            logger_ws_analysis.info(f"df_daily_steps_sleep correlation: {correlation}, obs_count: {obs_count}")
            return correlation, obs_count
        except Exception as e:
            logger_ws_analysis.info(f"error in corr_sleep_heart_rate: {e}")
            return "insufficient data", "insufficient data"
    else:
        logger_ws_analysis.info(f"- corr_steps_sleep had no observations")
        return "insufficient data", "insufficient data"

#reverted timezone change
def corr_steps_heart_rate(df_qty_cat):
    logger_ws_analysis.info("- in corr_steps_sleep")

    # df_qty_cat = create_user_qty_cat_df(1)
    user_id = df_qty_cat['user_id'].iloc[0]
    # Step 1: Create daily steps dataframe
    df_daily_steps = create_df_daily_steps(df_qty_cat)
    if len(df_daily_steps) == 0:
        logger_ws_analysis.info("- if len(df_daily_steps) == 0:")
        return "insufficient data", "insufficient data"
    # df_daily_steps['startDate_dateOnly']=pd.to_datetime(df_daily_steps['startDate_dateOnly'])
    # Step 2: Create daily heart rate df
    df_daily_heart_rate = create_df_daily_heart_rate(df_qty_cat)# create daily steps
    if len(df_daily_heart_rate) == 0:
        return "insufficient data", "insufficient data"
    # df_n_minus1_daily_steps = create_df_n_minus1_daily_steps(df_daily_steps)
    df_n_minus1_daily_heart_rate = create_df_n_minus1_daily_heart_rate(df_daily_heart_rate)
    df_n_minus1_daily_heart_rate['startDate_dateOnly']=pd.to_datetime(df_n_minus1_daily_heart_rate['startDate_dateOnly'])
    
    logger_ws_analysis.info("- in corr_steps_sleep ---> has enough df_daily_steps and df_n_minus1_daily_heart_rate")

    # This will keep only the rows that have matching 'dateUserTz' values in both dataframes
    df_daily_steps_heart_rate_n_minus1 = pd.merge(df_daily_steps,df_n_minus1_daily_heart_rate, on='startDate_dateOnly')
    df_daily_steps_heart_rate_n_minus1.dropna(inplace=True)

    if len(df_daily_steps_heart_rate_n_minus1) > 0:
        logger_ws_analysis.info("- if len(df_daily_steps_heart_rate_n_minus1) > 0:")

        try:
            # save csv file for user
            csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_steps_heart_rate_n_minus1.csv")
            df_daily_steps_heart_rate_n_minus1.to_csv(csv_path_and_filename)

            correlation = df_daily_steps_heart_rate_n_minus1['step_count'].corr(df_daily_steps_heart_rate_n_minus1['heart_rate_avg'])
            obs_count = len(df_daily_steps_heart_rate_n_minus1)
            # logger_ws_analysis.info(f"correlation: {correlation}, corr type: {correlation}")
            logger_ws_analysis.info(f"df_daily_steps_heart_rate_n_minus1 correlation: {correlation}, obs_count: {obs_count}")
            return correlation, obs_count
        except Exception as e:
            logger_ws_analysis.info(f"error in corr_sleep_heart_rate: {e}")
            return "insufficient data", "insufficient data"
    else:
        logger_ws_analysis.info(f"- corr_sleep_heart_rate had no observations")
        return "insufficient data", "insufficient data"

#reverted timezone change
def corr_steps_cloudiness(df_qty_cat):

    logger_ws_analysis.info("- in corr_workouts_heart_rate")
    user_id = df_qty_cat['user_id'].iloc[0]

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

    
    # Step 3: Create daily steps dataframe
    df_daily_steps = create_df_daily_steps(df_qty_cat)
    if len(df_daily_steps) == 0:
        logger_ws_analysis.info("- if len(df_daily_steps) == 0:")
        return "insufficient data", "insufficient data"
    # df_daily_steps['startDate_dateOnly']=pd.to_datetime(df_daily_steps['startDate_dateOnly'])


    df_daily_steps_cloudcover = pd.merge(df_daily_cloudcover,df_daily_steps, on='startDate_dateOnly')
    df_daily_steps_cloudcover['startDate_dateOnly'] = df_daily_steps_cloudcover['startDate_dateOnly'].dt.strftime('%Y-%m-%d')

    # save csv file for user
    csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_steps_cloudcover.csv")
    df_daily_steps_cloudcover.to_csv(csv_path_and_filename)

    correlation = df_daily_steps_cloudcover['step_count'].corr(df_daily_steps_cloudcover['cloudcover'])
    obs_count = len(df_daily_steps_cloudcover)
    
    return correlation, obs_count

#reverted timezone change
def corr_steps_temperature(df_qty_cat):

    logger_ws_analysis.info("- in corr_workouts_heart_rate")
    user_id = df_qty_cat['user_id'].iloc[0]

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
    
    # Step 3: Create daily steps dataframe
    df_daily_steps = create_df_daily_steps(df_qty_cat)
    if len(df_daily_steps) == 0:
        logger_ws_analysis.info("- if len(df_daily_steps) == 0:")
        return "insufficient data", "insufficient data"
    # df_daily_steps['startDate_dateOnly']=pd.to_datetime(df_daily_steps['startDate_dateOnly'])


    df_daily_steps_temperature = pd.merge(df_daily_temperature,df_daily_steps, on='startDate_dateOnly')
    df_daily_steps_temperature['startDate_dateOnly'] = df_daily_steps_temperature['startDate_dateOnly'].dt.strftime('%Y-%m-%d')

    # save csv file for user
    csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_steps_temperature.csv")
    df_daily_steps_temperature.to_csv(csv_path_and_filename)

    correlation = df_daily_steps_temperature['step_count'].corr(df_daily_steps_temperature['temp'])
    obs_count = len(df_daily_steps_temperature)
    
    return correlation, obs_count

