import os
import json
from ..common.config_and_logger import config, logger_ws_analysis
from ..common.utilities import get_startDate_3pm, \
    calculate_duration_in_hours
import pandas as pd
from datetime import datetime, timedelta
import pytz


### NOTE:
# Replacements:
# dateUserTz replaced by startDate_dateOnly
# dateUserTz_3pm replaced by startDate_dateOnly_sleep_adj
# sleepTimeUserTz replaced by sleep_duration
# startDateUserTz deleted
# endDateUserTz deleted


# Note: "df" parameter is strictly df from create_user_qty_cat_df
def create_df_daily_sleep(df):
    logger_ws_analysis.info("- in create_df_daily_sleep")

    df_sleep = df[df['sampleType']=='HKCategoryTypeIdentifierSleepAnalysis'].copy()
    df_sleep['startDate'] = pd.to_datetime(df_sleep['startDate'])
    df_sleep['endDate'] = pd.to_datetime(df_sleep['endDate'])
    # df_sleep['startDate_dateOnly'] = pd.to_datetime(df_sleep['startDate_dateOnly'])
    if len(df_sleep) == 0:
        # return pd.DataFrame()#<-- return must return dataframe, expecting df on other end
        print("no data")
    else:
        
        # Apply the function to each row to create the new dateUserTz_3pm column
        # df_sleep['startDate'] = df_sleep.apply(get_dateUserTz_3pm, axis=1)
        df_sleep['startDate_dateOnly_sleep_adj'] = df_sleep.apply(get_startDate_3pm, axis=1)
        df_sleep_states_3_4_5 = df_sleep[df_sleep['value'].isin(["3.0", "4.0", "5.0", "3", "4", "5"])]

    df_sleep_states_3_4_5['sleep_duration'] = df_sleep_states_3_4_5.apply(lambda row: calculate_duration_in_hours(row['startDate'], row['endDate']), axis=1)
    aggregated_sleep_data = df_sleep_states_3_4_5.groupby('startDate_dateOnly_sleep_adj')['sleep_duration'].sum().reset_index()

    # Create csv file for daily sleep
    user_id = df['user_id'].iloc[0]
    csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_sleep.csv")
    aggregated_sleep_data.to_csv(csv_path_and_filename)

    return aggregated_sleep_data


def create_df_n_minus1_daily_sleep(user_id, df_daily_sleep):
    logger_ws_analysis.info("- in create_df_n_minus1_daily_sleep")

    # Subtract one day from each date in the column
    df_daily_sleep['startDate_dateOnly'] = df_daily_sleep['startDate_dateOnly'] - timedelta(days=1)
    
    # Create csv file for daily (n-1) sleep
    csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_n_minus1_daily_sleep.csv")
    df_daily_sleep.to_csv(csv_path_and_filename)

    return df_daily_sleep