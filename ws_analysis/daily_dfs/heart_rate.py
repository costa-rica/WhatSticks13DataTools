from ..common.config_and_logger import config, logger_ws_analysis
import pandas as pd
from datetime import datetime, timedelta



def create_df_daily_heart_rate(df):
    logger_ws_analysis.info("- in create_df_daily_heart_rate")
    df_heart_rate = df[df.sampleType == 'HKQuantityTypeIdentifierHeartRate']
    if len(df_heart_rate) == 0:
        return pd.DataFrame()#<-- return must return dataframe, expecting df on other end
    df_heart_rate['quantity'] = df_heart_rate['quantity'].astype('float')
    aggregated_heart_rate_data = df_heart_rate.groupby('startDate_dateOnly')['quantity'].mean().reset_index()
    aggregated_heart_rate_data.rename(columns=({'quantity':'heart_rate_avg'}),inplace=True)
    return aggregated_heart_rate_data


def create_df_n_minus1_daily_heart_rate(df_daily_heart_rate):
    logger_ws_analysis.info("- in create_df_n_minus1_daily_heart_rate")

    # df_daily_heart_rate['startDate_dateOnly'] = pd.to_datetime(df_daily_heart_rate['startDate_dateOnly'])
    # Subtract one day from each date in the column
    df_daily_heart_rate['startDate_dateOnly'] = df_daily_heart_rate['startDate_dateOnly'] - timedelta(days=1)
    # # Convert back to 'YYYY-MM-DD' format if needed
    # df_daily_heart_rate['startDate_dateOnly'] = df_daily_heart_rate['startDate_dateOnly'].dt.strftime('%Y-%m-%d')

    return df_daily_heart_rate