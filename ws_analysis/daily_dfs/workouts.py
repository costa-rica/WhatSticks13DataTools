from ..common.config_and_logger import config, logger_ws_analysis
import pandas as pd; import numpy as np


def create_df_daily_workout_duration(df):
    logger_ws_analysis.info("- in create_df_daily_workout_duration")
    df['duration'] = pd.to_numeric(df['duration'])
    aggregated_workout_durations = df.groupby('startDate_dateOnly')['duration'].sum().reset_index()
    return aggregated_workout_durations


def create_df_daily_workout_duration_dummies(df):
    logger_ws_analysis.info("- in create_df_daily_workout_duration_dummies")
    df_daily_workout_duration = create_df_daily_workout_duration(df)

    # Step 1: Get the min and max values of duration
    min_duration = df_daily_workout_duration['duration'].min()
    max_duration = df_daily_workout_duration['duration'].max()

    # Step 2: Create a list of column names based on increments of 10
    max_col_value = int(np.ceil(max_duration / 10.0)) * 10  # Round up to the nearest 10
    column_names = [f'dur_{i}_dummy' for i in range(10, max_col_value + 10, 10)]

    # # Step 3: Add columns to the dataframe
    for i, col in enumerate(column_names, start=1):
        lower_bound = (i - 1) * 10
        upper_bound = i * 10
        df_daily_workout_duration[col] = df_daily_workout_duration['duration'].apply(
            lambda x: 1 if lower_bound < x <= upper_bound else 0
        )
    
    return df_daily_workout_duration