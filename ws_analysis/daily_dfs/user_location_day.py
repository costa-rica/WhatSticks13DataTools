from ..common.config_and_logger import config, logger_ws_analysis
from ..common.create_user_df import create_user_location_date_df
import pandas as pd
from datetime import datetime, timedelta

# renders the interpolation in WS11Scheduler obsolete
def create_df_daily_user_location_consecutive(user_id):
    logger_ws_analysis.info("- in create_df_daily_user_location_consecutive -")

    df_user_location_date = create_user_location_date_df(user_id)
    if len(df_user_location_date) == 0:
        logger_ws_analysis.info("- User has no location associated due to NO data in User Location Date -")
        return pd.DataFrame()
    
    start_date_obj = df_user_location_date['date'].min()
    search_row_date = start_date_obj
    df = pd.DataFrame()
    # while search_row_date <= end_date_obj:
    while search_row_date <= datetime.now().date():
    
        df_next_row = return_next_day(df_user_location_date, search_row_date)
        df_next_row.reset_index(inplace=True, drop=True)

        if df_next_row.date.loc[0] == search_row_date:
            # Use pd.concat to add the new row to the existing DataFrame
            df = pd.concat([df, df_next_row], ignore_index=True)
        else:
            location_id = df_next_row.location_id.loc[0]
            city = df_next_row.city.loc[0]
            country = df_next_row.country.loc[0]
            tz_id = df_next_row.tz_id.loc[0]
            df_next_row = pd.DataFrame({
                'date': [search_row_date], 
                'location_id': [location_id],
                'city': [city],
                'country': [country],
                'tz_id': [tz_id]
                })
            df = pd.concat([df, df_next_row], ignore_index=True)

        search_row_date = search_row_date + timedelta(days=1)

    # If user has less than 5 days of historical data, extend the history back 14 days
    if len(df) < 5:
        df = extend_historically_user_location_date(df)

    return df

################
# Utility #
###############

def return_next_day(df_daily_user_location, search_row_date):
    # Attempt to find a row that matches the search date exactly
    df = df_daily_user_location[df_daily_user_location['date'] == search_row_date]

    # logger_ws_analysis.info(df_daily_user_location.dtypes)
    # logger_ws_analysis.info(f"row 1: {df_daily_user_location.loc[0].date}")
    # logger_ws_analysis.info(f"-----------------------------")

    if len(df) == 0:
        # Filter the DataFrame for rows with dates older than the search date
        older_dates_df = df_daily_user_location[df_daily_user_location['date'] < search_row_date]
        # Filter the DataFrame for rows with dates newer than the search date
        newer_dates_df = df_daily_user_location[df_daily_user_location['date'] > search_row_date]
        if not older_dates_df.empty:
            # Find the maximum date within the filtered DataFrame for older dates
            closest_older_date = older_dates_df['date'].max()
            # Retrieve the row with the closest older date
            df = df_daily_user_location[df_daily_user_location['date'] == closest_older_date]
            # logger_ws_analysis.info(f"Row with the closest older date to {search_row_date} :\n {df}")
        elif not newer_dates_df.empty:
            # Find the minimum date within the filtered DataFrame for newer dates
            closest_newer_date = newer_dates_df['date'].min()
            # Retrieve the row with the closest newer date
            df = df_daily_user_location[df_daily_user_location['date'] == closest_newer_date]
            # logger_ws_analysis.info(f"Row with the closest newer date to {search_row_date} :\n {df}")
        else:
            # If no older or newer dates are found, return an empty DataFrame with the same columns
            # logger_ws_analysis.info("No matching rows found for the specified criteria in the DataFrame.")
            df = pd.DataFrame(columns=df_daily_user_location.columns)
    return df


def extend_historically_user_location_date(df_user_location_date, back_to_date=None):
    # Check if the dataframe is empty
    if df_user_location_date.empty:
        raise ValueError("The dataframe is empty")
    
    # Find the oldest date in the dataframe
    oldest_date = df_user_location_date['date'].min()
    
    # Determine the backfill start date
    if back_to_date is None:
        back_to_date = oldest_date - timedelta(days=14)
    
    # Check if back_to_date is already before the oldest date
    if back_to_date >= oldest_date:
        return df_user_location_date
    
    # Find the location_id of the oldest date entry
    oldest_location_id = df_user_location_date.loc[df_user_location_date['date'] == oldest_date, 'location_id'].iloc[0]
    
    # Generate the date range from back_to_date to the day before the oldest_date
    new_dates = pd.date_range(start=back_to_date, end=oldest_date - timedelta(days=1), freq='D')
    
    # Create a new dataframe with these dates, converting Timestamp to datetime.date
    new_df = pd.DataFrame({
        'date': new_dates.date,
        'location_id': oldest_location_id
    })
    
    # Concatenate the new dataframe with the original dataframe
    extended_df = pd.concat([new_df, df_user_location_date]).sort_values(by='date').reset_index(drop=True)
    
    return extended_df

# Example usage:
# Assuming df_user_location_date is your existing dataframe
# extended_df = extend_historically_user_location_date(df_user_location_date, back_to_date=datetime.date(2020, 1, 1))


