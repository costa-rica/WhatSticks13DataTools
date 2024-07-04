from ..common.config_and_logger import config, logger_ws_analysis
from ..common.utilities import wrap_up_session
import pandas as pd
from ws_models import engine, DatabaseSession, WeatherHistory

def create_df_weather_history():
    db_session = DatabaseSession()
    weather_hist_query = db_session.query(WeatherHistory)
    weather_hist_df = pd.read_sql(weather_hist_query.statement, engine)
    wrap_up_session(db_session)

    # Convert 'date_time' column from string to datetime
    weather_hist_df['date_time'] = pd.to_datetime(weather_hist_df['date_time'])
    # Extract the date component and overwrite the 'date_time' column
    weather_hist_df['date_time'] = weather_hist_df['date_time'].dt.date
    weather_hist_df.rename(columns={'date_time': 'date'},inplace=True)

    return weather_hist_df