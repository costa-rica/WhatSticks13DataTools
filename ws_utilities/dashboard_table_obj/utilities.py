import os
# from ws_models import engine, DatabaseSession, AppleHealthQuantityCategory
import pandas as pd
import json
# from common.config_and_logger import config, logger_apple
from ..common.config_and_logger import config, logger_ws_utilities
from ..common.utilities import wrap_up_session

from ..web.admin import create_df_from_db_table_name


def get_apple_health_count_date(user_id):
    logger_ws_utilities.info(f"- in get_apple_health_count_date -")
    # db_session = DatabaseSession()

    user_apple_qty_cat_dataframe_pickle_file_name = f"user_{int(user_id):04}_apple_health_dataframe.pkl"
    user_apple_workouts_dataframe_pickle_file_name = f"user_{int(user_id):04}_apple_workouts_dataframe.pkl"
    pickle_data_path_and_name_qty_cat = os.path.join(config.DATAFRAME_FILES_DIR, user_apple_qty_cat_dataframe_pickle_file_name)
    pickle_data_path_and_name_workouts = os.path.join(config.DATAFRAME_FILES_DIR, user_apple_workouts_dataframe_pickle_file_name)

    # Make dataframe from AppleHealthQuantityCategory data
    if os.path.exists(pickle_data_path_and_name_qty_cat):
        logger_ws_utilities.info(f"- reading pickle file for qty_cat: {pickle_data_path_and_name_qty_cat} -")
        # df_existing_user_workouts_data=pd.read_pickle(pickle_apple_qty_cat_path_and_name)
        # df=pd.read_pickle(pickle_data_path_and_name_qty_cat)
        df_apple_qty_cat = pd.read_pickle(pickle_data_path_and_name_qty_cat)
    else:
        # # query = f"SELECT * FROM apple_health_quantity_category WHERE user_id = {user_id}"
        # db_query = db_session.query(AppleHealthQuantityCategory).filter_by(user_id=user_id)
        # df = pd.read_sql_query(db_query.statement, engine)
        # wrap_up_session(db_session)
        df_apple_qty_cat = create_df_from_db_table_name("apple_health_quantity_category")
    
    logger_ws_utilities.info(f"** seemed to stop here: ")
    logger_ws_utilities.info(f"df_apple_qty_cat length:{len(df_apple_qty_cat)} ")
    
    if os.path.exists(pickle_data_path_and_name_workouts):
        df_apple_workouts = pd.read_pickle(pickle_data_path_and_name_workouts)
    else:
        df_apple_workouts = create_df_from_db_table_name("apple_health_workout")

    # get count of qty_cat and workouts
    apple_health_record_count = "{:,}".format(len(df_apple_qty_cat) + len(df_apple_workouts))

    # Convert startDate to datetime
    df_apple_qty_cat['startDate'] = pd.to_datetime(df_apple_qty_cat['startDate'])
    earliest_date_qty_cat = df_apple_qty_cat['startDate'].min()

    if not os.path.exists(pickle_data_path_and_name_workouts):
        earliest_date_str = earliest_date_qty_cat.strftime('%b %d, %Y')
        return apple_health_record_count, earliest_date_str

    df_apple_workouts['startDate'] = pd.to_datetime(df_apple_workouts['startDate'])
    earliest_date_workouts = df_apple_workouts['startDate'].min()
    earliest_date_str = ""
    if earliest_date_workouts < earliest_date_qty_cat:
        earliest_date_str = earliest_date_workouts.strftime('%b %d, %Y')
    else:
        earliest_date_str = earliest_date_qty_cat.strftime('%b %d, %Y')

    return apple_health_record_count, earliest_date_str