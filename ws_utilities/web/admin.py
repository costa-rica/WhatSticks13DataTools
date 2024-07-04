import os
import pandas as pd
import zipfile
import shutil
from ws_models import Base, engine, DatabaseSession, Users, AppleHealthWorkout, AppleHealthQuantityCategory,  UserLocationDay, \
    Locations, WeatherHistory
from ..common.config_and_logger import config, logger_ws_utilities
from ..common.utilities import wrap_up_session


# def create_df_crosswalk(table_name, zip_filename):
def create_df_crosswalk(table_name_for_crosswalk, zip_filename):

    ########################################################################################
    # even if zip file has no new id's in it a df_crosswalk will be created
    # even if zip file has no new id's but new data
    ##########################################################################################

    logger_ws_utilities.info("- accessed: ws_utlitiles/api/admin.py create_df_crosswalk_users ")
    logger_ws_utilities.info(f"--- creating crosswalk for {table_name_for_crosswalk} ---")
    # Step1: Make dictioanry of all the dataframes of new data
    zip_path = f"{config.DB_UPLOAD}/{zip_filename}"

    # Read files into dictionary
    dataframes_dict = read_files_into_dict(zip_path)

    # Step 2: create df_crosswalk_users w/ columns 'id', 'lat', 'lon'
    if table_name_for_crosswalk == 'locations':
        df_crosswalk = dataframes_dict.get(table_name_for_crosswalk)[['id', 'lat','lon']].copy()
        columns_match_list = ['lat','lon']
        logger_ws_utilities.info(f"- rows found in NEW/dict locations: {len(df_crosswalk)} ")
    elif table_name_for_crosswalk == 'users':
        df_crosswalk = dataframes_dict.get(table_name_for_crosswalk)[['id', 'email']].copy()
        columns_match_list = ['email']
    
    # Step 3: create df_locations from database
    df_from_db = create_df_from_db_table_name(table_name_for_crosswalk)
    logger_ws_utilities.info(f"- rows found in EXISTING/db locations: {len(df_from_db)} ")
    # Step 4: Remove columns not in database
    new_data_column_names = dataframes_dict.get(table_name_for_crosswalk).columns
    for col_name in new_data_column_names:
        if col_name not in df_from_db.columns:
            dataframes_dict.get(table_name_for_crosswalk).drop(columns=col_name, inplace=True)

    
    # Step 5: Assign the df_from_dict
    if len(df_from_db) == 0:
        df_from_dict = dataframes_dict.get(table_name_for_crosswalk)
    # elif len(dataframes_dict.get(table_name_for_crosswalk)) > 0:
    elif len(df_from_db) > 0:
        # Step 5a: remove matching rows, if any rows exist in the database already
        df_from_dict = remove_matching_rows(dataframes_dict.get(table_name_for_crosswalk), df_from_db, columns_match_list)

    # Step 6: add df_locations to Locations table
    count_of_rows_added = df_from_dict.to_sql(table_name_for_crosswalk, con=engine, if_exists='append', index=False)
    logger_ws_utilities.info(f"- count_of_rows_added: {count_of_rows_added} ")
    
    logger_ws_utilities.info(f"- rows found in df_from_dict: {len(df_from_dict)} ")
    
    # recreate df_users with all the new users added
    df_from_db = create_df_from_db_table_name(table_name_for_crosswalk)

    # Step 7: append to crosswalk
    if table_name_for_crosswalk == "locations":
        # Perform the merge on 'lat' and 'lon' columns
        df_crosswalk = pd.merge(
            df_crosswalk,
            df_from_db[['id', 'lat', 'lon']],
            on=['lat', 'lon'],
            how='left',
            suffixes=('', '_new')
        )
    elif table_name_for_crosswalk == "users":
        # Merge df_crosswalk_users with df_users on 'email' to map 'id' from df_users to 'new_id' in df_crosswalk_users
        df_crosswalk = df_crosswalk.merge(df_from_db[['id', 'email']], on='email', suffixes=('', '_new'))

    # Rename the 'id_new' column to 'new_id'
    df_crosswalk.rename(columns={'id_new': 'new_id'}, inplace=True)

    if len(df_from_dict) > 0:
        # Step 8: add indicator for new user/location
        df_from_dict['new_row']='yes'
        # df_from_dict.rename(columns={'id': 'new_id'}, inplace=True)

        logger_ws_utilities.info(f"df_from_dict length: {len(df_from_dict)} ")
        logger_ws_utilities.info(f"df_from_dict columns: {list(df_from_dict.columns)} ")
        logger_ws_utilities.info(f"df_crosswalk length: {len(df_crosswalk)} ")
        logger_ws_utilities.info(f"df_crosswalk columns: {list(df_crosswalk.columns)} ")

        if table_name_for_crosswalk == "locations":
            df_crosswalk_with_new_row_indicator = pd.merge(df_crosswalk,df_from_dict[['lat','lon','new_row']],
                                                            on=['lat','lon'],how='left', suffixes=('', '_new'))
        elif table_name_for_crosswalk == "users":
            # df_from_dict will does not have id or new_id so merge on email
            df_crosswalk_with_new_row_indicator = pd.merge(df_crosswalk,df_from_dict[['email','new_row']],
                                                                on=['email'],how='left', suffixes=('', '_new'))

        logger_ws_utilities.info(f"- completed df_crosswalk for : {table_name_for_crosswalk} ")
        return df_crosswalk_with_new_row_indicator
        
    logger_ws_utilities.info(f"- completed df_crosswalk for : {table_name_for_crosswalk} ")
    return df_crosswalk

def update_and_append_via_df_crosswalk_users(table_name,zip_filename,df_crosswalk_users):
    logger_ws_utilities.info("- accessed: ws_utlitiles/api/admin.py update_and_append_via_df_crosswalk_users ")

    if len(df_crosswalk_users) == 0:
        logger_ws_utilities.info("- No rows in df_crosswalk_users -> likely due to no users found in zip file or no different user_ids.")
        return 0

    # Step 1: create df_from_dict from zip file in db_upload
    zip_path = f"{config.DB_UPLOAD}/{zip_filename}"
    dataframes_dict = read_files_into_dict(zip_path)
    # df_from_dict = dataframes_dict.get(table_name)
    try:
        df_from_dict = dataframes_dict[table_name]
    except Exception as e:
        logger_ws_utilities.info(f"{type(e).__name__}: {e}")
        logger_ws_utilities.info(f"- If KeyError, most likely due to no new data for {table_name} table in .zip file")
        return 0

    # Step 2: using df_crosswalk_users update the user_id column
    # The map will have old user_id as index (id) and new user_id as values (new_id)
    user_id_map = df_crosswalk_users.set_index('id')['new_id']
    # Update df_from_dict.user_id by mapping using the user_id_map
    df_from_dict['user_id'] = df_from_dict['user_id'].map(user_id_map)

    # Step 3: remove user_id == Nan rows, these are rows that have user_ids that are no longer matched to accounts.
    ## ---> users have deleted accounts, but for some reason corresponding data was not deleted.
    df_from_dict_no_nans = df_from_dict.dropna(subset=['user_id'])
    # This process converts user_id and totalEnergyBurned Convert the user_id column from float64 to int64
    df_from_dict_no_nans['user_id'] = df_from_dict_no_nans['user_id'].astype('int64')

    # Step 4: remove any duplicate rows - Drop duplicates based on unique constraint
    unique_constraint_column_names_list = ['user_id', 'sampleType', 'UUID','startDate']
    df_from_dict_no_nans.drop_duplicates(subset=unique_constraint_column_names_list, inplace=True)

    # Step 5: Get existing data from database make df, combine new data with existing data, verify no duplicates in combined df
    ## - drop column id
    df_from_dict_no_nans.drop(columns=['id'], inplace=True)
    
    ## - combine existing data from db to df_from_dict_no_nans and then go to Step 5
    table_object = get_class_from_tablename(table_name)
    # df_from_db_workouts = create_df_from_db_table(AppleHealthWorkout)
    df_from_db = create_df_from_db_table(table_object)
    df_from_db.drop(columns=['id'], inplace=True)
    
    # if df_from_dict_no_nans is empty by now, then there is no need to do any more because all the data was duplicates

    if len(df_from_dict_no_nans) > 0:

        df_from_dict_unique_new = df_from_dict_no_nans

        if len(df_from_db) > 0:
            ### Here this stpe needs to remove all rows in df_from_dict_no_nans that are already in df_from_db_workouts
            # Potentially could be done with:
            df_from_dict_unique_new = remove_matching_rows(df_from_dict_no_nans, df_from_db, unique_constraint_column_names_list)

        #Step 6: Add data
        count_of_rows_added = df_from_dict_unique_new.to_sql(table_name, con=engine, if_exists='append', index=False)

        logger_ws_utilities.info(f"- successfully added {count_of_rows_added:,} rows to {table_name} table ")

        return count_of_rows_added

    logger_ws_utilities.info(f"- No rows added to {table_name} table because data was all duplicates")
    
    return 0

def update_and_append_via_df_crosswalk_locations(table_name, id_column_name, zip_filename,df_crosswalk_locations):
    logger_ws_utilities.info("- accessed: ws_utlitiles/api/admin.py update_and_append_via_df_crosswalk_locations ")

    if len(df_crosswalk_locations) == 0:
        logger_ws_utilities.info("- No rows in df_crosswalk_locations -> likely due to no locations found in zip file or no different location_ids.")
        return 0

    # Step 1: create df_from_dict from zip file in db_upload
    zip_path = f"{config.DB_UPLOAD}/{zip_filename}"
    dataframes_dict = read_files_into_dict(zip_path)
    # df_from_dict = dataframes_dict.get(table_name)
    try:
        df_from_dict = dataframes_dict[table_name]
    except Exception as e:
        logger_ws_utilities.info(f"{type(e).__name__}: {e}")
        logger_ws_utilities.info(f"- If KeyError, most likely due to no new data for {table_name} table in .zip file")
        return 0

    # Step 2: using df_crosswalk_users update the user_id column
    # The map will have old user_id as index (id) and new user_id as values (new_id)
    location_id_map = df_crosswalk_locations.set_index('id')['new_id']
    # Update df_from_dict.user_id by mapping using the location_id_map
    df_from_dict[id_column_name] = df_from_dict[id_column_name].map(location_id_map)

    # Step 3: remove user_id == Nan rows, these are rows that have user_ids that are no longer matched to accounts.
    ## ---> users have deleted accounts, but for some reason corresponding data was not deleted.
    df_from_dict_no_nans = df_from_dict.dropna(subset=[id_column_name])

    # # Step 4: remove any duplicate rows - Drop duplicates based on unique constraint
    unique_constraint_column_names_list = ['location_id', 'date_time']
    df_from_dict_no_nans.drop_duplicates(subset=unique_constraint_column_names_list, inplace=True)

    # Step 5: Get existing data from database make df, combine new data with existing data, verify no duplicates in combined df
    ## - drop column id
    df_from_dict_no_nans.drop(columns=['id'], inplace=True)
    df_from_dict_no_nans[id_column_name] = df_from_dict_no_nans[id_column_name].astype('int64')

    df_from_db_weather_hist = create_df_from_db_table_name(table_name)
    df_from_db_weather_hist.drop(columns=['id'], inplace=True)
    
    # if df_from_dict_no_nans is empty by now, then there is no need to do any more because all the data was duplicates
    if len(df_from_dict_no_nans) > 0:

        df_from_dict_unique_new = df_from_dict_no_nans

        if len(df_from_db_weather_hist) > 0:
            ### Here this stpe needs to remove all rows in df_from_dict_no_nans that are already in df_from_db_<table_name>
            df_from_dict_unique_new = remove_matching_rows(df_from_dict_no_nans, df_from_db_weather_hist, unique_constraint_column_names_list)


        #Step 6: Add data
        count_of_rows_added = df_from_dict_unique_new.to_sql(table_name, con=engine, if_exists='append', index=False)
        
        logger_ws_utilities.info(f"- successfully added {count_of_rows_added:,} rows to {table_name} table ")
        
        return count_of_rows_added
    logger_ws_utilities.info(f"- No rows added to {table_name} table because data was all duplicates")
    
    return 0

def update_and_append_user_location_day(zip_filename,df_crosswalk_users,df_crosswalk_locations):
    logger_ws_utilities.info("- accessed: ws_utlitiles/api/admin.py update_and_append_user_location_day ")
    
    table_name = 'user_location_day'
    
    # Step 2: create df_from_dict from zip file in db_upload
    zip_path = f"{config.DB_UPLOAD}/{zip_filename}"
    dataframes_dict = read_files_into_dict(zip_path)
    try:
        df_from_dict = dataframes_dict[table_name]
    except Exception as e:
        logger_ws_utilities.info(f"{type(e).__name__}: {e}")
        logger_ws_utilities.info(f"- If KeyError, most likely due to no new data for {table_name} table in .zip file")
        return 0

    if len(df_crosswalk_users) > 0:

        # Step 3: update/map user_id
        user_id_map = df_crosswalk_users.set_index('id')['new_id']
        df_from_dict['user_id'] = df_from_dict['user_id'].map(user_id_map)

        # Step 3 b: remove user_id == Nan rows
        df_from_dict_no_nans_01 = df_from_dict.dropna(subset=['user_id'])
    else:
        df_from_dict_no_nans_01 = df_from_dict
        logger_ws_utilities.info("- No user_ids to adjust ")

    if len(df_crosswalk_locations) > 0:

        # Step 4: update/map location_id
        location_id_map = df_crosswalk_locations.set_index('id')['new_id']
        df_from_dict_no_nans_01['location_id'] = df_from_dict_no_nans_01['location_id'].map(location_id_map)
        # Step 4 b: remove location_id == Nan rows
        df_from_dict_no_nans_02 = df_from_dict_no_nans_01.dropna(subset=['location_id'])

    else:
        df_from_dict_no_nans_02 = df_from_dict_no_nans_01
        logger_ws_utilities.info("- No location_ids to adjust ")


    # Step 5: create df_from_db
    df_from_db = create_df_from_db_table_name(table_name)

    # Step 6:
    # if df_from_dict_no_nans_02 is empty by now, then there is no need to do any more because all the data was duplicates
    if len(df_from_dict_no_nans_02) > 0:
        df_from_dict_unique_new = df_from_dict_no_nans_02
        # Step 6: remove existing rows based on user_id, date_utc_user_check_in
        if len(df_from_db) > 0:
            unique_constraint_column_names_list = ['user_id', 'date_utc_user_check_in']
            df_from_dict_unique_new = remove_matching_rows(df_from_dict_no_nans_02, df_from_db, unique_constraint_column_names_list)

        #Step 7: Add data
        count_of_rows_added = df_from_dict_unique_new.to_sql(table_name, con=engine, if_exists='append', index=False)

        logger_ws_utilities.info(f"- successfully added {count_of_rows_added:,} rows to {table_name} table ")
        
        return count_of_rows_added
        
    logger_ws_utilities.info(f"- No rows added to {table_name} table because data was all duplicates")
    
    return 0

####################################
# Utilities to create df_crosswalk #
####################################

def read_files_into_dict(zip_path):
    # Dictionary to store DataFrames
    dfs = {}
    extract_folder_path = os.path.join(os.path.dirname(zip_path),"temp_dir")
    # Open the ZIP file
    with zipfile.ZipFile(zip_path, 'r') as z:
        # Extract all the files into a temporary directory
        z.extractall(extract_folder_path)
        # Iterate over each file in the directory
        for filename in z.namelist():
            if filename[:len("__MACOSX/")] != "__MACOSX/":
            
                # Determine the file's full path
                file_path = os.path.join(extract_folder_path, filename)
                # Get the file name without the extension
                file_key = os.path.splitext(os.path.basename(filename))[0]
                
                # Check the file extension and load accordingly
                if filename.endswith('.csv'):
                    dfs[file_key] = pd.read_csv(file_path)
                elif filename.endswith('.pkl'):
                    dfs[file_key] = pd.read_pickle(file_path)

    shutil.rmtree(extract_folder_path)
    
    return dfs

def remove_matching_rows(df_from_dict, df_from_db, match_columns):
    # df_from_dict: all rows downloaded
    # df_from db: rows currently in database
    # match_columns: list of column names to match, must have at least one
    
    # Step 4: Dynamically create 'match-string-column' based on match_columns
    separator = ','
    df_from_dict['match-string-column'] = df_from_dict[match_columns].astype(str).agg(separator.join, axis=1)
    df_from_db['match-string-column'] = df_from_db[match_columns].astype(str).agg(separator.join, axis=1)
    
    # Step 5: Remove rows from df_from_dict that are in df_from_db
    unique_match_string = set(df_from_db['match-string-column'])
    df_from_dict = df_from_dict[~df_from_dict['match-string-column'].isin(unique_match_string)]
    
    # Assuming 'id' column exists and needs to be dropped. Adjust accordingly if not applicable.
    columns_to_drop = ['id', 'match-string-column'] if 'id' in df_from_dict.columns else ['match-string-column']
    
    # Step 6: Drop the 'id' and 'match-string-column' columns from df_from_dict
    df_from_dict.drop(columns=columns_to_drop, inplace=True)
    
    return df_from_dict

def create_df_from_db_table(sqlalchemy_table_object):
    db_session = DatabaseSession()# This new db_session is ok, becuase this session is isolated, to just get data into a dataframe (open an close is ok)
    df_db_query = db_session.query(sqlalchemy_table_object)
    df_from_db = pd.read_sql(df_db_query.statement, engine)
    wrap_up_session(db_session)
    return df_from_db

def get_class_from_tablename(tablename):
  for c in Base.__subclasses__():
    if c.__tablename__ == tablename:
      return c

def create_df_from_db_table_name(table_name):
    logger_ws_utilities.info("- in create_df_from_db_table_name -")
    db_session = DatabaseSession()# This new db_session is ok, becuase this session is isolated, to just get data into a dataframe (open an close is ok)
    sqlalchemy_table_object = get_class_from_tablename(table_name)
    df_db_query = db_session.query(sqlalchemy_table_object)
    df_from_db = pd.read_sql(df_db_query.statement, engine)

    wrap_up_session(db_session)
    return df_from_db
