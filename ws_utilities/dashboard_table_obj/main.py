from ..common.config_and_logger import config, logger_ws_utilities
from ..common.utilities import wrap_up_session
from .dependent_variables import sleep_time, workouts_duration, \
    steps_count
from .independent_variables import user_sleep_time_correlations, \
    user_workouts_duration_correlations, user_steps_count_correlations
from .utilities import get_apple_health_count_date
import os
from ws_models import DatabaseSession, Users
import pandas as pd
import json
from ws_analysis import create_df_daily_user_location_consecutive
from datetime import datetime


def create_dashboard_table_object_json_file(user_id):
    logger_ws_utilities.info(f"- in create_dashboard_table_object_json_file (ws_utilities) -")
    logger_ws_utilities.info(f"- for user: {user_id} -")
    user_id = int(user_id)
    array_dashboard_table_object = []

    ### CREATE sleep_time dashbaord object ####
    # keys to indep_var_object must match WSiOS IndepVarObject
    list_of_dictIndepVarObjects = user_sleep_time_correlations(user_id = user_id)# new
    logger_ws_utilities.info(f"*  SLEEP TIME  Dashboard Objs (list_of_dictIndepVarObjects) *")
    logger_ws_utilities.info(f"- {list_of_dictIndepVarObjects} -")

    if len(list_of_dictIndepVarObjects) > 0:# FIX: NEED TO GIVE TEH opportuity to return 0
        
        # keys to dashboard_table_object must match WSiOS DashboardTableObject
        dashboard_table_object = sleep_time()
        arry_indep_var_objects = []

        for dictIndepVarObjects in list_of_dictIndepVarObjects:
            if dictIndepVarObjects.get('correlationValue') != "insufficient data":
                long_f_string = (
                    f"- {dictIndepVarObjects.get('independentVarName')} (indep var) correlation with" +
                    f" {dictIndepVarObjects.get('forDepVarName')} (dep var): {dictIndepVarObjects.get('correlationValue')} -"
                )
                logger_ws_utilities.info(long_f_string)
                arry_indep_var_objects.append(dictIndepVarObjects)

        # Sorting (biggest to smallest) the list by the absolute value of correlationValue
        sorted_arry_indep_var_objects = sorted(arry_indep_var_objects, key=lambda x: abs(x['correlationValue']), reverse=True)

        # Converting correlationValue to string without losing precision
        for item in sorted_arry_indep_var_objects:
            item['correlationValue'] = str(item['correlationValue'])
            item['correlationObservationCount'] = str(item['correlationObservationCount'])

        dashboard_table_object['arryIndepVarObjects'] = sorted_arry_indep_var_objects
        array_dashboard_table_object.append(dashboard_table_object)
    ### END CREATE sleep_time dashbaord object ###

    
    ### START CREATE workouts_duration (Exercise Time) dashbaord object ###
    # keys to indep_var_object must match WSiOS IndepVarObject
    list_of_dictIndepVarObjects = user_workouts_duration_correlations(user_id)# new
    if len(list_of_dictIndepVarObjects) > 0:# FIX: NEED TO GIVE TEH opportuity to return 0
        # keys to dashboard_table_object must match WSiOS DashboardTableObject
        dashboard_table_object = workouts_duration()
        arry_indep_var_objects = []

        if list_of_dictIndepVarObjects != None:
            for dictIndepVarObjects in list_of_dictIndepVarObjects:
                if dictIndepVarObjects.get('correlationValue') != "insufficient data":
                    long_f_string = (
                        f"- {dictIndepVarObjects.get('independentVarName')} (indep var) correlation with" +
                        f" {dictIndepVarObjects.get('forDepVarName')} (dep var): {dictIndepVarObjects.get('correlationValue')} -"
                    )
                    logger_ws_utilities.info(long_f_string)
                    arry_indep_var_objects.append(dictIndepVarObjects)


            # Sorting (biggest to smallest) the list by the absolute value of correlationValue
            sorted_arry_indep_var_objects = sorted(arry_indep_var_objects, key=lambda x: abs(x['correlationValue']), reverse=True)

            # Converting correlationValue to string without losing precision
            for item in sorted_arry_indep_var_objects:
                item['correlationValue'] = str(item['correlationValue'])
                item['correlationObservationCount'] = str(item['correlationObservationCount'])

            dashboard_table_object['arryIndepVarObjects'] = sorted_arry_indep_var_objects
            array_dashboard_table_object.append(dashboard_table_object)
    ### END CREATE workouts_duration dashbaord object ###

    
    
    ### START CREATE steps_count  dashbaord object ###
    # keys to indep_var_object must match WSiOS IndepVarObject
    list_of_dictIndepVarObjects = user_steps_count_correlations(user_id)# new
    if len(list_of_dictIndepVarObjects) > 0:# FIX: NEED TO GIVE TEH opportuity to return 0
        # keys to dashboard_table_object must match WSiOS DashboardTableObject
        dashboard_table_object = steps_count()
        arry_indep_var_objects = []

        if list_of_dictIndepVarObjects != None:
            for dictIndepVarObjects in list_of_dictIndepVarObjects:
                if dictIndepVarObjects.get('correlationValue') != "insufficient data":
                    long_f_string = (
                        f"- {dictIndepVarObjects.get('independentVarName')} (indep var) correlation with" +
                        f" {dictIndepVarObjects.get('forDepVarName')} (dep var): {dictIndepVarObjects.get('correlationValue')} -"
                    )
                    logger_ws_utilities.info(long_f_string)
                    arry_indep_var_objects.append(dictIndepVarObjects)


            # Sorting (biggest to smallest) the list by the absolute value of correlationValue
            sorted_arry_indep_var_objects = sorted(arry_indep_var_objects, key=lambda x: abs(x['correlationValue']), reverse=True)

            # Converting correlationValue to string without losing precision
            for item in sorted_arry_indep_var_objects:
                item['correlationValue'] = str(item['correlationValue'])
                item['correlationObservationCount'] = str(item['correlationObservationCount'])

            dashboard_table_object['arryIndepVarObjects'] = sorted_arry_indep_var_objects
            array_dashboard_table_object.append(dashboard_table_object)
    ### END CREATE steps_count dashbaord object ###




    if len(array_dashboard_table_object) > 0:
        # new file name:
        # note: since user_id is string the code below needs convert back to int to use this `:04` shorthand
        user_data_table_array_json_file_name = f"data_table_objects_array_{int(user_id):04}.json"

        json_data_path_and_name = os.path.join(config.DASHBOARD_FILES_DIR, user_data_table_array_json_file_name)
        print(f"Writing file name: {json_data_path_and_name}")
        with open(json_data_path_and_name, 'w') as file:
            json.dump(array_dashboard_table_object, file)
        
        db_session = DatabaseSession()
        user_obj = db_session.get(Users,user_id)
        if user_obj.location_permission_ws==True:
            # create user_location_day csv file
            df_user_locations_day = create_df_daily_user_location_consecutive(user_id)

            # save csv file for user
            csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_user_city_by_day.csv")
            df_user_locations_day.to_csv(csv_path_and_filename)
        wrap_up_session(db_session)
        logger_ws_utilities.info(f"- WSAS COMPLETED dashboard file for user: {user_id} -")
        logger_ws_utilities.info(f"- WSAS COMPLETED dashboard file path: {json_data_path_and_name} -")
    else:
        logger_ws_utilities.info(f"- WSAS COMPLETED dashboard file for user: {user_id} -- -")
        logger_ws_utilities.info(f"- WSAS COMPLETED - NOT enough - dashboard data to produce a file for this user -")


def create_data_source_object_json_file(user_id, time_stamp_str=datetime.now().strftime('%Y%m%d-%H%M')):
    logger_ws_utilities.info(f"- WSAS creating data source object file for user: {user_id} -")
    list_data_source_objects = []


    #get user's apple health record count
    # keys to data_source_object_apple_health must match WSiOS DataSourceObject
    data_source_object_apple_health={}
    data_source_object_apple_health['name']="Apple Health Data"
    # record_count_apple_health = sess.query(AppleHealthQuantityCategory).filter_by(user_id=current_user.id).all()
    
    # user_apple_health_dataframe_pickle_file_name = f"user_{int(user_id):04}_apple_health_dataframe.pkl"
    # pickle_data_path_and_name = os.path.join(config.DATAFRAME_FILES_DIR, user_apple_health_dataframe_pickle_file_name)
    # df_apple_health = pd.read_pickle(pickle_data_path_and_name)
    apple_health_record_count, earliest_date_str = get_apple_health_count_date(user_id)
    data_source_object_apple_health['recordCount'] = apple_health_record_count
    data_source_object_apple_health['earliestRecordDate'] = earliest_date_str

    # Convert the string to a datetime object
    time_stamp_str_converted_to_date_obj = datetime.strptime(time_stamp_str, '%Y%m%d-%H%M')
    # Format the datetime object to the desired string format
    formatted_last_update_date_str = time_stamp_str_converted_to_date_obj.strftime('%b %d, %Y %H:%M')


    data_source_object_apple_health['lastUpdate'] = formatted_last_update_date_str
    # data_source_object_apple_health['earliestRecordDate']="{:,}".format(len(df_apple_health))
    list_data_source_objects.append(data_source_object_apple_health)

    # note: since user_id is string the code below needs convert back to int to use this `:04` shorthand
    user_data_source_json_file_name = f"data_source_list_for_user_{int(user_id):04}.json"

    json_data_path_and_name = os.path.join(config.DATA_SOURCE_FILES_DIR, user_data_source_json_file_name)
    logger_ws_utilities.info(f"Writing file name: {json_data_path_and_name}")
    with open(json_data_path_and_name, 'w') as file:
        json.dump(list_data_source_objects, file)



