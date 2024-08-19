
# What Sticks 13 Data Tools

![What Sticks Logo](https://what-sticks.com/website_images/wsLogo180.png)

## Description
These packages provide calculations and data reshaping data for the What Sticks suite of applications. What Sticks 13 Data Tools consists of two custom Python packages, ws_analysis and ws_utilities. Mainly here we are calculating correlations and creating dataframe's either for .csv/.pkl files or for direct use in another process.


## Installation Instructions
To install the What Sticks 13 Data Tools, clone the repository and install the required dependencies:
```
git clone [repository-url]
cd WhatSticks13DataTools
pip install -e .
```


## Usage
After installation, import the modules into your Python projects as needed:

```python
from ws_analysis import create_user_qty_cat_df, create_user_workouts_df, \
    create_df_daily_sleep, \
    create_df_n_minus1_daily_sleep, \
    create_df_daily_steps, \
    create_df_n_minus1_daily_steps, \
    create_df_daily_heart_rate, \
    create_df_n_minus1_daily_heart_rate, \
    create_df_daily_workout_duration, \
    create_df_daily_workout_duration_dummies, \
    corr_sleep_steps, \
    corr_sleep_heart_rate, corr_sleep_workouts, \
    corr_workouts_sleep, \
    corr_workouts_steps, corr_workouts_heart_rate, corr_sleep_workout_dummies
from ws_utilities import interpolate_missing_dates_exclude_references, \
    add_weather_history
```


## Features
All features are specific to What Sticks Platform Applicaitons and leverage the WS11Core library to map to database and directories for resources
- create dataframes
- calculate correlations
- user dataframes for the website
- create pickle dataframes to lessen the need to make database calls


## Note on ws_utilities
The subdirectories within ws_utilities will be labeled by the application within the platform that first required the process. It will start with a main.py. The intention is that these processes will be reused and to not duplicate difficult or complicated tasks. As we develop better ways or run into problems we want to fix them in one place.

## :page_with_curl: Documentation (General) 

### Communicating with the What Sticks Database and using database sessions
Most functions in WS11DataTools assume that if there is a connection with the database the database session should be passed in as an argument. The What Sticks convention is to have an object called `db_session` which will be created by the source application. The WS11DataTools function will then use the session but will not commit/rollback and close, that will be done by the source application that calls the function.


## :page_with_curl: Documentation (ws_analysis) 
create_df_daily_<DATA_ELEMENT> the date column is expected to be of datetime.date()
### common
#### create_user_df.py
create_df_daily_user_location_consecutive 
- purpose: used to create a user's daily location_id so that weather correlations can be calculated.
- parameters: 
  - user_id: integer
  - start_date: string in format `%Y-%m-%d`
  - end_date: string in format `%Y-%m-%d`
- returns a dataframe with date and location_id

### daily_dfs

#### user_location_day.py
create_df_daily_user_location_consecutive
- purpose to create a consecutive series of historical dates and location_id for the user based on the data they have.
- if less than 5 days are found then extend_historically_user_location_date is triggered

extend_historically_user_location_date
- required paramter: df_user_location_date (dataframe object with date and location_id columns)
- optional parameter: back_to_date (datetime.date object)
  - if back_to_date is provided the function will extend historically back to the date. Otherwise it will extend back 14 days.


## :page_with_curl: Documentation (ws_utilities) 

### /api/
#### users.py > add_user_loc_day_process()
- function screens for UserLocationDay within same day before adding. In other words, a user will not be allowed to have multiple rows within the same day.


### /dashboard_table_obj/
These functions created the dictionaries that get converted to .json files that get sent to the WS11iOS app for the dashboard.
This funcationality was formerly in WS11AppleService

#### main.py > create_dashboard_table_object_json_file()
- required parameter: user_id (string)
- returns nothing but completes with the creation of .json file 
  - creates file in config.DASHBOARD_FILES_DIR folder
  - user_data_table_array_json_file_name = f"data_table_objects_array_{int(user_id):04}.json"

#### main.py > create_data_source_object_json_file()
- required parameter: user_id (string)
- returns nothing but completes with the creation of .json file 
  - creates file in config.DATA_SOURCE_FILES_DIR folder

### /visual_crossing_api/
#### vc_api_requests.py > request_visual_crossing_for_one_day()
- required paramters
  - location_db_obj: database object from Locations
  - date_1_start: date string in format `%Y-%m-%d`
- returns dictionary of weather objects from Visual Crossing for date_1_start

#### vc_api_requests.py > request_visual_crossing_for_last_30days()
- required paramters
  - location_db_obj: database object from Locations
- returns list of dictionary of weather objects from Visual Crossing from current day going back 30 days.


### /web/
- The website upload .zip uses a sequence of functions found in ws_utilites/api/admin.py. Important to note, even if no new users are added, the creation of a crosswalk table (df_crosswalk_users) will still create a df (df_crosswalk_users) that provides a mapping from existing user_ids to the user_ids found in the .zip file. Then convert the ones user_ids from the .zip file and append to the database correctly.

#### admin.py > create_df_crosswalk
- even if zip file has no new id's in users of locations it a df_crosswalk will be created


### /scheduler/

#### collect_yesterday_weather_history_from_visual_crossing()
#### add_weather_history(db_session, location_id, weather_data)


## Contributing

We welcome contributions to the What Sticks project.

For any queries or suggestions, please contact us at nrodrig1@gmail.com.
