import boto3
import requests

from json import loads, dumps
from time import time, sleep
from datetime import datetime

# Tip from oficial Open Meteo Pricing page:
# To calculate the number of API calls accurately,
# fractional counts are used. For example, 
# 15 weather variables count as 1.5 API calls,
# while 4 weeks of weather data count as 3.0 API calls.


def calc_open_meteo_request_weight(link_params):
    target_features = (link_params['hourly_features'] +
                       link_params["daily_features"])

    features_weight = len(target_features) * 0.1
    start_date = datetime.strptime(link_params["start_date"], "%Y-%m-%d")
    end_date = datetime.strptime(link_params["end_date"], "%Y-%m-%d")

    days_difference = end_date - start_date
    weeks_difference = days_difference / 7

    request_weight = (weeks_difference / 4) * 3 * features_weight

    return request_weight


def retrieve_from_open_meteo(link_params):
    for task in link_params["tasks"]:
        url = f"https://archive-api.open-meteo.com/v1/archive?latitude={task['latitude']}&longitude={task['longitude']}&start_date={task['start_date']}&end_date={task['end_date']}&hourly={','.join(task['hourly_features'])}&daily={','.join(task['daily_features'])}&timezone={task['timezone']}&tilt={task['tilt']}"
        response = requests.get(url)

        return response.json()

# Tasks file must follow json structure:
# services: {
#   service1:{
#       tasks:{}
#   }           
#   service2:{
#       tasks: {}
#   }
# }

# Simple example:
# {
#    "services": {
#        "open_meteo": {
#             "tasks": [{
#                 "hourly_features": ["temperature_2m","relative_humidity_2m", "apparent_temperature","precipitation","rain","weather_code","surface_pressure","cloud_cover","wind_speed_10m","wind_direction_10m","soil_temperature_7_to_28cm"],
#                 "daily_features": ["temperature_2m_max","temperature_2m_min","precipitation_hours"],
#                 "latitude": "25.761681",
#                 "longitude": "-80.191788",
#                 "start_date": "2024-03-23",
#                 "end_date": "2024-03-26",
#                 "timezone": "GMT",
#                 "tilt": "5"
#             }]
#     }}
# }


def check_tasks_file_structure(s3_object):
    general_error_str = "File from tasks bucket must contain json file with appropriate structure, even if there's no current tasks!"
    
    file_content = s3_object["Body"].read().decode("utf-8")
    
    if len(file_content) == 0:
        raise ValueError(general_error_str, "len of file == 0")
    
    file_content = loads(file_content)

    if "services" not in file_content:
        raise ValueError(general_error_str, "'services' object not found")

    for service in file_content["services"]:
        if "tasks" not in service:
            raise ValueError(general_error_str,
                             "'tasks' object not found in service " + service)

    return file_content


def handler(event, context):
    s3 = boto3.client("s3")
    bucket_name = event["tasks_bucket"]
    tasks_file_key = event["tasks_file_key"]    
    s3_object = s3.get_object(Bucket=bucket_name, Key=tasks_file_key)

    tasks_file_content = check_tasks_file_structure(s3_object)
   
    daily_left = event["daily_left"]
    hourly_left = event["hourly_left"]
    by_minute_left = event["by_minute_left"]
    
    prev_task_runtime = 0

    for service in tasks_file_content["services"].keys():
        if service == "open_meteo":
            for task in tasks_file_content["services"][service]["tasks"]:
                
                request_weight = calc_open_meteo_request_weight(task)
                if request_weight > min([daily_left, 
                                         hourly_left, by_minute_left]):
                    if min([daily_left,
                            hourly_left, by_minute_left]) == by_minute_left:
                        
                        sleep(60 - prev_task_runtime) if prev_task_runtime <= 60 else sleep(0)
                    else:
                        print("Api requests limit reached!")
                        return
                retrieve_from_open_meteo(task)
                # load to raw s3 function
                # implement logic for counting requests left


# Ideas for raw data filenames
# filename = "latitude=" + task["latitude"] + "&longitude=" + task["longitude"] + "&start_date=" + task["start_date"]+ "&end_date=" +task["end_date"] + ".json"

