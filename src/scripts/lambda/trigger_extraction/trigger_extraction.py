import boto3
import requests

from json import loads
from datetime import datetime

# from os import path

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
#                 "hourly_features": ["temperature_2m","relative_humidity_2m", 
#                                     "apparent_temperature","precipitation","rain","weather_code",
#                                     "surface_pressure","cloud_cover","wind_speed_10m","wind_direction_10m","soil_temperature_7_to_28cm"],
#                 "daily_features": ["temperature_2m_max",
#                                    "temperature_2m_min","precipitation_hours"],
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


# Function that manages task - explained in note below
# permission like 'events:PutRule' needs to be added for this lambda

# Important note:
#   rule_name of EventBus rule that triggers this lambda needs to be added,
#   to adjust execution schedule,
#   based on amount of tasks, amount of api requests left

def change_schedule_rate(rule_name, new_rate):
    cron_string = None
    if new_rate == "default":
        cron_string = "cron(5 0 * * ? *)"
    elif new_rate == "frequent":
        cron_string = "cron(0,30 * * * ? *)"

    events = boto3.client("events")        

    res = events.put_rule(
            Name=rule_name,
            ScheduleExpression=cron_string,
            State='ENABLED'
    )
    
    return {
        "statusCode": 200,
        "result": res
    }


# def load_raw_open_meteo(data, bucket_name, path_to_dir, task):
#     s3 = boto3.resource("s3")
#     bucket = s3.Bucket(bucket_name)
    
#     filename = ("start_date=" + task["start_date"] + 
#                 "&end_date=" + task["end_date"])

#     dirname = path.join(path_to_dir, ("latitude=" + task["latitude"] + 
#                                       "&longitude=" + task["longitude"] + "/"))
    
#     full_filepath = path.join(dirname, filename + ".json")
    
#     matched_obj = list(bucket.objects.filter(Prefix=full_filepath))

#     if len(matched_obj) > 0:
#         timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
#         full_filepath = (dirname + filename + "_version_"  
#                          + timestamp + ".json")
        

def handler(event, context):
    s3 = boto3.client("s3")
    bucket_name = event["tasks_bucket"]
    tasks_file_key = event["tasks_file_key"]    
    s3_object = s3.get_object(Bucket=bucket_name, Key=tasks_file_key)

    tasks_file_content = check_tasks_file_structure(s3_object)
   
    daily_left = event["daily_left"]
    hourly_allowed = event["hourly_allowed"]
    by_minute_allowed = event["by_minute_allowed"]

    for service in tasks_file_content["services"].keys():
        if service == "open_meteo":
            for task in tasks_file_content["services"][service]["tasks"]:
                
                request_weight = calc_open_meteo_request_weight(task)

                minimal_requests = min([daily_left,
                                        hourly_allowed,
                                        by_minute_allowed])

                if request_weight > minimal_requests:
                    if minimal_requests == by_minute_allowed:
                        #some notifier needed -  sns topic etc.
                        print("request cannot be proccessed - request weight bigger that maximal allowed by minute", task)
                        return
                    else:
                        print("Api requests limit reached!")
                        return
                    
                api_response = retrieve_from_open_meteo(task)
                
                daily_left -= request_weight
                
                # load function call or trigger 

    # Decide what to do with raw data - trigger 
    # instant transformation acccording response structure
    # or store just json files in bucket 
    # why it is bad - you don't know what parameters are inside 
    # file, possibly if tasks repeated in scheduler - you store same info twice 



