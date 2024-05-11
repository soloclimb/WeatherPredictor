import pytest
import json
import boto3
import unittest
import botocore
from unittest.mock import patch, Mock, MagicMock
from io import BytesIO

from ....src.scripts.lambda_functions.extraction.open_meteo import (
    calc_request_weight,
    retrieve_historical,
    check_tasks_file_structure, 
    change_schedule_rate,
    load_raw
)


@pytest.mark.parametrize(("task", "expected_weight"), [({
                "hourly_features": ["temperature_2m", "relative_humidity_2m", 
                                    "apparent_temperature", "precipitation",
                                    "rain", "weather_code",
                                    "surface_pressure", "cloud_cover",
                                    "wind_speed_10m", "wind_direction_10m", 
                                    "soil_temperature_7_to_28cm"],
                "daily_features": ["temperature_2m_max",
                                   "temperature_2m_min",
                                   "precipitation_hours"],
                "latitude": "25.761681",
                "longitude": "-80.191788",
                "start_date": "2024-04-01",
                "end_date": "2024-04-26",
                "timezone": "GMT",
                    }, "2.5"),
            ({
                "hourly_features": ["temperature_2m", "relative_humidity_2m", 
                                    "apparent_temperature", "precipitation",
                                    "rain", "weather_code",
                                    "surface_pressure", "cloud_cover",
                                    "wind_speed_10m", "wind_direction_10m", 
                                    "soil_temperature_7_to_28cm"],
                "daily_features": ["temperature_2m_max",
                                   "temperature_2m_min",
                                   "precipitation_hours"],
                "latitude": "25.761681",
                "longitude": "-80.191788",
                "start_date": "2014-03-23",
                "end_date": "2024-03-26",
                "timezone": "GMT",
                    }, "365.6"),
            ({
                "hourly_features": ["temperature_2m", "relative_humidity_2m", 
                                    "apparent_temperature", "precipitation",
                                    "rain", "weather_code",
                                    "surface_pressure", "cloud_cover",
                                    "wind_speed_10m", "wind_direction_10m", 
                                    "soil_temperature_7_to_28cm"],
                "daily_features": ["temperature_2m_max",
                                   "temperature_2m_min",
                                   "precipitation_hours"],
                "latitude": "25.761681",
                "longitude": "-80.191788",
                "start_date": "2013-01-05",
                "end_date": "2024-04-26",
                "timezone": "GMT",
                    }, "412.9"
            ),
            ({
                "hourly_features": ["temperature_2m", "relative_humidity_2m", 
                                    "apparent_temperature", "precipitation",
                                    "rain", "weather_code",
                                    "surface_pressure", "cloud_cover",
                                    "wind_speed_10m", "wind_direction_10m", 
                                    "soil_temperature_7_to_28cm"],
                "daily_features": ["temperature_2m_max",
                                   "temperature_2m_min",
                                   "precipitation_hours"],
                "latitude": "25.761681",
                "longitude": "-80.191788",
                "start_date": "2010-01-05",
                "end_date": "2024-04-26",
                "timezone": "GMT",
                    }, "522.5")])
def test_calc_open_meteo_request_weight(task, expected_weight):
    assert calc_request_weight(task=task, extraction_topic_arn="arn:aws:sns:...") == float(expected_weight)


@pytest.mark.parametrize(("tasks", "expected_url"), [
    ({"tasks": [{
        "hourly_features": ["temperature_2m", "relative_humidity_2m", 
                            "apparent_temperature", "precipitation",
                            "rain", "weather_code",
                            "surface_pressure", "cloud_cover",
                            "wind_speed_10m", "wind_direction_10m", 
                            "soil_temperature_7_to_28cm"],
        "daily_features": ["temperature_2m_max",
                           "temperature_2m_min",
                           "precipitation_hours"],
        "latitude": "52.52",
        "longitude": "13.41",
        "start_date": "2010-01-05",
        "end_date": "2024-04-26",
        "timezone": "GMT",
    }]}, "https://archive-api.open-meteo.com/v1/archive?latitude=52.52&longitude=13.41&start_date=2010-01-05&end_date=2024-04-26&hourly=temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,rain,weather_code,surface_pressure,cloud_cover,wind_speed_10m,wind_direction_10m,soil_temperature_7_to_28cm&daily=temperature_2m_max,temperature_2m_min,precipitation_hours&timezone=GMT")
])
@patch("requests.get")
@patch("boto3.client")
def test_retrieve_from_open_meteo(mock_client, get_call, tasks, expected_url):
    mock_publish = mock_client.return_value.publish
    get_call_mock = Mock()
    get_call_mock.return_value = ""
    get_call_mock.status_code = 200
    get_call_mock.reason = "bla bla"

    get_call_mock.json = lambda: {"mock": "json"}
    get_call.return_value = get_call_mock

    retrieve_historical(retrieval_tasks=tasks, extraction_topic_arn="arn:aws:sns:...")

    get_call.assert_called_with(expected_url)


empty_s3_object = {"Body": b'{}'}
invalid_file_content = b"{}"  
valid_file_content = json.dumps({ 
  "services": {
    "open_meteo": {
      "tasks": [{"hourly_features": ["temperature_2m", "..."]}]
    }
  }
}).encode()


@patch("boto3.client")
def test_empty_file(mock_client):
    mock_publish = mock_client.return_value.publish
    with pytest.raises(ValueError) as e:
        msg = "File from tasks bucket must contain json file with appropriate structure, even if there's no current tasks! len of file == 0"
        check_tasks_file_structure(s3_object=empty_s3_object, extraction_topic_arn="arn:aws:sns:...")
        assert msg == str(e)
        mock_publish.assert_called_once_with(
            TopicArn="arn:aws:sns:...",
            Message=msg
        )


@patch("boto3.client")
def test_missing_services_key(mock_client):
    mock_publish = mock_client.return_value.publish
    with pytest.raises(ValueError) as e:
        msg = "File from tasks bucket must contain json file with appropriate structure, even if there's no current tasks! 'services' object not found"
        check_tasks_file_structure(s3_object={"Body": invalid_file_content}, extraction_topic_arn="arn:aws:sns:...")
        assert msg == str(e)
        mock_publish.assert_called_once_with(
            TopicArn="arn:aws:sns:...",
            Message=msg
        )


@patch("boto3.client")
def test_valid_file(mock_client):
    mock_publish = mock_client.return_value.publish
    returned_content = check_tasks_file_structure(s3_object={"Body": valid_file_content}, extraction_topic_arn="arn:aws:sns:...")
    assert returned_content == json.loads(valid_file_content) 
    mock_publish.assert_not_called()

event_rule_name = "some name"
default_cron_string = "cron(5 0 * * ? *)"
frequent_cron_string = "cron(0,30 * * * ? *)"
default_rate = "default"
frequent_rate = "frequent"
extraction_topic_arn = "arn:aws:sns:..."

@patch("boto3.client")
def test_default_schedule_rate(mock_client):
    mock_put_rule = mock_client.return_value.put_rule
    mock_put_rule.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
    res = change_schedule_rate(rule_name=event_rule_name,
                               new_rate=default_rate,
                               extraction_topic_arn=extraction_topic_arn)
    mock_put_rule.assert_called_with(Name=event_rule_name,
                                     ScheduleExpression=default_cron_string,
                                     State='ENABLED')
    assert res == {
        "statusCode": 200,
        "result": mock_put_rule.return_value
    }

@patch("boto3.client")
def test_frequent_schedule_rate(mock_client):
    mock_put_rule = mock_client.return_value.put_rule
    mock_put_rule.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
    res = change_schedule_rate(rule_name=event_rule_name,
                               new_rate=frequent_rate,
                               extraction_topic_arn=extraction_topic_arn)
    mock_put_rule.assert_called_with(Name=event_rule_name,
                                     ScheduleExpression=frequent_cron_string,
                                     State='ENABLED')
    assert res == {
        "statusCode": 200,
        "result": mock_put_rule.return_value
    }    


@patch("boto3.client")
def test_errored_schedule_rate(mock_client):
    mock_put_rule = mock_client.return_value.put_rule
    mock_publish = mock_client.return_value.publish

    mock_put_rule.return_value = {"ResponseMetadata": {"HTTPStatusCode": 400},
                                  "Error": {
                                      "Message": "error occured",
                                      "Type": "Sender"
                                  }}
    res = change_schedule_rate(rule_name=event_rule_name,
                               new_rate=frequent_rate,
                               extraction_topic_arn=extraction_topic_arn)
    mock_put_rule.assert_called_with(Name=event_rule_name,
                                     ScheduleExpression=frequent_cron_string,
                                     State='ENABLED')
    
    mock_publish.assert_called_once_with(
            TopicArn=extraction_topic_arn,
            Message="Error occured in change_schedule_rate(): " + 
            mock_put_rule.return_value['Error']['Message']
        )
    
    assert res == {
        "statusCode": 400,
        "message": "error occured",
        "type": "Sender",
        "result": mock_put_rule.return_value
    }    


load_raw_dct_data = {"data": "mocking data"}
load_raw_bucket_name = "someBucket"
load_raw_task = {"hourly_features": ["temperature_2m", "relative_humidity_2m", 
                                     "apparent_temperature", "precipitation",
                                     "rain", "weather_code",
                                     "surface_pressure", "cloud_cover",
                                     "wind_speed_10m", "wind_direction_10m", 
                                     "soil_temperature_7_to_28cm"],
                 "daily_features": ["temperature_2m_max",
                                    "temperature_2m_min",
                                    "precipitation_hours"],
                 "latitude": "25.761681",
                 "longitude": "-80.191788",
                 "start_date": "2024-04-01",
                 "end_date": "2024-04-26",
                 "timezone": "GMT",
}
expected_load_raw_dct_data = bytes(json.dumps(load_raw_dct_data), 
                                   encoding='utf-8')

@patch("boto3.resource")
def test_success_load_raw(mock_resource):
    mock_bucket = MagicMock()
    mock_resource.return_value.Bucket.return_value = mock_bucket

    load_raw(dct_data=load_raw_dct_data,
             bucket_name=load_raw_bucket_name,
             task=load_raw_task,
             extraction_topic_arn=extraction_topic_arn)

    mock_bucket.put_object.assert_called_once_with(
        Bucket=load_raw_bucket_name, Body=expected_load_raw_dct_data
    )

missing_keys_task = {
                 "daily_features": ["temperature_2m_max",
                                    "temperature_2m_min",
                                    "precipitation_hours"],
                 "latitude": "25.761681",
                 "longitude": "-80.191788"
}

@patch("boto3.client")
@patch("boto3.resource")
def test_failure_missing_keys(mock_resource, mock_client):
    mock_publish = mock_client.return_value.publish
    mock_bucket = MagicMock()
    mock_resource.return_value.Bucket.return_value = mock_bucket

    load_raw(dct_data=load_raw_dct_data,
             bucket_name=load_raw_bucket_name,
             task=missing_keys_task,
             extraction_topic_arn=extraction_topic_arn)

    mock_publish.assert_called_once_with(
            TopicArn=extraction_topic_arn,
            Message="Error occured in load_raw(): 'start_date'"
        )
