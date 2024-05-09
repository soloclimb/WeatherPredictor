import pytest
import json
import boto3
from unittest.mock import patch, Mock, MagicMock
from io import BytesIO

from ....src.scripts.lambda_functions.extraction.open_meteo import (
    calc_request_weight,
    retrieve_historical,
    check_tasks_file_structure
)


@pytest.mark.parametrize(("link_params", "expected_weight"), [({
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
def test_calc_open_meteo_request_weight(link_params, expected_weight):
    assert calc_request_weight(link_params=link_params) == float(expected_weight)


@pytest.mark.parametrize(("link_params", "expected_url"), [
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
def test_retrieve_from_open_meteo(get_call, link_params, expected_url):
    get_call_mock = Mock()
    get_call_mock.json = lambda: {"mock": "json"}
    get_call.return_value = get_call_mock

    retrieve_historical(link_params=link_params)

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
        check_tasks_file_structure(empty_s3_object, "arn:aws:sns:...")
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
        check_tasks_file_structure({"Body": invalid_file_content}, "arn:aws:sns:...")
        assert msg == str(e)
        mock_publish.assert_called_once_with(
            TopicArn="arn:aws:sns:...",
            Message=msg
        )


@patch("boto3.client")
def test_valid_file(mock_client):
    mock_publish = mock_client.return_value.publish
    returned_content = check_tasks_file_structure({"Body": valid_file_content}, "arn:aws:sns:...")
    assert returned_content == json.loads(valid_file_content) 
    mock_publish.assert_not_called()


