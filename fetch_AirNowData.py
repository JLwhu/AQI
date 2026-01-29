import requests
import pandas as pd
from typing import Optional, Dict, Union
import logging

logging.basicConfig(
    level=logging.INFO,
    filename="AQI.log",
    filemode="a",   # append; use "w" to overwrite
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)
from datetime import datetime, timedelta

import json
import csv
import re

def flatten_dict(d, parent_key="", sep="_"):
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items
def json_txt_to_csv(input_txt, output_csv):
    """
    Convert appended JSON records in a txt file to a CSV file.
    Assumes JSON blocks are lists of dictionaries.
    """

    all_records = []

    with open(input_txt, "r", encoding="utf-8") as f:
        content = f.read()

    # Extract JSON list blocks
    json_blocks = re.findall(r"\[.*?\]", content, re.DOTALL)

    for block in json_blocks:
        try:
            records = json.loads(block)
            if isinstance(records, list):
                all_records.extend(records)
        except json.JSONDecodeError:
            continue

    if not all_records:
        print("No valid JSON records found.")
        return

    flattened_records = [flatten_dict(rec) for rec in all_records]

    # Collect all fieldnames dynamically
    fieldnames = []
    for rec in flattened_records:
        for key in rec.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    # Write CSV
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(flattened_records)

    print(f"Converted {len(flattened_records)} records → {output_csv}")


def fetch_airnow_forecast(
        zip_code: str,
        date: str,
        distance: int = 25,
        api_key: str = "54F0EE81-7325-4612-B5DD-6A4C3A6F1838",
        format_type: str = "application/json",
        base_url: str = "https://www.airnowapi.org/aq/forecast/zipCode/"
) -> Union[pd.DataFrame, Dict, str, None]:
    """
    Takes a Zip code, date (optional), and distance (optional) and returns the air quality forecast.

    Parameters:
    -----------
    zip_code : str
        Zip code
    date : str
        Date of forecast. If date is omitted, the current forecast is returned.
    distance : int, optional
        If no reporting area is associated with the specified Zip Code,
        return a forecast from a nearby reporting area within this distance (in miles)
        default 25
    api_key : str, optional
        Unique API key, associated with the AirNow user account.
    format_type : str, optional
        Format of the payload file returned.
        Options:
            CSV (text/csv)
            JSON (application/json)
            XML (application/xml)
    base_url : str, optional
        API基础URL

    Returns:
    --------
    Union[pd.DataFrame, Dict, str, None]
        根据format_type返回不同格式的数据：
        - "text/csv": 返回pandas DataFrame
        - "application/json": 返回解析后的字典
        - "application/xml": 返回XML字符串
        - 请求失败时返回None

    Raises:
    -------
    ValueError
        当参数无效时
    """

    # 参数验证
    if not zip_code or not isinstance(zip_code, str):
        raise ValueError("zip_code must be a non-empty string")

    if not date or not isinstance(date, str):
        raise ValueError("date must be a non-empty string in format 'YYYY-MM-DD'")

    if not isinstance(distance, int) or distance < 0:
        raise ValueError("distance must be a non-negative integer")

    valid_formats = ["text/csv", "application/json", "application/xml"]
    if format_type not in valid_formats:
        raise ValueError(f"format_type must be one of {valid_formats}")

    # 构建请求URL
    params = {
        "format": format_type,
        "zipCode": zip_code,
        "date": date,
        "distance": distance,
        "API_KEY": api_key
    }

    try:
        logger.info(f"Requesting AirNow forecast data for zip_code={zip_code}, date={date}")
        logger.debug(f"Request URL: {base_url}")
        logger.debug(f"Parameters: { {k: v for k, v in params.items() if k != 'API_KEY'} }")

        # 发送请求
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()

        logger.info(f"Request successful. Status code: {response.status_code}")

        # 根据格式处理响应
        if format_type == "text/csv":
            # 处理CSV数据
            import io
            csv_data = io.StringIO(response.text)
            df = pd.read_csv(csv_data)
            logger.info(f"CSV data loaded. Shape: {df.shape}")
            return df

        elif format_type == "application/json":
            # 处理JSON数据
            data = response.json()
            logger.info(f"JSON data loaded. Type: {type(data)}")
            return data

        elif format_type == "application/xml":
            # 返回XML字符串
            logger.info(f"XML data received. Length: {len(response.text)} characters")
            return response.text

    except requests.exceptions.Timeout:
        logger.error("Request timed out after 30 seconds")
        return None
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error occurred: {e}")
        logger.error(f"Response content: {response.text[:200]}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error occurred: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
        return None


def fetch_airnow_forecast_with_retry(
        zip_code: str,
        date: str,
        distance: int = 25,
        api_key: str = "54F0EE81-7325-4612-B5DD-6A4C3A6F1838",
        format_type: str = "text/csv",
        max_retries: int = 3,
        retry_delay: int = 2
) -> Union[pd.DataFrame, Dict, str, None]:
    """
    Parameters:
    -----------
    max_retries : int
        最大重试次数
    retry_delay : int
        重试延迟时间（秒）

    Returns:
    --------
    Union[pd.DataFrame, Dict, str, None]
        根据format_type返回不同格式的数据：
        - "text/csv": 返回pandas DataFrame
        - "application/json": 返回解析后的字典
        - "application/xml": 返回XML字符串
        - 请求失败时返回None

    Raises:
    -------
    ValueError
        当参数无效时
    """


    import time

    for attempt in range(max_retries):
        try:
            result = fetch_airnow_forecast(
                zip_code=zip_code,
                date=date,
                distance=distance,
                api_key=api_key,
                format_type=format_type
            )

            if result is not None:
                return result
            elif attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt + 1} failed, retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)

        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed with error: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)

    logger.error(f"All {max_retries} attempts failed")
    return None

def fetch_arinow_observation_MonitoringSite(
        bbox: str,
        startdate: str,
        enddate: str,
        parameters: str,
        datatype: str,
        monitortype: int = 0,
        format: str = "application/json",
        api_key: str = "54F0EE81-7325-4612-B5DD-6A4C3A6F1838",
        base_url: str = "https://www.airnowapi.org/aq/data/",
        verbose: int = 1,
        includerawconcentrations: int = 1
) -> Union[pd.DataFrame, Dict, str, None]:
    # 构建请求URL
    params = {
        "bbox": bbox,
        "startdate": startdate,
        "enddate": enddate,
        "format": format,
        "parameters": parameters,
        "datatype": datatype,
        "monitortype": monitortype,
        "API_KEY": api_key,
        "verbose": verbose,
        "includerawconcentrations": includerawconcentrations
    }


    try:
        logger.info(f"Requesting AirNow observation data Monitor site for bbox={bbox}")
        logger.debug(f"Request URL: {base_url}")
        logger.debug(f"Parameters: { {k: v for k, v in params.items() if k != 'API_KEY'} }")

        # 发送请求
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()

        logger.info(f"Request successful. Status code: {response.status_code}")

        # 根据格式处理响应
        if format == "text/csv":
            # 处理CSV数据
            import io
            csv_data = io.StringIO(response.text)
            df = pd.read_csv(csv_data)
            logger.info(f"CSV data loaded. Shape: {df.shape}")
            return df

        elif format == "application/json":
            # 处理JSON数据
            data = response.json()
            logger.info(f"JSON data loaded. Type: {type(data)}")
            return data

        elif format == "application/xml":
            # 返回XML字符串
            logger.info(f"XML data received. Length: {len(response.text)} characters")
            return response.text

    except requests.exceptions.Timeout:
        logger.error("Request timed out after 30 seconds")
        return None
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error occurred: {e}")
        logger.error(f"Response content: {response.text[:200]}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error occurred: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
        return None

if __name__ == "__main__":
    print("\n=== 1. Get Forcast Data 15224 ===")
    logger.info(f"\n=== 1. Get Forcast Data 15224 ===")
    forcast_date = datetime.now().strftime("%Y-%m-%d")
    data_with_retry = fetch_airnow_forecast_with_retry(
        zip_code="15224",
        date=forcast_date,
        distance=150,
        format_type="application/json",
        max_retries=2
    )

    # Append result to a txt file
    output_file = "airnow_forecast.json"

    with open(output_file, "a", encoding="utf-8") as f:
        #f.write(f"\n=== Forecast Date: {forcast_date} ===\n")
        if isinstance(data_with_retry, (dict, list)):
            f.write(json.dumps(data_with_retry, indent=2))
        else:
            f.write(str(data_with_retry))
        f.write("\n")

    print(f"Forecast data 15224 appended to {output_file}")

    print("\n=== 2. Get Forcast Data 15025 ===")
    logger.info(f"\n=== 2. Get Forcast Data 15025 ===")
    forcast_date = datetime.now().strftime("%Y-%m-%d")
    data_with_retry = fetch_airnow_forecast_with_retry(
        zip_code="15025",
        date=forcast_date,
        distance=150,
        format_type="application/json",
        max_retries=2
    )

    # Append result to a txt file
    output_file = "airnow_forecast.json"

    with open(output_file, "a", encoding="utf-8") as f:
        #f.write(f"\n=== Forecast Date: {forcast_date} ===\n")
        if isinstance(data_with_retry, (dict, list)):
            f.write(json.dumps(data_with_retry, indent=2))
        else:
            f.write(str(data_with_retry))
        f.write("\n")

    print(f"Forecast data 15025 appended to {output_file}")

    json_txt_to_csv(
        input_txt="airnow_forecast.json",
        output_csv="airnow_forecast.csv"
    )

    print("\n=== 3. 24 Hour Observations by Monitoring Site By geographic bounding box===")
    logger.info(f"\n=== 3. 24 Hour Observations by Monitoring Site By geographic bounding box===")
    enddate = datetime.now().strftime("%Y-%m-%dT%H:%M")
    startdate = (datetime.now() + timedelta(days=-1)).strftime("%Y-%m-%dT%H:%M")
    json_site = fetch_arinow_observation_MonitoringSite(
        bbox="-80.84, 40.20, -79.69, 40.68",
        startdate=startdate,
        enddate=enddate,
        parameters="ozone,pm25",
        datatype="B",
        format="application/json",
    )
    # Append result to a txt file
    output_file = "airnow_24HSite.json"

    with open(output_file, "a", encoding="utf-8") as f:
        if isinstance(json_site, (dict, list)):
            f.write(json.dumps(json_site, indent=2))
        else:
            f.write(str(json_site))
        f.write("\n")

    json_txt_to_csv(
        input_txt="airnow_24HSite.json",
        output_csv="airnow_24HSite.csv"
    )
