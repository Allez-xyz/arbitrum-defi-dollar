import os
import clickhouse_connect
import modal
import logging
import time
from dotenv import load_dotenv
import requests
import pandas as pd
import time
from datetime import datetime, timedelta, timezone

import polars as pl
import clickhouse_connect
from multicall import Call, Multicall

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

custom_image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install_from_requirements("requirements.txt")
)

app = modal.App(name="fetch_arbitrum_blocks", image=custom_image)


@app.function(
    timeout=60 * 60 * 5,
    concurrency_limit=1,
    schedule=modal.schedule.Cron("10 */6 * * *"),
    secrets=[
        modal.Secret.from_name("clickhouse_nuevo"),
        modal.Secret.from_name("etherscan_keys")
    ])
def main():
    ARBISCAN_KEY = os.environ["arbiscan_key"]

    clickhouse_client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST"),
        username=os.getenv("CLICKHOUSE_USER"),
        password=os.getenv("CLICKHOUSE_PASSWORD"),
        port=8443,
    )

    clickhouse_client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST"),
            username=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            port=8443,
        )

    latest_ts = clickhouse_client.query_df("""
        SELECT max(hour) as latest_timestamp
        FROM ingest.arbitrum_hourly_blocktimes
    """)
    latest_ts.latest_timestamp.values[0]

    latest_timestamp_np = latest_ts.latest_timestamp.values[0]

    # Convert to datetime
    latest_datetime = pd.Timestamp(latest_timestamp_np).to_pydatetime()

    # Get current UTC time rounded down to the nearest hour
    now_utc = datetime.utcnow().replace(minute=0, second=0, microsecond=0)

    # Define start time (next hour after the latest timestamp)
    start_time = latest_datetime + timedelta(hours=1)

    # Check if start_time is before now_utc to avoid empty ranges
    if start_time > now_utc:
        hour_list = []
        print("No new hours to add.")
    else:
        # Generate the range of hours using pandas
        # hours_range = pd.date_range(start=start_time, end=now_utc, freq='H', tz='UTC')
        # hour_list = hours_range.strftime('%Y-%m-%dT%H:%M:%SZ').tolist()

        # Alternatively, using list comprehension:
        num_hours = int((now_utc - start_time).total_seconds() // 3600) + 1
        hour_list = [
            (start_time + timedelta(hours=i)).strftime('%Y-%m-%dT%H:%M:%SZ')
            for i in range(num_hours)
        ]


    def get_arbitrum_block(unix_timestamp, ARBISCAN_KEY):
        base_url = "https://api.arbiscan.io/api"

        # First API call: Get block number by timestamp
        params_block = {
            "module": "block",
            "action": "getblocknobytime",
            "timestamp": unix_timestamp,
            "closest": "after",  # Use 'after' to get the first block in the hour
            "apikey": ARBISCAN_KEY
        }
        response = requests.get(base_url, params=params_block, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data['result']


    results = []
    for hour in hour_list:
        timestamp = int(datetime.strptime(hour, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc).timestamp())
        first_block = get_arbitrum_block(timestamp,ARBISCAN_KEY)
        hour = datetime.strptime(hour, '%Y-%m-%dT%H:%M:%SZ')
        hour_result = [timestamp, hour, first_block]
        results.append(hour_result)
        time.sleep(1)

    clickhouse_client.insert(table='ingest.arbitrum_hourly_blocktimes',
                            data=results,
                            column_names=['timestamp',
                                        'hour',
                                        'first_block'])