import json
from web3 import Web3
from dotenv import load_dotenv
import os
import clickhouse_connect
import modal
import logging
import json
import time
import requests
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

custom_image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install_from_requirements("requirements.txt")
)

app = modal.App(name="fetch_fluid_defidollar", image=custom_image)


@app.function(
    timeout=60 * 60 * 5,
    concurrency_limit=1,
    secrets=[
        modal.Secret.from_name("clickhouse_nuevo"),
        modal.Secret.from_name("RPC_ENDPOINTS")
    ],
    schedule=modal.schedule.Cron("25 */6 * * *"),
    mounts=[modal.Mount.from_local_file("datacapture/abis/fluid/fluid_lending.json", remote_path="/root/abis/fluid/fluid_lending.json"),
            modal.Mount.from_local_file("datacapture/abis/fluid/fluid_vault.json", remote_path="/root/abis/fluid/fluid_vault.json")]
    
)
def main():
    rpc_url = os.getenv("arbitrum")
    web3 = Web3(Web3.HTTPProvider(rpc_url))

    clickhouse_client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST"),
        username=os.getenv("CLICKHOUSE_USER"),
        password=os.getenv("CLICKHOUSE_PASSWORD"),
        port=8443,
    )

    blocks_to_query = clickhouse_client.query_df("""
        SELECT t.timestamp, t.hour, t.first_block as block, m.block
        FROM ingest.arbitrum_hourly_blocktimes t
        LEFT JOIN (SELECT distinct block FROM ingest.fluid_defidollar_vault_ingest) m
        on cast(t.first_block as Int64) = cast(m.block as Int64)
        WHERE m.block < 100
        AND t.hour > date('2025-01-01')
        ORDER BY t.timestamp desc
    """)

    if blocks_to_query.shape[0] == 0:
        print('Nothing to query, done.')
        return 0

    for result in query_and_upload.map(blocks_to_query.block.values):
        pass

@app.function(
    timeout=60 * 15 * 1,
    concurrency_limit=1,
    secrets=[
        modal.Secret.from_name("clickhouse_nuevo"),
        modal.Secret.from_name("RPC_ENDPOINTS")
    ],
    mounts=[modal.Mount.from_local_file("datacapture/abis/fluid/fluid_lending.json", remote_path="/root/abis/fluid/fluid_lending.json"),
            modal.Mount.from_local_file("datacapture/abis/fluid/fluid_vault.json", remote_path="/root/abis/fluid/fluid_vault.json")]
)
def query_markets(block):
    block = int(block)
    rpc_url = os.getenv("arbitrum")
    web3 = Web3(Web3.HTTPProvider(rpc_url))

    # Query F Tokens

    ftoken_abi_path = "/root/abis/fluid/fluid_lending.json"
    vault_abi_path = "/root/abis/fluid/fluid_vault.json"
    # Load the ABI file
    with open(ftoken_abi_path) as f:
        ftoken_abi = json.load(f)
    with open(vault_abi_path) as f:
        vault_abi = json.load(f)

    fluid_lending_market = web3.eth.contract(web3.to_checksum_address('0xdF4d3272FfAE8036d9a2E1626Df2Db5863b4b302'),
                                            abi=ftoken_abi)
    fluid_vaults = web3.eth.contract(web3.to_checksum_address('0xD6373b375665DE09533478E8859BeCF12427Bb5e'),
                                            abi=vault_abi)
    getFTokensEntireData = fluid_lending_market.functions.getFTokensEntireData().call(block_identifier = block)
    ftokendata = [list(tup) for tup in getFTokensEntireData]
    ftoken_index = [3, #name
                    4, #symbol
                    5, #decimals
                    6, #underlying
                    7, #totalAssets
                    8, #totalSupply
                    11,#rewardsRate
                    12 #supplyRate
                    ]
    ftokendata_selected = [[row[i] for i in ftoken_index] + [block] for row in ftokendata]

    v = fluid_vaults.functions.getVaultsEntireData().call(block_identifier=block)

    # Vault index
    vault = 2

    # Extract and organize data for each vault
    vault_data = []
    for sublist in v:
        vault_entry = [
            sublist[0],         # vault address
            sublist[6][1],      # total borrowed
            sublist[5][9],      # borrow rate
            sublist[3][9][0],   # debt token
            sublist[3][8][0],   # collateral token
            block                      # block number
        ]
        vault_data.append(vault_entry)

    return ftokendata_selected, vault_data


@app.function(
    timeout=60 * 15 * 1,
    concurrency_limit=1,
    secrets=[
        modal.Secret.from_name("clickhouse_nuevo"),
        modal.Secret.from_name("RPC_ENDPOINTS")
    ],
    mounts=[modal.Mount.from_local_file("datacapture/abis/fluid/fluid_lending.json", remote_path="/root/abis/fluid/fluid_lending.json"),
            modal.Mount.from_local_file("datacapture/abis/fluid/fluid_vault.json", remote_path="/root/abis/fluid/fluid_vault.json")]
    )
def query_and_upload(block):
    block = int(block)
    clickhouse_client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST"),
        username=os.getenv("CLICKHOUSE_USER"),
        password=os.getenv("CLICKHOUSE_PASSWORD"),
        port=8443,
    )
    logger.info(f"running {block}")

    #### If query_markets doesn't work, lets skip for now

    ftokendata, vault_data = query_markets.local(block)
    # print('ran')
    debug = False

    clickhouse_client.insert(table='ingest.fluid_defidollar_ftoken_ingest',
                            data=ftokendata,
                            column_names = [  
                                "name"
                                ,"symbol"
                                ,"decimals"
                                ,"underlying"
                                ,"totalAssets"
                                ,"totalSupply"
                                ,"rewardsRate"
                                ,"supplyRate"
                                ,"block"
                            ])

    clickhouse_client.insert(table='ingest.fluid_defidollar_vault_ingest',
                            data=vault_data,
                            column_names = [  
                            "vault"
                            ,"total_borrowed"
                            ,"borrow_rate"
                            ,"debt_token"
                            ,"collateral_token"
                            ,"block"
                            ])

    logger.info(f'block {block} complete')

@app.function(
    timeout=60 * 15 * 1,
    concurrency_limit=1,
    schedule=modal.Cron("12 * * * *"),
    secrets=[
        modal.Secret.from_name("clickhouse_nuevo"),
    ])
def query_api():

    clickhouse_client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST"),
        username=os.getenv("CLICKHOUSE_USER"),
        password=os.getenv("CLICKHOUSE_PASSWORD"),
        port=8443,
    )


    now = int(time.time())
    url = "https://api.fluid.instadapp.io/v2/42161/vaults"

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an error for bad status codes
        data = response.json()       # Assuming the response is in JSON format
        print(data)
    except requests.exceptions.RequestException as e:
        print("An error occurred:", e)

    fluid_api_df = pd.json_normalize(response.json())
    fluid_api_df.columns = fluid_api_df.columns.str.replace('.', '_', regex=False)
    fluid_api_df['ingest_timestamp'] = now

    clickhouse_client.insert_df(table='ingest.fluid_defidollar_api',df=fluid_api_df)