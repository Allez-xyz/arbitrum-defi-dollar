import json
from web3 import Web3
from dotenv import load_dotenv
import os
from multicall import Call, Multicall
import clickhouse_connect
import modal
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

custom_image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install_from_requirements("requirements.txt")
)

app = modal.App(name="fetch_dolomite_defidollar", image=custom_image)


@app.function(
    timeout=60 * 60 * 5,
    concurrency_limit=1,
    schedule=modal.schedule.Cron("25 */6 * * *"),
    secrets=[
        modal.Secret.from_name("clickhouse_nuevo"),
        modal.Secret.from_name("RPC_ENDPOINTS")
    ],
    mounts=[modal.Mount.from_local_file("abis/dolomite/abi.json", remote_path="/root/abis/dolomite/abi.json")]

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
            LEFT JOIN (SELECT distinct block FROM ingest.dolomite_defidollar_ingest) m
            on cast(t.first_block as Int64) = cast(m.block as Int64)
            WHERE m.block < 100
            AND t.hour >= date('2024-01-01')
            ORDER BY t.timestamp desc
    """)

    if blocks_to_query.shape[0] == 0:
        print('Nothing to query, done.')
        return 0

    # Run query_and_upload for each (silo_addresses, block) pair in parallel
    for result in query_and_upload.map(blocks_to_query.block.values):
        pass

@app.function(
    timeout=60 * 15 * 1,
    concurrency_limit=1,
    # retries=modal.Retries(
    #     max_retries=4,
    #     backoff_coefficient=2.0,
    #     initial_delay=1.0,
    # ),
    secrets=[
        modal.Secret.from_name("clickhouse_nuevo"),
        modal.Secret.from_name("RPC_ENDPOINTS")
    ],
    mounts=[modal.Mount.from_local_file("abis/dolomite/abi.json", remote_path="/root/abis/dolomite/abi.json")]

)
def query_markets(block):
    block = int(block)
    """
    Query the provided compound markets
    """
    rpc_url = os.getenv("arbitrum")
    web3 = Web3(Web3.HTTPProvider(rpc_url))

    abi_path = "/root/abis/dolomite/abi.json"
    if not os.path.exists(abi_path):
        logger.error(f"ABI file not found at {abi_path}")
        return []  # Early exit or handle as needed

    # Load the ABI file
    with open(abi_path) as f:
        abi = json.load(f)

    # Define addresses
    market_indexes = [5, # USDT
                    17, # USDC
                    2, # USDC.e
                    54, # USDe
                    1, # DAI
                    ]

    def convert_values(value):
        return value 

    dolomite_address = web3.to_checksum_address('0x6Bd780E7fDf01D77e4d475c821f1e7AE05409072')
    dolomite = web3.eth.contract(address=dolomite_address, abi=abi)

    results = []
    for index in market_indexes:
        getMarketWithInfo = dolomite.functions.getMarketWithInfo(index).call(block_identifier=block)
        getMarketTotalPar = dolomite.functions.getMarketTotalPar(index).call(block_identifier=block)
        getMarketCachedIndex = dolomite.functions.getMarketCachedIndex(index).call(block_identifier=block)
        getEarningsRate = dolomite.functions.getEarningsRate().call(block_identifier=block)
        entry = [
            index ,
            getMarketWithInfo[0][0],
            block,
            getMarketCachedIndex[0],
            getMarketCachedIndex[1],
            getMarketCachedIndex[2],
            getMarketTotalPar[0],
            getMarketTotalPar[1],
            getMarketWithInfo[3][0],
            getEarningsRate[0]
        ]
        entry_names = [
            'assetIndex',
            'assetAddress',
            'block',
            'borrowIndex',
            'supplyIndex',
            'lastUpdateIndex',
            'borrowPar',
            'supplyPar',
            'borrowRatePerSecond',
            'getEarningsRate'
        ]

        results.append(entry)
    return(results)


@app.function(
    timeout=60 * 15 * 1,
    concurrency_limit=20,
    secrets=[
        modal.Secret.from_name("clickhouse_nuevo"),
        modal.Secret.from_name("RPC_ENDPOINTS")
    ],
    mounts=[modal.Mount.from_local_file("abis/dolomite/abi.json", remote_path="/root/abis/dolomite/abi.json")]
)
def query_and_upload(block):
    clickhouse_client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST"),
        username=os.getenv("CLICKHOUSE_USER"),
        password=os.getenv("CLICKHOUSE_PASSWORD"),
        port=8443,
    )
    logger.info(f"running {block}")
    market_data = query_markets.local(block)
    clickhouse_client.insert(
        "ingest.dolomite_defidollar_ingest",
        data=market_data,
        column_names=[
            "asset_index",
            "asset_address",
            "block",
            "borrow_index",
            "supply_index",
            "last_update_index",
            "borrow_par",
            "supply_par",
            "borrow_rate_per_second",
            "get_earnings_rate"
        ],
    )

    logger.info(f'block {block} complete')
