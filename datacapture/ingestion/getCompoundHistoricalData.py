import json
from web3 import Web3
from dotenv import load_dotenv
import os
from multicall import Call, Multicall
import clickhouse_connect
import modal
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

custom_image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install_from_requirements("requirements_multicall.txt")
)

app = modal.App(name="fetch_compound_defidollar", image=custom_image)


@app.function(
    timeout=60 * 60 * 5,
    concurrency_limit=1,
    secrets=[
        modal.Secret.from_name("clickhouse_nuevo"),
        modal.Secret.from_name("RPC_ENDPOINTS")
    ],
        schedule=modal.schedule.Cron("25 */6 * * *"),

    # schedule=modal.schedule.Cron("5 * * * *")
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

    compound_markets_from_clickhouse = clickhouse_client.query_df("""
            WITH
                compound_markets AS (
                    SELECT 'USDC' AS market_name, '0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf' AS contract_address, 122080500 AS block_live
                    UNION ALL
                    SELECT 'USDC.e', '0xA5EDBDD9646f8dFF606d7448e414884C7d905dCA', 87335214
                    UNION ALL
                    SELECT 'USDT', '0xd98Be00b5D27fc98112BdE293e487f8D4cA57d07', 223796350
                )

            SELECT m.market_name, m.contract_address, t.timestamp, t.first_block as block
            FROM ingest.arbitrum_hourly_blocktimes t
            CROSS JOIN compound_markets m
            WHERE t.first_block > m.block_live
            AND t.hour >= date('2024-01-01')
            AND t.first_block not in (SELECT distinct block_number from ingest.compound_defidollar_ingest)
            ORDER BY t.timestamp desc
    """)

    if compound_markets_from_clickhouse.shape[0] == 0:
        print('Nothing to query, done.')
        return 0

    ### create chunks of silo_markets_from_clickhouse where each chunk is the data with the same block
    ### then do some sort of query_silos.map(chunk) for chunk in chunks
    grouped = compound_markets_from_clickhouse.groupby("block")
    block_silo_groups = []
    for block, group in grouped:
        addresses = group["contract_address"].unique().tolist()
        block_silo_groups.append((addresses, block))

    # Run query_and_upload for each (silo_addresses, block) pair in parallel
    for result in query_and_upload.starmap(block_silo_groups):
        # Each result corresponds to the return value of query_and_upload
        # (which is None in your case). You can handle logging here if needed.
        pass

@app.function(
    timeout=60 * 15 * 1,
    concurrency_limit=1,
    secrets=[
        modal.Secret.from_name("clickhouse_nuevo"),
        modal.Secret.from_name("RPC_ENDPOINTS")
    ],
)
def query_markets(compound_markets, block):
    """
    Query the provided compound markets
    """
    rpc_url = os.getenv("arbitrum")
    web3 = Web3(Web3.HTTPProvider(rpc_url))

    compPriceFeed = web3.toChecksumAddress("0xe7C53FFd03Eb6ceF7d208bC4C13446c76d1E5884")

    def convert_values(value):
        return value 

    utilization_calls = []
    for market in compound_markets:
        utilization_calls.append(
        Call(market, 'getUtilization()(uint256)', [(f'getUtilization_{market}',convert_values)]),
    )

    utilization_multicall = Multicall(
        utilization_calls
    ,_w3 = web3,block_id=block)

    utilizations = utilization_multicall()

    results = {}
    for market in compound_markets:
        utilization = utilizations[f'getUtilization_{market}']
        multi = Multicall([
            Call(market, 'totalBorrow()(uint256)', [('totalBorrow',convert_values)]),
            Call(market, 'totalSupply()(uint256)', [('totalSupply',convert_values)]),
            Call(market, 'trackingIndexScale()(uint256)', [('trackingIndexScale',convert_values)]),
            Call(market, 'borrowKink()(uint256)', [('borrowKink',convert_values)]),
            Call(market, 'getUtilization()(uint256)', [('getUtilization',convert_values)]),
            Call(market, 'baseIndexScale()(uint256)', [('baseIndexScale',convert_values)]),
            Call(market, 'baseTrackingBorrowSpeed()(uint256)', [('baseTrackingBorrowSpeed',convert_values)]),
            Call(market, 'baseTrackingSupplySpeed()(uint256)', [('baseTrackingSupplySpeed',convert_values)]),
            Call(market, ['getSupplyRate(uint256)(uint256)',utilization], [('getSupplyRate',convert_values)]),
            Call(market, ['getBorrowRate(uint256)(uint256)',utilization], [('getBorrowRate',convert_values)]),
            Call(market, ['getPrice(address)(uint256)',compPriceFeed], [('getPrice',convert_values)])
        ],_w3 = web3,block_id=block)
        results[market] = multi()
        results[market]['market'] = market
        results[market]['block'] = block
    
    column_order = [
        'market',
        'totalBorrow',
        'totalSupply',
        'trackingIndexScale',
        'borrowKink',
        'getUtilization',
        'baseIndexScale',
        'baseTrackingBorrowSpeed',
        'baseTrackingSupplySpeed',
        'getSupplyRate',
        'getBorrowRate',
        'getPrice',
        'block'
    ]
    results_array = [
        [sub_dict[column] for column in column_order]
        for sub_dict in results.values()
    ]
    return results_array




@app.function(
    timeout=60 * 15 * 1,
    concurrency_limit=10,
    secrets=[
        modal.Secret.from_name("clickhouse_nuevo"),
        modal.Secret.from_name("RPC_ENDPOINTS")
    ])
def query_and_upload(market_addresses, block):
    clickhouse_client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST"),
        username=os.getenv("CLICKHOUSE_USER"),
        password=os.getenv("CLICKHOUSE_PASSWORD"),
        port=8443,
    )
    logger.info(f"running {block}")
    try:
        market_data = query_markets.local(market_addresses, block)
        clickhouse_client.insert(
            "ingest.compound_defidollar_ingest",
            data=market_data,
            column_names=[
            'market',
            'total_borrow',
            'total_supply',
            'tracking_index_scale',
            'borrow_kink',
            'get_utilization',
            'base_index_scale',
            'base_tracking_borrow_speed',
            'base_tracking_supply_speed',
            'get_supply_rate',
            'get_borrow_rate',
            'get_price',
            'block_number'
            ],
        )
        logger.info(f'block {block} complete')
    except:
        logger.info(f'block {block} not run')


  
