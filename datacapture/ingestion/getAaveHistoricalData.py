import json
from web3 import Web3
from dotenv import load_dotenv
import os
from multicall import Call, Multicall
import clickhouse_connect
import modal
import logging
import json
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

custom_image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install_from_requirements("requirements.txt")
)

app = modal.App(name="fetch_aave_defidollar2", image=custom_image)


@app.function(
    timeout=60 * 60 * 5,
    concurrency_limit=1,
    schedule=modal.schedule.Cron("25 */6 * * *"),
    secrets=[
        modal.Secret.from_name("clickhouse_nuevo"),
        modal.Secret.from_name("RPC_ENDPOINTS")
    ],
    mounts=[modal.Mount.from_local_file("abis/aave/aToken.json", remote_path="/root/abis/aave/aToken.json"),
            modal.Mount.from_local_file("abis/aave/vToken.json", remote_path="/root/abis/aave/vToken.json"),
            modal.Mount.from_local_file("abis/aave/pool.json", remote_path="/root/abis/aave/pool.json"),]

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
        SELECT b.hour, b.first_block as block,  
            CASE WHEN n.block > 0 then 1 else 0 end as ran
        from ingest.arbitrum_hourly_blocktimes b
        LEFT JOIN (SELECT distinct block FROM ingest.aave_defidollar_ingest_new) n 
        ON n.block = cast(b.first_block as UInt64)
        WHERE ran = 0 
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
    secrets=[
        modal.Secret.from_name("clickhouse_nuevo"),
        modal.Secret.from_name("RPC_ENDPOINTS")
    ],
    mounts=[modal.Mount.from_local_file("abis/aave/aToken.json", remote_path="/root/abis/aave/aToken.json"),
            modal.Mount.from_local_file("abis/aave/vToken.json", remote_path="/root/abis/aave/vToken.json"),
            modal.Mount.from_local_file("abis/aave/pool.json", remote_path="/root/abis/aave/pool.json"),]
)
def query_markets(block):
    block = int(block)
    """
    Query the provided compound markets
    """
    rpc_url = os.getenv("arbitrum")
    web3 = Web3(Web3.HTTPProvider(rpc_url))

    # abi_path = "/root/abis/aave/ui_helper_v3.json"
    # if not os.path.exists(abi_path):
    #     logger.error(f"ABI file not found at {abi_path}")
    #     return []  # Early exit or handle as needed

    # Load the ABI file
    # with open(abi_path) as f:
    #     abi = json.load(f)
    with open("/root/abis/aave/pool.json") as f:
        aave_pool_abi = json.load(f)
    with open("/root/abis/aave/aToken.json") as f:
        aToken_abi = json.load(f)
    with open("/root/abis/aave/vToken.json") as f:
        vToken_abi = json.load(f)

    poolContract = web3.eth.contract(address = web3.to_checksum_address('0x794a61358d6845594f94dc1db02a252b5b4814ad'),abi=aave_pool_abi)
    aave_assets_df = pd.DataFrame([
        {
            "reserve": "dai",
            "underlyingAddress": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
            "aToken": "0x82E64f49Ed5EC1bC6e43DAD4FC8Af9bb3A2312EE",
            "vToken": "0x8619d80FB0141ba7F184CbF22fd724116D9f7ffC",
            "decimals": 18,
            "startblock":10
        },
        {
            "reserve": "usdc",
            "underlyingAddress": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            "aToken": "0x724dc807b04555b71ed48a6896b6F41593b8C637",
            "vToken": "0xf611aEb5013fD2c0511c9CD55c7dc5C1140741A6",
            "decimals": 6,
            "startblock":10
        },
        {
            "reserve": "usdc.e",
            "underlyingAddress": "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8",
            "aToken": "0x625E7708f30cA75bfd92586e17077590C60eb4cD",
            "vToken": "0xFCCf3cAbbe80101232d343252614b6A3eE81C989",
            "decimals": 6,
            "startblock":10
        },
        {
            "reserve": "usdt",
            "underlyingAddress": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
            "aToken": "0x6ab707Aca953eDAeFBc4fD23bA73294241490620",
            "vToken": "0xfb00AC187a8Eb5AFAE4eACE434F493Eb62672df7",
            "decimals": 6,
            "startblock":10
        },
        {
            "reserve": "gho",
            "underlyingAddress": "0x7dfF72693f6A4149b17e7C6314655f6A9F7c8B33",
            "aToken": "0xeBe517846d0F36eCEd99C735cbF6131e1fEB775D",
            "vToken": "0x18248226C16BF76c032817854E7C83a2113B4f06",
            "decimals": 18,
            "startblock":228027379
        }
    ])
    results = []
    for index, row in aave_assets_df.iterrows():
        if row['startblock'] > block:
            continue
        aTokenContract = web3.eth.contract(address=web3.to_checksum_address(row['aToken']), abi=aToken_abi)
        vTokenContract = web3.eth.contract(address=web3.to_checksum_address(row['vToken']), abi=vToken_abi)
        pool_call = poolContract.functions.getReserveData(web3.to_checksum_address(row['underlyingAddress'])).call(block_identifier=block)    
        underlyingAsset = row['underlyingAddress']
        name = row['reserve']
        symbol = row['reserve']
        # print(symbol)
        decimals = row['decimals']
        liquidityIndex = pool_call[1]
        variableBorrowIndex = pool_call[3]
        liquidityRate = pool_call[2]
        variableBorrowRate = pool_call[4]
        totalSupply = aTokenContract.functions.totalSupply().call(block_identifier=block)
        totalVariableDebt = vTokenContract.functions.totalSupply().call(block_identifier=block)
        # availableLiquidity = totalSupply - totalScaledVariableDebt
        block
        results.append([underlyingAsset,name,symbol,decimals,liquidityIndex,variableBorrowIndex,liquidityRate,variableBorrowRate,totalSupply,totalVariableDebt,block])

    return results

@app.function(
    timeout=60 * 15 * 1,
    concurrency_limit=20,
    secrets=[
        modal.Secret.from_name("clickhouse_nuevo"),
        modal.Secret.from_name("RPC_ENDPOINTS")
    ],
    retries=modal.Retries(
        max_retries=3,
        backoff_coefficient=1.0,
        initial_delay=3.0,
    ),
    mounts=[modal.Mount.from_local_file("abis/aave/aToken.json", remote_path="/root/abis/aave/aToken.json"),
            modal.Mount.from_local_file("abis/aave/vToken.json", remote_path="/root/abis/aave/vToken.json"),
            modal.Mount.from_local_file("abis/aave/pool.json", remote_path="/root/abis/aave/pool.json"),]
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

    market_data = query_markets.local(block)
    clickhouse_client.insert(table = 'ingest.aave_defidollar_ingest_new',
                         data = market_data,
                         column_names=['underlyingAsset','name','symbol','decimals','liquidityIndex','variableBorrowIndex','liquidityRate','variableBorrowRate','totalSupply','totalVariableDebt','block'])

    logger.info(f'block {block} complete')
