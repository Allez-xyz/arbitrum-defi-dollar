import json
from web3 import Web3
from dotenv import load_dotenv
import os
import pandas as pd
import polars as pl
import clickhouse_connect
import modal
import logging



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

custom_image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install_from_requirements("requirements.txt")
)

app = modal.App(name="fetch_silo_defidollar", image=custom_image)


@app.function(
    timeout=60 * 60 * 5,
    concurrency_limit=1,
    secrets=[
        modal.Secret.from_name("clickhouse_nuevo"),
        modal.Secret.from_name("RPC_ENDPOINTS")
    ],
    schedule=modal.schedule.Cron("25 */6 * * *"))
def main():
    # rpc_url = os.getenv("arbitrum")
    # web3 = Web3(Web3.HTTPProvider(rpc_url))

    clickhouse_client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST"),
        username=os.getenv("CLICKHOUSE_USER"),
        password=os.getenv("CLICKHOUSE_PASSWORD"),
        port=8443,
    )

    silo_markets_from_clickhouse = clickhouse_client.query_df("""
        SELECT m.silo_address, t.timestamp, t.first_block as block
        FROM ingest.arbitrum_hourly_blocktimes t
        CROSS JOIN ingest.silo_markets m
        WHERE t.first_block > m.block_live
        AND t.hour >= date('2024-01-01')
        AND t.first_block not in (SELECT distinct block from ingest.silo_defidollar_ingest)
        ORDER BY t.first_block desc
    """)

    if silo_markets_from_clickhouse.shape[0] == 0:
        print('Nothing to query, done.')
        return 0

    ### create chunks of silo_markets_from_clickhouse where each chunk is the data with the same block
    ### then do some sort of query_silos.map(chunk) for chunk in chunks
    grouped = silo_markets_from_clickhouse.groupby("block")
    block_silo_groups = []
    for block, group in grouped:
        silo_addresses = group["silo_address"].unique().tolist()
        block_silo_groups.append((silo_addresses, block))

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
    mounts=[modal.Mount.from_local_file("abis/silo/SiloLens.json", remote_path="/root/abis/silo/SiloLens.json")]
)
def query_silos(silo_addresses, block):
    rpc_url = os.getenv("arbitrum")
    web3 = Web3(Web3.HTTPProvider(rpc_url))

    abi_path = "/root/abis/silo/SiloLens.json"

    # Load the ABI file
    with open(abi_path) as f:
        silo_lens_abi = json.load(f)
    with open("abis/silo/SiloLens.json") as f:
        silo_lens_abi = json.load(f)
    USDCe_address = web3.to_checksum_address('0xff970a61a04b1ca14834a43f5de4533ebddb5cc8')
    
    
    SiloLens_address = web3.to_checksum_address("0xBDb843c7a7e48Dc543424474d7Aa63b61B5D9536")
    SiloLens_contract = web3.eth.contract(address=SiloLens_address, abi=silo_lens_abi)
    
    rows = []  # List of lists instead of a list of dicts

    for silo in silo_addresses:
        borrowAPY = SiloLens_contract.functions.borrowAPY(
            web3.to_checksum_address(silo),
            USDCe_address,
        ).call(block_identifier = block)
        
        depositAPY = SiloLens_contract.functions.depositAPY(
            web3.to_checksum_address(silo),
            USDCe_address,
        ).call(block_identifier = block)
        
        totalBorrowAmountWithInterest = SiloLens_contract.functions.totalBorrowAmountWithInterest(
            web3.to_checksum_address(silo),
            USDCe_address,
        ).call(block_identifier = block)
        
        totalDepositsWithInterest = SiloLens_contract.functions.totalDepositsWithInterest(
            web3.to_checksum_address(silo),
            USDCe_address,
        ).call(block_identifier = block)
        
        # Append data in the order matching the table columns
        rows.append([
            silo,
            borrowAPY,
            depositAPY,
            totalBorrowAmountWithInterest,
            totalDepositsWithInterest,
            int(block)
        ])
    
    # Return a list of lists (each sub-list is a single row)
    return rows

@app.function(
    timeout=60 * 15 * 1,
    concurrency_limit=20,
    secrets=[
        modal.Secret.from_name("clickhouse_nuevo"),
        modal.Secret.from_name("RPC_ENDPOINTS")
    ],
    mounts=[modal.Mount.from_local_file("abis/silo/SiloLens.json", remote_path="/root/abis/silo/SiloLens.json")]
)
def query_and_upload(silo_addresses, block):
    clickhouse_client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST"),
        username=os.getenv("CLICKHOUSE_USER"),
        password=os.getenv("CLICKHOUSE_PASSWORD"),
        port=8443,
    )
    logger.info(f"running {block}")
    silo_data = query_silos.local(silo_addresses, block)
    clickhouse_client.insert(
        "ingest.silo_defidollar_ingest",
        data=silo_data,
        column_names=[
            "silo_address",
            "borrow_apy",
            "deposit_apy",
            "total_borrow_amount_with_interest",
            "total_deposits_with_interest",
            "block",
        ],
    )
    logger.info(f'block {block} complete')
