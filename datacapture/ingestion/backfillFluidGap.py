"""One-off backfill for Fluid hours the legacy job cannot ingest.

The legacy job (arbitrum_defi_dollar_nuevo / arb_defi_dollar_get_fluid) reads
every vault with one getVaultsEntireData() call. If any single vault's oracle
reverts at a block (e.g. FluidUsdOracleError(310007) on 2026-11-14/16), the
whole call reverts, the block is never written, and the job retries it and
alerts on every run.

This script reads the same data vault by vault, skips only the vaults that
revert, and writes rows in the legacy format to the legacy tables. Once the
gap is filled the legacy job stops retrying.

Dry run (default, no ClickHouse writes):
    modal run datacapture/ingestion/backfillFluidGap.py --env main
Write:
    modal run datacapture/ingestion/backfillFluidGap.py --env main --write-rows
Explicit blocks (comma-separated) instead of the gap query:
    modal run ... --blocks 524277970,524290000
"""
import json
import os
import pathlib

import modal

ABI_DIR = pathlib.Path(__file__).resolve().parents[1] / "abis" / "fluid"
LENDING_RESOLVER = "0xdF4d3272FfAE8036d9a2E1626Df2Db5863b4b302"
VAULT_RESOLVER = "0xD6373b375665DE09533478E8859BeCF12427Bb5e"

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("web3==7.6.1", "clickhouse-connect==0.8.13")
    .add_local_file(ABI_DIR / "fluid_lending.json", "/root/abis/fluid_lending.json")
    .add_local_file(ABI_DIR / "fluid_vault.json", "/root/abis/fluid_vault.json")
)
app = modal.App("fluid_defidollar_gap_backfill", image=image)
# Override only to dry-run in an environment without clickhouse_nuevo.
CH_SECRET = os.environ.get("BACKFILL_CH_SECRET", "clickhouse_nuevo")

GAP_SQL = """
    SELECT t.first_block AS block
    FROM ingest.arbitrum_hourly_blocktimes t
    LEFT JOIN (SELECT DISTINCT block FROM ingest.fluid_defidollar_vault_ingest) m
      ON cast(t.first_block AS Int64) = cast(m.block AS Int64)
    WHERE m.block < 100 AND t.hour > date('2025-01-01')
    ORDER BY t.timestamp
"""


def clickhouse():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        port=8443,
    )


@app.function(secrets=[modal.Secret.from_name("RPC_ENDPOINTS")], timeout=60 * 30,
              max_containers=4, retries=2)
def fetch(block: int):
    from web3 import Web3
    from web3.exceptions import ContractLogicError

    # main uses "arbitrum"; dev names the same endpoint "ARB_ENDPOINT".
    w3 = Web3(Web3.HTTPProvider(os.environ.get("arbitrum") or os.environ["ARB_ENDPOINT"]))
    lending = w3.eth.contract(LENDING_RESOLVER, abi=json.load(open("/root/abis/fluid_lending.json")))
    vaults = w3.eth.contract(VAULT_RESOLVER, abi=json.load(open("/root/abis/fluid_vault.json")))

    # Same columns as the legacy job: name, symbol, decimals, underlying,
    # totalAssets, totalSupply, rewardsRate, supplyRate, block.
    ftokens = lending.functions.getFTokensEntireData().call(block_identifier=block)
    ftoken_rows = [[row[i] for i in (3, 4, 5, 6, 7, 8, 11, 12)] + [block] for row in ftokens]

    vault_rows, skipped = [], []
    for address in vaults.functions.getAllVaultsAddresses().call(block_identifier=block):
        try:
            v = vaults.functions.getVaultEntireData(address).call(block_identifier=block)
        except ContractLogicError as error:  # includes ContractCustomError
            skipped.append((address, str(error)[:80]))
            continue
        # vault, totalBorrowVault, borrowRateLiquidity, borrowToken.token0,
        # supplyToken.token0, block -- identical indices to the legacy job.
        vault_rows.append([v[0], v[6][1], v[5][9], v[3][9][0], v[3][8][0], block])
    return block, ftoken_rows, vault_rows, skipped


@app.function(secrets=[modal.Secret.from_name(CH_SECRET)], timeout=60 * 5)
def gap_blocks():
    return [int(b) for b in clickhouse().query_df(GAP_SQL).block.values]


@app.function(secrets=[modal.Secret.from_name(CH_SECRET)], timeout=60 * 5)
def write(block, ftoken_rows, vault_rows):
    ch = clickhouse()
    ch.insert(table="ingest.fluid_defidollar_ftoken_ingest", data=ftoken_rows,
              column_names=["name", "symbol", "decimals", "underlying", "totalAssets",
                            "totalSupply", "rewardsRate", "supplyRate", "block"])
    ch.insert(table="ingest.fluid_defidollar_vault_ingest", data=vault_rows,
              column_names=["vault", "total_borrowed", "borrow_rate", "debt_token",
                            "collateral_token", "block"])


@app.local_entrypoint()
def main(write_rows: bool = False, blocks: str = ""):
    targets = [int(b) for b in blocks.split(",") if b] if blocks else gap_blocks.remote()
    print(f"{len(targets)} blocks to backfill; write={write_rows}")
    for block, ftoken_rows, vault_rows, skipped in fetch.map(targets, order_outputs=False):
        print(f"block {block}: {len(ftoken_rows)} ftokens, {len(vault_rows)} vaults, "
              f"skipped {[a for a, _ in skipped]}")
        if not vault_rows:
            raise RuntimeError(f"block {block}: no vaults read, refusing to continue")
        if write_rows:
            write.remote(block, ftoken_rows, vault_rows)
    print("done")
