create view ingest.defidollar_aave_summary as 
with aave_summary as (
    SELECT 'aave' as protocol, hour as hour, a.block as block_number
        , symbol as market_name, underlyingAsset as debt_asset_address
        , liquidityRate / pow(10,27) as supply_rate_f
        , variableBorrowRate / pow(10,27) as borrow_rate_f
        , totalVariableDebt/pow(10,decimals) as total_borrow_f
        , totalSupply/pow(10,decimals) as total_supply_f
        , borrow_rate_f as net_borrow_rate_f
        , supply_rate_f as net_supply_rate_f
    FROM ingest.aave_defidollar_ingest_new  a
    LEFT JOIN ingest.arbitrum_hourly_blocktimes t
    ON cast(a.block as Int64) = cast(t.first_block as Int64)
    ORDER BY block desc, symbol
)

SELECT protocol, hour, block_number, market_name, debt_asset_address,
    supply_rate_f, borrow_rate_f,
    total_supply_f, total_borrow_f,
    net_supply_rate_f, net_borrow_rate_f
FROM 
    aave_summary
WHERE 
    lower(market_name) in ('usdc','usdt','dai','gho', 'usdc.e')