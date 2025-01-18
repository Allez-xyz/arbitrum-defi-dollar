CREATE OR REPLACE view ingest.defidollar_dolomite_summary as 
with dolomite_summary as (
    with assets as (
        SELECT '0xaf88d065e77c8cC2239327C5EDb3A432268e5831' as addr, 'USDC' as symbol, 6 as decimals
        UNION ALL
        SELECT '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9' as addr, 'USDT' as symbol, 6 as decimals
        UNION ALL
        SELECT '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8' as addr, 'USDC.e' as symbol, 6 as decimals
        UNION ALL
        SELECT '0xda10009cbd5d07dd0cecc66161fc93d7c9000da1' as addr, 'DAI' as symbol, 18 as decimals
        UNION ALL
        SELECT '0x7dff72693f6a4149b17e7c6314655f6a9f7c8b33' as addr, 'GHO' as symbol, 18 as decimals
    )
    SELECT assets.symbol, d.block as block, t.hour, d.asset_address as debt_asset_address
        , supply_index * supply_par / pow(10,18) / pow(10, decimals) as total_supply_f
        , borrow_index * borrow_par / pow(10,18) / pow(10, decimals) as total_borrow_f
        , borrow_rate_per_second * 86400 * 365 / pow(10,18) as borrow_rate_f
        , total_borrow_f / total_supply_f as utilization_f
        , borrow_rate_f * utilization_f * cast(get_earnings_rate as Float64) / pow(10,18) as supply_rate_f
        , 0 as incentive_borrow_rate_f
        , 0 as incentive_supply_rate_f
        , incentive_supply_rate_f + supply_rate_f as net_supply_rate_f
        , incentive_borrow_rate_f + borrow_rate_f as net_borrow_rate_f
    FROM ingest.dolomite_defidollar_ingest d
    INNER JOIN assets 
    ON lower(assets.addr) = lower(d.asset_address)
    LEFT JOIN ingest.arbitrum_hourly_blocktimes t
    ON t.first_block = cast(d.block as Int64)
    ORDER BY d.block desc, d.asset_address
)

SELECT 'dolomite' as protocol, hour, block as block_number
    , symbol as market_name, debt_asset_address
    , total_supply_f, total_borrow_f
    , supply_rate_f, borrow_rate_f
    , net_supply_rate_f, net_borrow_rate_f
FROM dolomite_summary 
ORDER BY hour desc, market_name
