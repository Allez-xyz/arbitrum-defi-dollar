create or replace view ingest.defidollar_compound_summary as 

with compound_summary as (

    with compound_markets AS (
        SELECT 'USDC' AS symbol
            , '0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf' AS contract_address
            , 122080500 AS block_live
            , '0xaf88d065e77c8cC2239327C5EDb3A432268e5831' as underlyingAsset 
        UNION ALL
        SELECT 'USDC.e'
            , '0xA5EDBDD9646f8dFF606d7448e414884C7d905dCA'
            , 87335214
            , '0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8'
        UNION ALL
        SELECT 'USDT', '0xd98Be00b5D27fc98112BdE293e487f8D4cA57d07', 223796350, '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9'
    )

    SELECT compound_markets.symbol as market
        , t.hour
        , block_number
        , compound_markets.symbol as debt_asset_symbol
        , compound_markets.underlyingAsset as debt_asset
        , total_supply/pow(10,6) as total_supply_f 
        , total_borrow/pow(10,6) as total_borrow_f 
        , get_utilization/pow(10,18) as utilization_f
        , get_supply_rate/pow(10,18)*60 * 60 * 24 * 365 as supply_rate_f
        , get_borrow_rate/pow(10,18)*60 * 60 * 24 * 365 as borrow_rate_f
        , base_tracking_supply_speed * 60*60*24 * get_price as rewardToSuppliersPerDay
        , cast(rewardToSuppliersPerDay as Float64) / total_supply_f * 365.25 / pow(10,23) as incentive_supply_rate_f
        , base_tracking_borrow_speed * 60*60*24 * get_price as rewardToBorrowersPerDay
        , cast(rewardToBorrowersPerDay as Float64) / total_borrow_f * 365.25 / pow(10,23) as incentive_borrow_rate_f
        , incentive_supply_rate_f + supply_rate_f as net_supply_rate_f
        , borrow_rate_f - incentive_borrow_rate_f as net_borrow_rate_f
    FROM ingest.compound_defidollar_ingest c
    INNER JOIN compound_markets 
    on compound_markets.contract_address = c.market
    LEFT JOIN ingest.arbitrum_hourly_blocktimes t
    ON t.first_block = cast(c.block_number as Int64)
    ORDER BY block_number desc, market 
)

SELECT 'compound' as protocol, hour, block_number
    , market as market_name, debt_asset as debt_asset_address
    , total_supply_f, total_borrow_f
    , supply_rate_f, borrow_rate_f
    , net_supply_rate_f, net_borrow_rate_f
FROM compound_summary 