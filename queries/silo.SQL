create or replace view ingest.defidollar_silo_summary as
with silo_summary as (
    with assets as (
        SELECT '0xaf88d065e77c8cC2239327C5EDb3A432268e5831' as addr, 'USDC' as symbol
        UNION ALL
        SELECT '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9' as addr, 'USDT' as symbol
        UNION ALL
        SELECT '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8' as addr, 'USDC.e' as symbol
        UNION ALL
        SELECT '0xda10009cbd5d07dd0cecc66161fc93d7c9000da1' as addr, 'DAI' as symbol
        UNION ALL
        SELECT '0x7dff72693f6a4149b17e7c6314655f6a9f7c8b33' as addr, 'GHO' as symbol
    )
    SELECT m.silo_name as market,
        t.hour as hour,
        asset_symbol as collateral,
        total_deposits_with_interest/pow(10,6) as total_supply_f,
        total_borrow_amount_with_interest/pow(10,6) as total_borrow_f,
        borrow_apy / pow(10,18) as borrow_rate_f,
        -- ln(1 + borrow_apy_f) as borrow_rate_f,
        deposit_apy / pow(10,18) as supply_rate_f,
        -- ln(1 + supply_apy_f) as supply_rate_f,
        borrow_rate_f as net_borrow_rate_f,
        -- ln(1 + borrow_apy_f) as borrow_rate_f,
        supply_rate_f as net_supply_rate_f,
        *
    FROM ingest.silo_defidollar_ingest s
    LEFT JOIN ingest.silo_markets m
    ON m.silo_address = s.silo_address
    LEFT JOIN ingest.arbitrum_hourly_blocktimes t
    ON t.first_block = cast(s.block as Int64)
    -- WHERE lower(asset_symbol) = 'wsteth'
    ORDER BY block desc, market
)
-- SELECT * 
-- FROM silo_summary 
-- LIMIT 10
SELECT 'silo' as protocol, hour, block as block_number
    , market as market_name, '0xaf88d065e77c8cC2239327C5EDb3A432268e5831' as debt_asset_address
    , total_supply_f, total_borrow_f
    , supply_rate_f, borrow_rate_f
    , net_supply_rate_f, net_borrow_rate_f
FROM silo_summary 
ORDER BY hour desc, market_name

