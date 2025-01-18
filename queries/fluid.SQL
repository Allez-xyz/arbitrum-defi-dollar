CREATE OR REPLACE VIEW ingest.defidollar_fluid_summary as
with fluid_summary as (
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

    , fluid_borrow_data as (
        SELECT block, debt_token
            , case 
                when debt_token = '0xaf88d065e77c8cC2239327C5EDb3A432268e5831' then 'USDC'
                when debt_token = '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9' then 'USDT'
                else 'other'
                end as debt_token_name
            , sum(total_borrowed)/pow(10,6) as total_debt_f
            , sum(total_borrowed * borrow_rate)/cast(sum(total_borrowed) as Float64)/10000 as rate
        FROM fluid_defidollar_vault_ingest
        WHERE debt_token in ('0xaf88d065e77c8cC2239327C5EDb3A432268e5831', '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9')
        GROUP BY block, debt_token
        ORDER BY block desc, debt_token
    )
    SELECT f.name as market, t.hour, substring(f.symbol, 2) as market_name, f.block as block_number
        , f.underlying as debt_asset_address
        , f.totalSupply / pow(10,decimals) as total_supply_f
        , fluid_borrow_data.total_debt_f as total_borrow_f
        , f.supplyRate / pow(10,4) as supply_rate_f
        , fluid_borrow_data.rate as borrow_rate_f
        , f.rewardsRate / pow(10,14) as incentive_supply_rate_f
        , 0 as incentive_borrow_rate_f
        , supply_rate_f + incentive_supply_rate_f as net_supply_rate_f
        , borrow_rate_f + incentive_borrow_rate_f as net_borrow_rate_f
    FROM ingest.fluid_defidollar_ftoken_ingest f
    LEFT JOIN ingest.arbitrum_hourly_blocktimes t
    ON t.first_block = cast(f.block as Int64)
    LEFT JOIN fluid_borrow_data
    ON fluid_borrow_data.block = f.block 
    AND fluid_borrow_data.debt_token_name = substring(f.symbol, 2)
    WHERE substring(f.symbol, 2) in ('USDC','USDT')
    ORDER BY f.block desc, substring(f.symbol, 2)
)

SELECT 'fluid' as protocol, hour, block_number as block_number
    , market_name as market_name, debt_asset_address
    , total_supply_f, total_borrow_f
    , supply_rate_f, borrow_rate_f
    , net_supply_rate_f, net_borrow_rate_f
FROM fluid_summary 

