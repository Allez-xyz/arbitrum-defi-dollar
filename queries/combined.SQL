CREATE OR REPLACE VIEW ingest.defidollar_all_markets as 
SELECT protocol, hour, cast(block_number as Int64) as block_number
    , market_name, debt_asset_address
    , total_supply_f, total_borrow_f
    , supply_rate_f, borrow_rate_f
    , net_supply_rate_f, net_borrow_rate_f
FROM defidollar_silo_summary 
UNION ALL 
SELECT protocol, hour, cast(block_number as Int64) as block_number
    , market_name, debt_asset_address
    , total_supply_f, total_borrow_f
    , supply_rate_f, borrow_rate_f
    , net_supply_rate_f, net_borrow_rate_f
FROM defidollar_fluid_summary 
UNION ALL 
SELECT protocol, hour, cast(block_number as Int64) as block_number
    , market_name, debt_asset_address
    , total_supply_f, total_borrow_f
    , supply_rate_f, borrow_rate_f
    , net_supply_rate_f, net_borrow_rate_f
FROM defidollar_dolomite_summary 
UNION ALL 
SELECT protocol, hour, cast(block_number as Int64) as block_number
    , market_name, debt_asset_address
    , total_supply_f, total_borrow_f
    , supply_rate_f, borrow_rate_f
    , net_supply_rate_f, net_borrow_rate_f
FROM defidollar_compound_summary 
UNION ALL 
SELECT protocol, hour, cast(block_number as Int64) as block_number
    , market_name, debt_asset_address
    , total_supply_f, total_borrow_f
    , supply_rate_f, borrow_rate_f
    , net_supply_rate_f, net_borrow_rate_f
FROM ingest.defidollar_aave_summary 

