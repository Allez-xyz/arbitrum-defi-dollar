# Arbitrum DeFi Dollar Rates

## Overview

**Arbitrum DeFi Dollar Rates** is an open-source data aggregation project and dashboard designed to aggregate and analyze stablecoin borrowing and lending rates across major DeFi platforms on the Arbitrum network. The project focuses on developing the **Arbitrum DeFi Dollar Supply Rate (DDSR)** and **Arbitrum DeFi Dollar Borrow Rate (DDBR)** indexes, providing a comprehensive view of stablecoin supply and demand within the Arbitrum ecosystem.

## Features

- **Data Aggregation:** Collects current and historical stablecoin borrowing and lending rates from major DeFi protocols such as Aave, Compound, Dolomite, Fluid, and Silo.
- **Indexes:** Computes the Arbitrum DDSR and DDBR to provide a holistic measure of stablecoin supply and demand.
- **Dashboard:** A dashboard for easy visualization of the Arbitrum DeFi Dollar metrics across time, protcols and stablecoins.


## Table of Contents

- [Project Structure](#project-structure)
- [ Project Prerequisites](#installation)

## Project Structure
```
├── README.md
├── datacapture
│   ├── abis
│   │   ├── aave
│   │   │   ├── ...
│   │   ├── compound
│   │   │   └── ...
│   │   ├── dolomite
│   │   │   └── ...
│   │   ├── fluid
│   │   │   ├── ...
│   │   └── silo
│   │       ├── ...
│   └── ingestion
│       ├── getAaveHistoricalData.py
│       ├── getArbitrumBlocks.py
│       ├── getCompoundHistoricalData.py
│       ├── getDolomiteHistoricalData.py
│       ├── getFluidHistoricalData.py
│       ├── getSiloHistoricalData.py
│       ├── requirements.txt
│       └── requirements_multicall.txt
├── queries
│   ├── aave.SQL
│   ├── combined.SQL
│   ├── compound.SQL
│   ├── dolomite.SQL
│   ├── fluid.SQL
│   └── silo.SQL
├── requirements.txt
└── requirements_multicall.txt
```

### Directory Breakdown

- **datacapture/**
  - The datacapture directory contains all code to pull lending rates and volumes from Arbitrum. 
  - **abis/**: Contains ABI (Application Binary Interface) files for the various contracts we are hitting for this project.
  - **get\*HistoricalData.py**: Python scripts following this naming convention are deployed to collect and upload historical lending data from each of the included protocols.
  - **getArbitrumBlocks**: Pulls new arbitrum blocks using the Arbiscan API
  - **requirements.txt**: Python dependencies for data pulling scripts.
  - **requirements_multicall.txt**: Additional dependencies for multi-call operations.

- **queries/**
  - The queries directory contains views to aggregate and transform the raw data ingested by the `datacapture` jobs.
  - **\*.SQL**: SQL based views to standardize lending market data. For each market we standardize the data to the following attributes: `protocol`, `hour` `block_number` , `market_name` `debt_asset_address` , `total_supply_f`, `total_borrow_f` , `supply_rate_f`, `borrow_rate_f` , `net_supply_rate_f`, `net_borrow_rate_f` columns.
  - **combined.SQL**: Combines all standardized data views into a single view.

- **testing/**
  - Jupyter notebooks and Python scripts for testing and validating data pipelines.


## Project Prerequisites

We use [Modal](https://modal.com/) to host these jobs which upload to a [clickhouse](https://clickhouse.com/) database. Additioanlly, you will need an arbitrum RPC for these jobs to run.
- **Modal Account**: For deploying data capture jobs. Note: They have a generous free tier.
- **ClickHouse Database**: For data storage, transformation, and 
- **Arbiscan API**: For fetching hourly block numbers.
