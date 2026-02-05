# finance-ml

This repository contains an extensible Python-based framework for **financial market data ingestion and machine learning research**.

The current implementation focuses on:
- Construction of a U.S. equity universe (S&P 500 constituents)
- Retrieval of historical market data via the Twelve Data API

The project is designed to evolve into a reproducible research and experimentation pipeline supporting feature engineering, predictive modeling, and systematic backtesting.

---

## Project Structure

```
.
├── data/            # datasets and outputs (CSVs)
├── scripts/
│   ├── build_universe_sp500.py
│   ├── twelvedata.py
│   └── trial.py
├── README.md
├── requirements.txt
└── LICENSE
```

---

## Project Scope and Objectives

The primary objectives of this repository are to:

- Provide a structured and reproducible workflow for financial data collection
- Support rigorous experimentation with machine learning models on time-series financial data
- Enable systematic comparison of baseline strategies and ML-based approaches
- Facilitate research into predictive performance, robustness, and evaluation methodology

This repository is intended for **research and portfolio demonstration purposes**. It does not constitute financial advice and is not intended for live trading or production deployment.

---

## Current Status

### Implemented Components
- S&P 500 universe construction (`scripts/build_universe_sp500.py`)
- Twelve Data API utilities for market data retrieval (`scripts/twelvedata.py`)
- Experimental and prototyping scripts (`scripts/trial.py`)
- Dependency specification (`requirements.txt`)

### Planned Extensions

**Data engineering**
- Unified data ingestion and storage layer
- Local persistence of raw and processed datasets (CSV/Parquet)
- Caching and rate-limit management

**Feature engineering**
- Return and log-return calculations
- Rolling volatility and momentum features
- Technical indicators (e.g., SMA, EMA, RSI, MACD)

**Machine learning**
- Classification tasks (e.g., short-horizon direction or regime identification)
- Regression tasks (e.g., short-horizon return forecasting)
- Baseline models and benchmark predictors

**Evaluation methodology**
- Time-series cross-validation and walk-forward evaluation
- Leakage prevention and data-snooping controls
- Stability and robustness analysis

**Backtesting and performance analysis**
- Signal-to-portfolio backtesting framework
- Transaction cost and slippage modeling
- Risk-adjusted performance metrics (e.g., Sharpe ratio, maximum drawdown)

---
