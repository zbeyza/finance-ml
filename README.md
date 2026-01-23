# finance-ml

A practical, end-to-end playground for **financial data collection + ML experiments** in Python.
The project currently focuses on **building a stock universe (S&P 500)** and **pulling market data via Twelve Data**, and will evolve into a full ML pipeline with feature engineering, modeling, and backtesting. :contentReference[oaicite:1]{index=1}

## What this repo is (and isn’t)
- ✅ A learning + portfolio project showing clean data ingestion, experimentation, and ML workflow in finance
- ✅ A place to grow toward research-grade evaluation (proper splits, leakage checks, backtests)
- ❌ Not financial advice, not a production trading system (yet)

---

## Current status (v0)
### Implemented / in progress
- Build an S&P 500 “universe” list (`build_universe_sp500.py`) :contentReference[oaicite:2]{index=2}
- Twelve Data API wrapper/utility (`twelvedata.py`) :contentReference[oaicite:3]{index=3}
- Sandbox / early experiments (`trial.py`) :contentReference[oaicite:4]{index=4}
- Dependencies defined (`requirements.txt`) :contentReference[oaicite:5]{index=5}

### Next milestones (planned)
- Data pipeline (download → cache → clean → store)
- Feature engineering (returns, volatility, momentum, technical indicators)
- ML tasks:
  - **Classification**: next-day direction / regime label
  - **Regression**: next-day/next-week return prediction
- Proper evaluation:
  - time-series splits, walk-forward validation
  - leakage prevention checks
- Backtesting & benchmarking:
  - naive baselines (buy&hold, moving-average crossover)
  - ML-based strategy vs baselines
- Reporting:
  - metrics + charts + experiment tracking

