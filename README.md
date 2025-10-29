# Argentina Reform Investment Analysis

This project analyzes investment opportunities and market dynamics related to Argentina's economic reform initiatives. The system aggregates data from multiple sources, processes financial and economic indicators, and provides interactive visualizations and insights through a Streamlit-based application.

## Stage 1: Core Analytics & Decision Framework

Stage 1 implements a comprehensive nowcasting and decision framework with the following capabilities:

- **Nowcasting**: Real-time computation of key economic indicators (FX gap, reserves momentum, CPI corridors, EMBI levels)
- **Corridors**: CPI forecast corridors based on FX pass-through, regulated prices, and wage dynamics
- **Triggers**: Multi-dimensional state evaluation (GREEN/YELLOW/RED) with gate-based decision logic
- **Sizing**: Sharpe ratio-based position sizing with sigmoid smoothing and dynamic hedging

### Current Thresholds

The system uses the following thresholds for state classification:

- **FX Gap**: 
  - GREEN: < 15%
  - YELLOW: 15-25%
  - RED: > 25%

- **Reserves Momentum (4-week)**:
  - GREEN: > +3%
  - YELLOW: 0% to +3%
  - RED: < 0%

- **Core CPI (3-month annualized)**:
  - GREEN: < 25%
  - YELLOW: 25-35%
  - RED: > 35%

- **EMBI**:
  - GREEN: Level < 1400 bps OR 30-day trend < -300 bps
  - YELLOW: Level 1400-1600 bps OR trend -300 to -100 bps
  - RED: Level > 1600 bps OR trend > -100 bps

Overall state is GREEN when all conditions are met simultaneously; YELLOW when mixed; RED when all dimensions are RED.

## Getting Started

### Setup

1. Install dependencies:
   ```bash
   make install
   ```

2. Initialize the database:
   ```bash
   make initdb
   ```

3. Refresh data from sources:
   ```bash
   make refresh
   ```

### Running

- **Dashboard**: Launch the Streamlit dashboard
  ```bash
  make run
  ```

- **Watcher**: Monitor state changes and receive alerts (runs every 10 minutes)
  ```bash
  make watch
  ```

### Testing

Run the test suite:
```bash
pytest
```

Or use the make target:
```bash
make test
```

## Next Steps

- Replace placeholder data feeds with real sources:
  - BCRA API for official FX and reserves
  - INDEC API for CPI data
  - Bloomberg/Refinitiv for EMBI and CDS spreads
  - Alternative sources for parallel FX rates
- Integrate email/Slack notifications for alerts
- Add historical backtesting capabilities
- Implement additional feature engineering modules

