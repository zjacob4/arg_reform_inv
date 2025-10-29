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

1. Create conda environment:
   ```bash
   conda env create -f environment.yml
   conda activate arg-reform-inv
   ```

   Or install dependencies directly:
   ```bash
   make install
   ```

2. Set up data provider preferences (optional):
   ```bash
   export PREFERRED_PROVIDERS=BCRA,INDEC,YAHOOFX
   ```
   This configures which data providers to try first (default: "BCRA,INDEC,YAHOOFX,IMF,TE").
   See [Data Sources](#data-sources) section for details on provider configuration.

3. Initialize the database:
   ```bash
   make initdb
   ```

4. Refresh data from sources:
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

## Data Sources

The system uses a provider router with automatic fallback to ensure data availability. Each data source is configured with specific series IDs and can be customized.

### Provider Configuration

#### BCRA (Banco Central de la República Argentina)
- **API Base**: `https://api.bcra.gob.ar/series/v2.0`
- **Series Mappings**:
  - `USDARS_OFFICIAL` → Series ID `2501` (Tipo de Cambio Mayorista)
  - `RESERVES_USD` → Series ID `3519` (Reservas Internacionales)
- **Configuration**: Set `BCRA_API_BASE` in `.env` to override default endpoint

#### INDEC (Instituto Nacional de Estadística y Censos)
- **API Base**: `https://apis.datos.gob.ar/series/api/series`
- **Series Mappings**:
  - `CPI_HEADLINE` → Series ID `ipc_nivel_general_nacional`
  - `CPI_CORE` → Series ID `ipc_nucleo_nivel_general_nacional`
- **Configuration**: Set `INDEC_API_BASE` in `.env` to override default endpoint

#### Yahoo Finance
- **Provider**: `yfinance` library (no API key required)
- **Series Mappings**:
  - `USDARS_OFFICIAL` → Ticker `USDARS=X`
- **Limitations**: Spot rates only; no NDF curve data (handled in NDF step)
- **Configuration**: No additional setup required

### Fallback Behavior

The system tries providers in the order specified by `PREFERRED_PROVIDERS`:

1. **Primary**: BCRA for official FX/reserves, INDEC for CPI
2. **Secondary**: Yahoo Finance for additional FX data
3. **Tertiary**: IMF and TradingEconomics (if configured)

If a provider fails or returns empty data, the system automatically tries the next provider. If all providers fail, the application will display descriptive error messages instead of synthetic data.

### Customizing Series Mappings

To change series IDs or add new data sources:

1. **Edit Provider Files**: Modify the `*_SERIES` dictionaries in:
   - `src/data/providers/bcra.py` (BCRA series IDs)
   - `src/data/providers/indec.py` (INDEC series IDs)
   - `src/data/providers/yahoo_fx.py` (Yahoo tickers)

2. **Update Series Registry**: Add new series to `src/config/series_registry.py`

3. **Test Configuration**: Run `make refresh` to test new mappings

### Environment Variables

Add to your `.env` file:
```bash
# Provider precedence (comma-separated)
PREFERRED_PROVIDERS=BCRA,INDEC,YAHOOFX

# API bases (override if needed)
BCRA_API_BASE=https://api.bcra.gob.ar/series/v2.0
INDEC_API_BASE=https://apis.datos.gob.ar/series/api/series

# TradingEconomics API (if using)
TRADING_ECON_API_KEY=your_user:your_token
```

## Next Steps

- Integrate email/Slack notifications for alerts
- Add historical backtesting capabilities
- Implement additional feature engineering modules
- Add NDF curve data sources for forward rate analysis

