.PHONY: install run test refresh refresh-embi initdb watch

# Install project dependencies
install:
	pip install -e .

# Run the Streamlit application
run:
	python -m streamlit run src/app/dashboard.py

# Run tests
test:
	pytest

# Refresh data from all sources
refresh:
	python -m src.data.refresh_all

# Refresh only EMBI data
refresh-embi:
	python -c "from src.data.pull_embi_resilient import refresh_embi_resilient; refresh_embi_resilient(start='2024-01-01', min_bonds=2)"

# Refresh EMBI synthetic data
refresh-embi-synth:
	python -c "from src.data.pull_embi_synthetic import refresh_embi_synth_usd; refresh_embi_synth_usd(start='2024-01-01')"

# Refresh CDS data
refresh-cds:
	python -c "from src.data.pull_cds import refresh_cds_arg_5y; print(refresh_cds_arg_5y(mode='latest'))"
# Expect: 1  (and a journal INFO entry)


# Initialize database schema and load series registry
initdb:
	python -m src.data.init_db

# Watch for state changes and send alerts (runs every 10 minutes)
watch:
	python -m src.app.watcher

