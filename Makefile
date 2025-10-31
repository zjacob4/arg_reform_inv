.PHONY: install run test refresh refresh-embi refresh-embi-synth refresh-cds refresh-ndf refresh-policy initdb watch

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

# Refresh NDF curve data
refresh-ndf:
	python -c "from src.data.pull_ndf import refresh_ndf_curve; print(refresh_ndf_curve(start='2024-01-01'))"

# Refresh policy rate data
refresh-policy:
	python -c "from src.data.pull_policy_rate import refresh_policy_rate; print(refresh_policy_rate(start='2023-01-01'))"

# Initialize database schema and load series registry
initdb:
	python -m src.data.init_db

# Watch for state changes and send alerts (runs every 10 minutes)
watch:
	python -m src.app.watcher

