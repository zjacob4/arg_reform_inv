.PHONY: install run test refresh initdb watch

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

# Initialize database schema and load series registry
initdb:
	python -m src.data.init_db

# Watch for state changes and send alerts (runs every 10 minutes)
watch:
	python -m src.app.watcher

