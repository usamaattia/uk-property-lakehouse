DBT = dbt build --project-dir transform --profiles-dir transform

.PHONY: install demo real build site serve test parity lint fmt clean

install:          ## dev install
	pip install -e ".[dev]"

demo:             ## synthetic data -> full build -> dashboard data (no network)
	lakehouse demo
	$(DBT)
	lakehouse export-site

real:             ## real HM Land Registry data (downloads several hundred MB)
	lakehouse monthly --backfill-years 3
	$(DBT)
	lakehouse export-site

build:            ## incremental dbt build of whatever has been landed
	$(DBT)

serve:            ## dashboard on http://localhost:8000
	python -m http.server 8000 --directory site

test:
	pytest -q

parity:           ## prove DuckDB and Spark produce identical gold tables
	pip install "pyspark==3.5.*" "dbt-spark[session]"
	$(DBT)
	$(DBT) --target spark --full-refresh
	python scripts/check_engine_parity.py

lint:
	ruff check . && ruff format --check .

fmt:
	ruff format . && ruff check --fix .

clean:
	rm -rf data warehouse/*.duckdb* site/data transform/target transform/logs logs spark-warehouse metastore_db derby.log
