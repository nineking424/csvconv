.PHONY: test test-unit test-integration lint format type build-pex docker-test-pex clean

test:
	pytest tests/

test-unit:
	pytest tests/unit/

test-integration:
	pytest tests/integration/

lint:
	ruff check src/ tests/
	ruff format --check src/ tests/

format:
	ruff format src/ tests/

type:
	mypy src/csvconv/

build-pex:
	bash scripts/build_pex.sh

docker-test-pex:
	docker build -f Dockerfile.build -t csvconv-test .
	docker run --rm csvconv-test

clean:
	rm -rf dist/ build/ *.egg-info src/*.egg-info .tox .mypy_cache .pytest_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
