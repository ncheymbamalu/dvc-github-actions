.PHONY: clean data_pipeline update_artifacts

LOOK_BACK_PERIOD ?= 14

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf .ruff_cache .pytest_cache

data_pipeline:
	dvc pull && \
	uv run python -Wignore data.py $(LOOK_BACK_PERIOD)

update_artifacts:
	dvc add ./artifacts && \
	git add artifacts.dvc && \
	git commit -m "Updating artifacts.dvc" && \
	dvc push && \
	git push
