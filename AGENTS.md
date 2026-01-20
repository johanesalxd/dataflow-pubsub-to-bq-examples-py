# Agent Guidelines for Dataflow Pub/Sub to BigQuery Pipeline

This document provides instructions and guidelines for AI agents and developers working on this repository.

## 1. Project Overview

This project is a Python-based Apache Beam pipeline that reads from Google Cloud Pub/Sub and writes to BigQuery.
It supports both local execution (`DirectRunner`) and production deployment on Dataflow (`DataflowRunner`).

**Key Technologies:**
- Python 3.12+
- Apache Beam (GCP)
- `uv` for dependency management
- Google Cloud Platform (Dataflow, Pub/Sub, BigQuery)

## 2. Environment & Build

### Dependency Management
This project uses `uv` for fast package management.
- **Install dependencies:**
  ```bash
  uv sync
  ```
- **Add a dependency:**
  ```bash
  uv add <package_name>
  ```

### Build Configuration
- Configuration is stored in `pyproject.toml`.
- The package name is `dataflow-pubsub-to-bq-pipeline`.

## 3. Development Commands

### Running the Pipeline
- **Local Testing (DirectRunner):**
  Use the provided shell script to run locally. This sets up necessary resources and runs the pipeline.
  ```bash
  ./run_local.sh
  ```
  *Ensure you have authenticated with `gcloud auth application-default login`.*

- **Production Deployment (Dataflow):**
  Submits the job to Dataflow.
  ```bash
  ./run_dataflow.sh
  ```

### Testing
*Note: formalized tests are currently being established. Please follow these guidelines when adding tests.*

- **Run Tests:**
  ```bash
  uv run pytest
  ```
- **Run Single Test:**
  ```bash
  uv run pytest tests/test_file.py::test_function_name
  ```
- **Test Location:**
  Place all unit tests in a `tests/` directory at the project root.
- **Test Style:**
  Use `pytest` fixtures and parameterized tests where possible. Mock external GCP services (Pub/Sub, BigQuery) using `unittest.mock`.

### Linting & Formatting
*Recommended tools (configuration to be added to `pyproject.toml`):*

- **Lint:**
  ```bash
  uv run ruff check .
  ```
- **Format:**
  ```bash
  uv run ruff format .
  ```

## 4. Code Style & Conventions

Follow these strict guidelines when writing or refactoring code.

### General Python
- **Version:** Target Python 3.12+.
- **Indentation:** Use 4 spaces. No tabs.
- **Line Length:** Aim for 80-100 characters.

### Type Hinting
- **Mandatory:** All function signatures must have type hints.
- **Imports:** Use `typing` module (e.g., `List`, `Dict`, `Any`, `Optional`) or modern standard types (`list`, `dict`) if compatible.
  ```python
  def process_message(data: dict[str, Any]) -> list[str]:
      ...
  ```

### Naming Conventions
- **Classes:** `CamelCase` (e.g., `ParsePubSubMessage`).
- **Functions/Methods:** `snake_case` (e.g., `get_bigquery_schema`).
- **Variables:** `snake_case`.
- **Constants:** `UPPER_CASE` (e.g., `TABLE_SCHEMA`).

### Imports
- Group imports in the following order:
  1. Standard library imports
  2. Third-party library imports (e.g., `apache_beam`)
  3. Local application imports
- Sort imports alphabetically within groups.
- Prefer explicit imports over `*`.

### Docstrings
- Use **Google Style** docstrings for all modules, classes, and functions.
- Include `Args:`, `Returns:`, and `Raises:` sections.

  ```python
  def my_function(param1: str) -> bool:
      """Brief description of what the function does.

      Args:
          param1: Description of param1.

      Returns:
          True if successful, False otherwise.
      """
      ...
  ```

### Error Handling
- Use specific exception handling (`try...except ValueError` instead of `except Exception`).
- **Logging:** specific errors should be logged using the `logging` module.
  ```python
  import logging
  try:
      ...
  except json.JSONDecodeError as e:
      logging.error(f"Failed to parse JSON: {e}")
  ```

### Apache Beam Specifics
- **Transforms:** Implement custom transforms as subclasses of `beam.DoFn` or `beam.PTransform`.
- **Pipeline Options:** specific pipeline options should be defined in `pipeline_options.py`.
- **Testing Transforms:** Use `TestPipeline` and `util.assert_that` for testing Beam transforms.

## 5. Repository Structure

```
dataflow-pubsub-to-bq-examples-py/
├── dataflow_pubsub_to_bq/      # Main package
│   ├── pipeline.py             # Pipeline entry point
│   ├── pipeline_options.py     # Options configuration
│   └── transforms/             # Custom Beam transforms
├── run_local.sh                # Local execution script
├── run_dataflow.sh             # Dataflow deployment script
└── pyproject.toml              # Build & dependencies
```

## 6. Workflow for Agents

1.  **Analyze:** specific files using `read` to understand context.
2.  **Plan:** Propose changes ensuring they align with `apache_beam` patterns.
3.  **Implement:** Edit files, ensuring type hints and docstrings are present.
4.  **Verify:** Since explicit tests may be missing, carefully review logic or create a small reproduction script if complex changes are made.
