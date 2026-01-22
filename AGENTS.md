# Agent Guidelines for Dataflow Pub/Sub to BigQuery Pipeline

This document provides comprehensive instructions and guidelines for AI agents and developers working on this repository.

## 1. Project Overview

This repository contains Apache Beam pipelines for reading from Google Cloud Pub/Sub and writes to BigQuery.
It supports two implementations:
1.  **Python** (Primary): Located in the root directory.
2.  **Java** (Comparison): Located in the `java/` directory.

Both implementations support:
-   **Production Deployment**: `DataflowRunner` for Google Cloud.

**Key Technologies:**
-   **Python:** 3.12+, `uv` manager, Apache Beam (GCP).
-   **Java:** JDK 17, Maven, Apache Beam (GCP) 2.70.0 (matching Python).

## 2. Environment Setup & Build

### Python (Root Directory)
**Manager:** `uv`
-   **Sync Environment:**
    ```bash
    uv sync
    ```
-   **Add Dependency:**
    ```bash
    uv add <package_name>
    ```
-   **Build Wheel (for Dataflow workers):**
    ```bash
    uv build --wheel
    ```

### Java (`java/` Directory)
**Manager:** Maven (`mvn`)
-   **Java Version:** JDK 17
-   **Build & Package (Skip Tests):**
    ```bash
    mvn -f java/pom.xml clean package -DskipTests
    ```
-   **Artifact Location:**
    The JAR is built to `java/target/dataflow-pubsub-to-bq-json-1.0-SNAPSHOT.jar`.

## 3. Testing & Verification

Both implementations include unit tests for critical transformations (JSON parsing, DLQ routing).

### Python
**Tool:** `pytest`
-   **Run Tests:** `uv run pytest`

### Java
**Tool:** `junit`
-   **Run Tests:** `mvn -f java/pom.xml test`

## 4. Linting & Formatting

### Python
**Tool:** `ruff`
-   **Lint:** `uv run ruff check .`
-   **Format:** `uv run ruff format .`

### Java
**Style:** **2 spaces** indentation (Standard Java).
-   **Format:** Ensure code is formatted consistently (2 spaces) before committing.
-   **Imports:** Clean up unused imports.

## 5. Code Style & Conventions

### Python Guidelines
-   **Indentation:** 4 spaces.
-   **Type Hints:** Mandatory for all function signatures.
-   **Docstrings:** Google Style (`Args:`, `Returns:`, `Raises:`).
-   **Naming:** `snake_case` for functions/vars, `CamelCase` for classes.
-   **Imports:** Grouped (StdLib, ThirdParty, Local), sorted alphabetically.

### Java Guidelines
-   **Indentation:** **2 spaces**.
-   **Naming:**
    -   Classes: `PascalCase` (e.g., `PubSubToBigQueryJson`).
    -   Methods/Variables: `camelCase` (e.g., `getSubscription`).
    -   Constants: `UPPER_SNAKE_CASE`.
-   **Javadoc:** Required for public classes and methods.
-   **Structure:**
    ```java
    package com.johanesalxd;
    
    import ...;
    
    public class MyClass {
      // 2 spaces indentation
      public void myMethod() {
        ...
      }
    }
    ```

## 6. Repository Structure

```
dataflow-pubsub-to-bq-examples-py/
├── dataflow_pubsub_to_bq/      # [Python] Main Source Code
│   ├── pipeline.py             # Entry point (Standard)
│   ├── pipeline_json.py        # Entry point (JSON)
│   ├── pipeline_options.py     # Options configuration
│   └── transforms/             # Custom Beam PTransforms
├── tests/                      # [Python] Unit Tests
│   ├── test_json_to_tablerow.py
│   └── test_raw_json.py
├── java/                       # [Java] Project Root
│   ├── pom.xml                 # Maven configuration
│   └── src/
│       ├── main/java/com/johanesalxd/
│       │   ├── PubSubToBigQueryJson.java
│       │   ├── transforms/
│       │   └── schemas/
│       └── test/java/com/johanesalxd/transforms/
│           └── PubsubMessageToRawJsonTest.java
├── run_dataflow.sh             # [Python] Execution script (Standard)
├── run_dataflow_json.sh        # [Python] Execution script (JSON)
├── run_dataflow_json_java.sh   # [Java] Execution script
└── pyproject.toml              # [Python] Dependencies
```

## 7. Workflow for Agents

1.  **Identify Language:** Determine if the task is for Python (root) or Java (`java/`).
2.  **Contextualize:** Read relevant config (`pyproject.toml` or `java/pom.xml`) and execution scripts (`run_*.sh`) to understand how the code is built and deployed.
3.  **Develop:**
    -   **Python:** Edit in `dataflow_pubsub_to_bq/`. Use Type Hints.
    -   **Java:** Edit in `java/src/...`. Use 2-space indentation.
4.  **Verify:**
    -   **Python:** Lint (`ruff`) and Test (`pytest`).
    -   **Java:** Compile (`mvn package`) and Test (`mvn test`).
5.  **Finalize:** Ensure code compiles/runs and all debug artifacts are removed.
