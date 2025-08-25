# Optimal Data Core

## Summary

The Optimal Data Core repository serves as the centralized hub for all data processing projects related to Optimal Systems, containing ETL processes for various supermarket data sources.

## Stack

* [Python](https://www.python.org/)
* [uv](https://docs.astral.sh/uv/) - Fast Python package installer and resolver
* [ruff](https://docs.astral.sh/ruff/) - Extremely fast Python linter and formatter
* [Redis](https://redis.io/) - In-memory data structure store (implemented selectively)

## Build

This repository uses a multi-project [uv](https://docs.astral.sh/uv/)-based build system for Python projects.

### Prerequisites

* [python 3.11+](https://www.python.org/downloads/)
* [uv](https://docs.astral.sh/uv/getting-started/installation.html)

### Under the data folder:

#### Install dependencies for a specific project

```bash
cd <subproject>
uv sync
```

#### Run a specific project

```bash
cd <subproject>
uv run main.py
```

## Deployment

### Infrastructure dependencies

- Python 3.11+
- Redis (latest version) - Optional, project-specific implementation

### Environment variables

| Variable           | Description                                    | Example value                    |
|--------------------|------------------------------------------------|----------------------------------|
| REDIS_URL          | Redis connection URL                           | redis://localhost:6379          |
| REDIS_PASSWORD     | Redis authentication password                   | redis-pass                      |
| REDIS_DB           | Redis database number                          | 0                               |
| LOG_LEVEL          | Logging level for the application              | INFO                            |
| ENVIRONMENT        | Environment name (dev, staging, prod)          | development                     |

### Project Structure

Each supermarket project is implemented as an independent UV project within this repository:

```
data/
├── carrefour/          # Carrefour ETL project
├── ahorramas/          # Ahorramas ETL project
└── shared/             # Shared utilities and components
```

### Data Processing

ETL processes are designed to handle:
- Data extraction from various supermarket APIs
- Data transformation and cleaning
- Data loading into target systems
- Caching strategies using Redis where applicable
