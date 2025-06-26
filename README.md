# Spark Job Monitor

A full-stack application for generating, serving, and visualizing synthetic Apache Spark job data. This project consists of three main components:

- **Data Generator** (`spark_data_generator.py`): Generates synthetic Spark job, operator, and error data.
- **API Server** (`api_server.py`): Serves the generated data via a RESTful API.
- **Frontend UI** (`/spark-ui`): A React + TypeScript + Vite web application for visualizing and exploring the data.

---

## Project Structure

```
.
├── spark_data_generator.py   # Synthetic data generator (Python)
├── api_server.py             # FastAPI backend server (Python)
├── spark_data/               # Generated data (Parquet files)
└── spark-ui/                 # Frontend React app (TypeScript, Vite)
```

---

## 1. Data Generator (`spark_data_generator.py`)

**Scope:**
- Generates large-scale, realistic synthetic data for Spark jobs, operators, and errors.
- Outputs data as Parquet files in the `spark_data/` directory.
- Useful for testing, demos, and analytics without needing a real Spark cluster.

**How to Run:**

```bash
pip install pandas numpy pyarrow
python spark_data_generator.py
```

- By default, generates 100,000 jobs and related data.
- Output files:
  - `spark_data/jobs/spark_jobs.parquet`
  - `spark_data/operators/spark_operators.parquet`
  - `spark_data/errors/spark_errors.parquet`
  - `spark_data/summary_stats.parquet`


The data generator creates three main datasets:

1. **Jobs Data** (`spark_jobs.parquet`):
   - Each row represents a Spark job, with fields such as job ID, job name, job type (e.g., ETL, ML Training, Analytics), status (COMPLETED/FAILED), start/end times, duration, resource usage (executors, memory, CPU), data sizes, and shuffle metrics.
   - Job types are modeled with different operator pipelines, average durations, and failure rates to simulate real-world diversity.
   - Job durations, resource usage, and data sizes are randomized within realistic ranges.
   - Both successful and failed jobs are generated, with failures occurring at type-specific rates.

2. **Operators Data** (`spark_operators.parquet`):
   - Each row represents an operator (stage/task) within a job, including operator name/type, execution order, status (COMPLETED/FAILED/SKIPPED), timing, records processed, memory/CPU usage, shuffle metrics, and dependencies.
   - Operator durations are distributed based on typical weights (e.g., joins and ML training take longer).
   - Failed jobs may have failed or skipped operators, simulating partial pipeline execution.

3. **Errors Data** (`spark_errors.parquet`):
   - Contains error and warning records at both job and operator levels.
   - Includes error type (job failure, operator failure, warning), error message, timestamp, stack trace, retry count, and recoverability.
   - Warnings (e.g., high memory usage) are also generated for some successful jobs.

4. **Summary Statistics** (`summary_stats.parquet`):
   - Aggregated statistics about the generated dataset: job counts, failure rates, average durations, total data processed, and job type breakdowns.
---

## 2. API Server (`api_server.py`)

**Scope:**
- FastAPI backend that loads the generated Parquet data and exposes RESTful endpoints for jobs, operators, errors, stats, and more.
- Supports filtering, pagination, and statistics aggregation.

**How to Run:**

```bash
pip install fastapi uvicorn pandas pyarrow pydantic
uvicorn main:app --host 0.0.0.0 --port 3001 --reload
```

- The server will start on `http://localhost:3001` by default.
- API documentation available at `http://localhost:3001/docs`
- Endpoints include:
  - `/api/jobs` (list/filter jobs)
  - `/api/jobs/{id}` (job details)
  - `/api/jobs/{id}/operators` (job operators)
  - `/api/jobs/{id}/errors` (job errors)
  - `/api/jobs/{id}/timeline` (job timeline)
  - `/api/stats` (system statistics)
  - `/api/job-types` (available job types)
  - `/api/refresh` (refresh data cache)

---

## 3. Frontend UI (`/spark-ui`)

**Scope:**
- Modern React + TypeScript + Vite web application for visualizing Spark job data.
- Connects to the API server for data.
- Features dashboards, job lists, operator timelines, error views, and more.

**How to Run:**

```bash
cd spark-ui
# Install dependencies
yarn install
# Start the development server
yarn dev
```

- The app will be available at `http://localhost:5173` (or as indicated in the terminal).
- Make sure the API server is running and accessible at `http://localhost:3001`.

