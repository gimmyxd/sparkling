from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import pandas as pd
import os
import time
from datetime import datetime, timedelta
import json
from pathlib import Path
import uvicorn

class PaginationInfo(BaseModel):
    total: int
    limit: int
    offset: int
    has_more: bool

class JobsResponse(BaseModel):
    jobs: List[Dict[str, Any]]
    pagination: PaginationInfo

class JobOperatorsResponse(BaseModel):
    job_id: str
    operators: List[Dict[str, Any]]

class JobErrorsResponse(BaseModel):
    job_id: str
    errors: List[Dict[str, Any]]

class JobTimelineResponse(BaseModel):
    job_id: str
    job_start: str
    job_end: str
    total_duration: float
    operators: List[Dict[str, Any]]

class StatsResponse(BaseModel):
    overview: Dict[str, Any]
    performance: Dict[str, Any]
    job_types: Dict[str, int]
    recent_activity: Dict[str, int]
    errors: Dict[str, int]
    last_updated: str

class JobTypesResponse(BaseModel):
    job_types: List[str]

class HealthResponse(BaseModel):
    status: str
    timestamp: str
    dataDir: str

class RefreshResponse(BaseModel):
    message: str

app = FastAPI(
    title="Spark Job Monitor API",
    description="API for monitoring Spark jobs, operators, and errors",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PORT = int(os.getenv("PORT", 3001))
DATA_DIR = Path(__file__).parent / "spark_data"

cache = {
    "jobs": None,
    "operators": None,
    "errors": None,
    "stats": None,
    "last_update": None
}

CACHE_DURATION = 5 * 60

def read_parquet_file(file_path: Path) -> List[Dict[str, Any]]:
    """Read parquet file and return as list of dictionaries"""
    try:
        df = pd.read_parquet(file_path)
        return df.to_dict('records')
    except Exception as e:
        print(f"Error reading parquet file {file_path}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to read parquet file: {str(e)}")

async def load_data(force_refresh: bool = False) -> Dict[str, Any]:
    """Load data from parquet files with caching"""
    now = time.time()

    if not force_refresh and cache["last_update"] and (now - cache["last_update"]) < CACHE_DURATION:
        return cache

    try:
        print("Loading data from parquet files...")


        jobs_path = DATA_DIR / "jobs" / "spark_jobs.parquet"
        if jobs_path.exists():
            cache["jobs"] = read_parquet_file(jobs_path)


        operators_path = DATA_DIR / "operators" / "spark_operators.parquet"
        if operators_path.exists():
            cache["operators"] = read_parquet_file(operators_path)


        errors_path = DATA_DIR / "errors" / "spark_errors.parquet"
        if errors_path.exists():
            cache["errors"] = read_parquet_file(errors_path)


        stats_path = DATA_DIR / "summary_stats.parquet"
        if stats_path.exists():
            stats_data = read_parquet_file(stats_path)
            cache["stats"] = stats_data[0] if stats_data else {}

        cache["last_update"] = now
        print("Data loaded successfully")

    except Exception as e:
        print(f"Error loading data: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to load data: {str(e)}")

    return cache

def format_duration(seconds: float) -> str:
    """Format duration in seconds to human readable format"""
    hrs = int(seconds // 3600)
    mins = int((seconds % 3600) // 60)
    secs = int(seconds % 60)

    if hrs > 0:
        return f"{hrs}h {mins}m {secs}s"
    elif mins > 0:
        return f"{mins}m {secs}s"
    else:
        return f"{secs}s"

def format_job_data(jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Format job data with proper date formatting"""
    formatted_jobs = []
    for job in jobs:
        formatted_job = job.copy()
        if 'start_time' in job:
            formatted_job['start_time'] = pd.to_datetime(job['start_time']).isoformat()
        if 'end_time' in job:
            formatted_job['end_time'] = pd.to_datetime(job['end_time']).isoformat()
        if 'duration_seconds' in job:
            formatted_job['duration_formatted'] = format_duration(job['duration_seconds'])
        formatted_jobs.append(formatted_job)
    return formatted_jobs

def format_operator_data(operators: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Format operator data with proper date formatting"""
    formatted_operators = []
    for op in operators:
        formatted_op = op.copy()
        if 'start_time' in op:
            formatted_op['start_time'] = pd.to_datetime(op['start_time']).isoformat()
        if 'end_time' in op:
            formatted_op['end_time'] = pd.to_datetime(op['end_time']).isoformat()
        if 'duration_seconds' in op:
            formatted_op['duration_formatted'] = format_duration(op['duration_seconds'])
        if 'dependencies' in op:
            try:
                formatted_op['dependencies'] = json.loads(op['dependencies']) if op['dependencies'] else []
            except:
                formatted_op['dependencies'] = []
        formatted_operators.append(formatted_op)
    return formatted_operators

def format_error_data(errors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Format error data with proper date formatting"""
    formatted_errors = []
    for error in errors:
        formatted_error = error.copy()
        if 'error_timestamp' in error:
            formatted_error['error_timestamp'] = pd.to_datetime(error['error_timestamp']).isoformat()
        formatted_errors.append(formatted_error)
    return formatted_errors


@app.get("/api/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now().isoformat(),
        dataDir=str(DATA_DIR)
    )

@app.get("/api/jobs", response_model=JobsResponse)
async def get_jobs(
    status: Optional[str] = Query(None, description="Filter by job status"),
    job_type: Optional[str] = Query(None, description="Filter by job type"),
    limit: int = Query(50, description="Number of jobs to return"),
    offset: int = Query(0, description="Number of jobs to skip"),
    sort_by: str = Query("start_time", description="Field to sort by"),
    sort_order: str = Query("desc", description="Sort order (asc/desc)")
):
    """Get all jobs with pagination and filtering"""
    try:
        data = await load_data()
        jobs = data.get("jobs", [])


        if status:
            jobs = [job for job in jobs if job.get("status") == status]

        if job_type:
            jobs = [job for job in jobs if job.get("job_type") == job_type]


        reverse = sort_order == "desc"
        if sort_by in ["start_time", "end_time"]:
            jobs.sort(key=lambda x: pd.to_datetime(x.get(sort_by, 0)), reverse=reverse)
        else:
            jobs.sort(key=lambda x: x.get(sort_by, 0), reverse=reverse)


        total = len(jobs)
        start_index = offset
        end_index = start_index + limit
        paginated_jobs = jobs[start_index:end_index]

        return JobsResponse(
            jobs=format_job_data(paginated_jobs),
            pagination=PaginationInfo(
                total=total,
                limit=limit,
                offset=offset,
                has_more=end_index < total
            )
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch jobs: {str(e)}")

@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    """Get specific job details"""
    try:
        data = await load_data()
        jobs = data.get("jobs", [])

        job = next((j for j in jobs if j.get("job_id") == job_id), None)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        return format_job_data([job])[0]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch job: {str(e)}")

@app.get("/api/jobs/{job_id}/operators", response_model=JobOperatorsResponse)
async def get_job_operators(job_id: str):
    """Get operators for a specific job"""
    try:
        data = await load_data()
        operators = data.get("operators", [])

        job_operators = [op for op in operators if op.get("job_id") == job_id]

        if not job_operators:
            raise HTTPException(status_code=404, detail="No operators found for this job")


        job_operators.sort(key=lambda x: x.get("stage_id", 0))

        return JobOperatorsResponse(
            job_id=job_id,
            operators=format_operator_data(job_operators)
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch operators: {str(e)}")

@app.get("/api/jobs/{job_id}/errors", response_model=JobErrorsResponse)
async def get_job_errors(job_id: str):
    """Get errors for a specific job"""
    try:
        data = await load_data()
        errors = data.get("errors", [])

        job_errors = [err for err in errors if err.get("job_id") == job_id]

        return JobErrorsResponse(
            job_id=job_id,
            errors=format_error_data(job_errors)
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch errors: {str(e)}")

@app.get("/api/jobs/{job_id}/timeline", response_model=JobTimelineResponse)
async def get_job_timeline(job_id: str):
    """Get timeline data for a specific job"""
    try:
        data = await load_data()
        jobs = data.get("jobs", [])
        operators = data.get("operators", [])

        job = next((j for j in jobs if j.get("job_id") == job_id), None)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        job_operators = [op for op in operators if op.get("job_id") == job_id]


        job_operators.sort(key=lambda x: pd.to_datetime(x.get("start_time", 0)))

        timeline_operators = []
        for op in job_operators:
            timeline_operators.append({
                "operator_id": op.get("operator_id"),
                "operator_name": op.get("operator_name"),
                "start_time": pd.to_datetime(op.get("start_time")).isoformat(),
                "end_time": pd.to_datetime(op.get("end_time")).isoformat(),
                "duration_seconds": op.get("duration_seconds"),
                "status": op.get("status"),
                "records_processed": op.get("records_processed")
            })

        return JobTimelineResponse(
            job_id=job_id,
            job_start=pd.to_datetime(job.get("start_time")).isoformat(),
            job_end=pd.to_datetime(job.get("end_time")).isoformat(),
            total_duration=job.get("duration_seconds", 0),
            operators=timeline_operators
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch timeline: {str(e)}")

@app.get("/api/stats", response_model=StatsResponse)
async def get_stats():
    """Get overall system statistics"""
    try:
        data = await load_data()
        jobs = data.get("jobs", [])
        operators = data.get("operators", [])
        errors = data.get("errors", [])

        completed_jobs = [j for j in jobs if j.get("status") == "COMPLETED"]
        failed_jobs = [j for j in jobs if j.get("status") == "FAILED"]
        running_jobs = [j for j in jobs if j.get("status") == "RUNNING"]


        job_type_stats = {}
        for job in jobs:
            job_type = job.get("job_type")
            if job_type:
                job_type_stats[job_type] = job_type_stats.get(job_type, 0) + 1


        durations = [j.get("duration_seconds", 0) for j in jobs]
        avg_duration = sum(durations) / len(durations) if durations else 0

        total_data_processed = sum(j.get("input_size_mb", 0) for j in jobs)


        twenty_four_hours_ago = datetime.now() - timedelta(hours=24)
        recent_jobs = [
            job for job in jobs
            if pd.to_datetime(job.get("start_time")) > twenty_four_hours_ago
        ]

        return StatsResponse(
            overview={
                "total_jobs": len(jobs),
                "completed_jobs": len(completed_jobs),
                "failed_jobs": len(failed_jobs),
                "running_jobs": len(running_jobs),
                "success_rate": round(len(completed_jobs) / len(jobs) * 100, 1) if jobs else 0
            },
            performance={
                "avg_duration_seconds": round(avg_duration),
                "avg_duration_formatted": format_duration(round(avg_duration)),
                "total_data_processed_mb": round(total_data_processed),
                "total_data_processed_gb": round(total_data_processed / 1024 * 100) / 100
            },
            job_types=job_type_stats,
            recent_activity={
                "jobs_last_24h": len(recent_jobs),
                "failures_last_24h": len([j for j in recent_jobs if j.get("status") == "FAILED"])
            },
            errors={
                "total_errors": len([e for e in errors if e.get("error_type") != "WARNING"]),
                "total_warnings": len([e for e in errors if e.get("error_type") == "WARNING"])
            },
            last_updated=datetime.now().isoformat()
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch stats: {str(e)}")

@app.get("/api/job-types", response_model=JobTypesResponse)
async def get_job_types():
    """Get all job types for filtering"""
    try:
        data = await load_data()
        jobs = data.get("jobs", [])

        job_types = list(set(job.get("job_type") for job in jobs if job.get("job_type")))

        return JobTypesResponse(job_types=job_types)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch job types: {str(e)}")

@app.post("/api/refresh", response_model=RefreshResponse)
async def refresh_data():
    """Refresh data cache"""
    try:
        await load_data()
        return RefreshResponse(message="Data cache refreshed successfully")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to refresh data: {str(e)}")

@app.on_event("startup")
async def startup_event():
    """Initialize data on startup"""
    print(f"Spark Job Monitor API starting on port {PORT}")
    print(f"Data directory: {DATA_DIR}")

    try:
        await load_data()
        print("Initial data loaded successfully")
    except Exception as e:
        print(f"Failed to load initial data: {e}")
        print("Make sure to run the data generator first!")

    print("\nAvailable endpoints:")
    print("  GET  /api/health - Health check")
    print("  GET  /api/jobs - List all jobs (with pagination & filters)")
    print("  GET  /api/jobs/{id} - Get specific job details")
    print("  GET  /api/jobs/{id}/operators - Get job operators")
    print("  GET  /api/jobs/{id}/errors - Get job errors")
    print("  GET  /api/jobs/{id}/timeline - Get job timeline")
    print("  GET  /api/stats - System statistics")
    print("  GET  /api/job-types - Available job types")
    print("  POST /api/refresh - Refresh data cache")
    print("  GET  /docs - Auto-generated API documentation")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=PORT,
        reload=True,
        log_level="info"
    )
