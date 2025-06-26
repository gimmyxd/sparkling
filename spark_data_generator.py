import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os
import uuid
from typing import List, Dict, Tuple
import json

class SparkJobDataGenerator:
    def __init__(self, output_dir: str = "spark_data"):
        self.output_dir = output_dir
        self.ensure_output_dirs()

        # Job types with different characteristics
        self.job_types = {
            "ETL_Pipeline": {
                "operators": ["ReadCSV", "Filter", "Join", "Transform", "WriteParquet"],
                "avg_duration": 300,  # 5 minutes
                "failure_rate": 0.05
            },
            "ML_Training": {
                "operators": ["ReadParquet", "FeatureEngineering", "MLTransform", "ModelTrain", "ModelSave"],
                "avg_duration": 1800,  # 30 minutes
                "failure_rate": 0.10
            },
            "Analytics_Query": {
                "operators": ["ReadParquet", "Filter", "GroupBy", "Aggregate", "Sort"],
                "avg_duration": 120,  # 2 minutes
                "failure_rate": 0.02
            },
            "Data_Migration": {
                "operators": ["ReadDatabase", "Transform", "Validate", "WriteDatabase"],
                "avg_duration": 900,  # 15 minutes
                "failure_rate": 0.08
            },
            "Report_Generation": {
                "operators": ["ReadMultiple", "Join", "Aggregate", "Format", "WriteCSV"],
                "avg_duration": 180,  # 3 minutes
                "failure_rate": 0.03
            }
        }

        # Common error messages
        self.error_messages = [
            "OutOfMemoryError: Java heap space",
            "FileNotFoundException: Input path does not exist",
            "AnalysisException: Column 'user_id' cannot be resolved",
            "SparkException: Task failed while writing rows",
            "TimeoutException: Futures timed out after [300 seconds]",
            "IllegalArgumentException: Invalid partition column",
            "IOException: Failed to write to output path",
            "ParseException: Failed to parse CSV file"
        ]

    def ensure_output_dirs(self):
        """Create output directories if they don't exist"""
        dirs = [
            self.output_dir,
            f"{self.output_dir}/jobs",
            f"{self.output_dir}/operators",
            f"{self.output_dir}/errors"
        ]
        for dir_path in dirs:
            os.makedirs(dir_path, exist_ok=True)

    def generate_job_id(self) -> str:
        """Generate a realistic Spark job ID"""
        return f"application_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{random.randint(1000, 9999)}"

    def generate_jobs_data(self, num_jobs: int = 100000) -> pd.DataFrame:
        """Generate synthetic jobs data"""
        jobs = []
        base_time = datetime.now() - timedelta(days=7)  # Last 7 days

        for i in range(num_jobs):
            job_type = random.choice(list(self.job_types.keys()))
            job_config = self.job_types[job_type]

            # Generate timing
            start_time = base_time + timedelta(
                seconds=random.randint(0, 7*24*3600)  # Random time in last 7 days
            )

            # Duration with some randomness
            base_duration = job_config["avg_duration"]
            duration = max(30, int(np.random.normal(base_duration, base_duration * 0.3)))

            # Determine if job failed
            failed = random.random() < job_config["failure_rate"]

            if failed:
                # Failed jobs typically run shorter
                duration = int(duration * random.uniform(0.1, 0.8))
                status = "FAILED"
                end_time = start_time + timedelta(seconds=duration)
            else:
                status = "COMPLETED"
                end_time = start_time + timedelta(seconds=duration)

            # Generate resource usage
            num_executors = random.choice([2, 4, 8, 16, 32])
            num_stages = len(job_config["operators"])
            num_tasks = num_stages * num_executors * random.randint(1, 4)

            # Data sizes (MB)
            input_size = random.uniform(10, 10000)  # 10MB to 10GB
            output_size = input_size * random.uniform(0.3, 1.2) if not failed else 0

            # Memory usage
            memory_per_executor = random.choice([1024, 2048, 4096, 8192])  # MB
            total_memory = memory_per_executor * num_executors

            job = {
                "job_id": self.generate_job_id(),
                "job_name": f"{job_type}_{i+1:03d}",
                "job_type": job_type,
                "status": status,
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration,
                "num_stages": num_stages,
                "num_tasks": num_tasks,
                "num_executors": num_executors,
                "input_size_mb": round(input_size, 2),
                "output_size_mb": round(output_size, 2),
                "memory_per_executor_mb": memory_per_executor,
                "total_memory_mb": total_memory,
                "cpu_cores_per_executor": random.choice([2, 4, 8]),
                "shuffle_read_mb": round(random.uniform(0, input_size * 0.5), 2),
                "shuffle_write_mb": round(random.uniform(0, input_size * 0.3), 2),
                "gc_time_ms": random.randint(1000, 50000) if not failed else random.randint(500, 5000)
            }

            jobs.append(job)

        return pd.DataFrame(jobs)

    def generate_operators_data(self, jobs_df: pd.DataFrame) -> pd.DataFrame:
        """Generate operator-level data for each job"""
        operators = []

        for _, job in jobs_df.iterrows():
            job_type = job['job_type']
            job_operators = self.job_types[job_type]["operators"]
            job_duration = job['duration_seconds']
            failed = job['status'] == 'FAILED'

            # Distribute duration across operators (some are more expensive)
            operator_weights = {
                "ReadCSV": 0.15, "ReadParquet": 0.10, "ReadDatabase": 0.20, "ReadMultiple": 0.18,
                "Filter": 0.05, "Transform": 0.15, "FeatureEngineering": 0.25,
                "Join": 0.20, "GroupBy": 0.15, "Aggregate": 0.10,
                "MLTransform": 0.30, "ModelTrain": 0.40, "ModelSave": 0.05,
                "Sort": 0.12, "Validate": 0.08, "Format": 0.06,
                "WriteParquet": 0.12, "WriteCSV": 0.08, "WriteDatabase": 0.18
            }

            # Generate execution order and dependencies
            total_weight = sum(operator_weights.get(op, 0.10) for op in job_operators)
            cumulative_time = 0

            for i, operator in enumerate(job_operators):
                weight = operator_weights.get(operator, 0.10)
                base_duration = int((weight / total_weight) * job_duration)

                # Add some randomness
                op_duration = max(5, int(np.random.normal(base_duration, base_duration * 0.2)))

                # If job failed, make later operators potentially fail or not run
                if failed and i >= len(job_operators) // 2:
                    if random.random() < 0.3:  # 30% chance this operator failed
                        op_status = "FAILED"
                        op_duration = int(op_duration * random.uniform(0.1, 0.5))
                    elif random.random() < 0.5:  # 50% chance it didn't run
                        op_status = "SKIPPED"
                        op_duration = 0
                    else:
                        op_status = "COMPLETED"
                else:
                    op_status = "COMPLETED"

                # Start time is cumulative
                op_start_time = job['start_time'] + timedelta(seconds=cumulative_time)
                op_end_time = op_start_time + timedelta(seconds=op_duration)
                cumulative_time += op_duration

                # Resource usage per operator
                records_processed = random.randint(1000, 1000000)
                memory_usage = random.uniform(100, job['memory_per_executor_mb'] * 0.8)

                operator_data = {
                    "job_id": job['job_id'],
                    "operator_id": f"{job['job_id']}_op_{i:02d}",
                    "operator_name": operator,
                    "operator_type": operator.split('_')[0] if '_' in operator else operator,
                    "stage_id": i,
                    "status": op_status,
                    "start_time": op_start_time,
                    "end_time": op_end_time,
                    "duration_seconds": op_duration,
                    "records_processed": records_processed if op_status == "COMPLETED" else 0,
                    "memory_usage_mb": round(memory_usage, 2),
                    "cpu_time_ms": op_duration * 1000 * random.uniform(0.3, 0.9),
                    "spill_memory_mb": round(random.uniform(0, memory_usage * 0.2), 2),
                    "shuffle_read_records": random.randint(0, records_processed // 2),
                    "shuffle_write_records": random.randint(0, records_processed // 3),
                    "dependencies": json.dumps([f"{job['job_id']}_op_{j:02d}"
                                              for j in range(max(0, i-2), i)]) if i > 0 else "[]"
                }

                operators.append(operator_data)

        return pd.DataFrame(operators)

    def generate_errors_data(self, jobs_df: pd.DataFrame, operators_df: pd.DataFrame) -> pd.DataFrame:
        """Generate error data for failed jobs and operators"""
        errors = []

        # Job-level errors
        failed_jobs = jobs_df[jobs_df['status'] == 'FAILED']
        for _, job in failed_jobs.iterrows():
            error = {
                "job_id": job['job_id'],
                "operator_id": None,
                "error_type": "JOB_FAILURE",
                "error_message": random.choice(self.error_messages),
                "error_timestamp": job['end_time'],
                "stack_trace": self.generate_stack_trace(),
                "retry_count": random.randint(0, 3),
                "is_recoverable": random.choice([True, False])
            }
            errors.append(error)

        # Operator-level errors
        failed_operators = operators_df[operators_df['status'] == 'FAILED']
        for _, operator in failed_operators.iterrows():
            error = {
                "job_id": operator['job_id'],
                "operator_id": operator['operator_id'],
                "error_type": "OPERATOR_FAILURE",
                "error_message": random.choice(self.error_messages),
                "error_timestamp": operator['end_time'],
                "stack_trace": self.generate_stack_trace(),
                "retry_count": random.randint(0, 2),
                "is_recoverable": random.choice([True, False])
            }
            errors.append(error)

        # Add some warnings for successful jobs
        successful_jobs = jobs_df[jobs_df['status'] == 'COMPLETED'].sample(n=min(20, len(jobs_df)//5))
        for _, job in successful_jobs.iterrows():
            if random.random() < 0.3:  # 30% chance of warning
                error = {
                    "job_id": job['job_id'],
                    "operator_id": None,
                    "error_type": "WARNING",
                    "error_message": "High memory usage detected",
                    "error_timestamp": job['start_time'] + timedelta(seconds=random.randint(30, job['duration_seconds']-30)),
                    "stack_trace": None,
                    "retry_count": 0,
                    "is_recoverable": True
                }
                errors.append(error)

        return pd.DataFrame(errors)

    def generate_stack_trace(self) -> str:
        """Generate a realistic stack trace"""
        traces = [
            """java.lang.OutOfMemoryError: Java heap space
    at java.util.Arrays.copyOf(Arrays.java:3332)
    at org.apache.spark.sql.catalyst.expressions.UnsafeRow.copy(UnsafeRow.scala:600)
    at org.apache.spark.sql.execution.aggregate.HashAggregateExec.doExecute(HashAggregateExec.scala:156)""",

            """org.apache.spark.SparkException: Task failed while writing rows
    at org.apache.spark.sql.execution.datasources.FileFormatWriter$.write(FileFormatWriter.scala:200)
    at org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand.run(InsertIntoHadoopFsRelationCommand.scala:159)""",

            """java.io.FileNotFoundException: File does not exist: /path/to/input.csv
    at org.apache.hadoop.fs.RawLocalFileSystem.deprecatedGetFileStatus(RawLocalFileSystem.java:611)
    at org.apache.spark.sql.execution.datasources.csv.CSVFileFormat.readFile(CSVFileFormat.scala:89)"""
        ]
        return random.choice(traces)

    def generate_all_data(self, num_jobs: int = 100000):
        """Generate all synthetic data and save to parquet files"""
        print(f"Generating synthetic data for {num_jobs} Spark jobs...")

        # Generate jobs data
        print("1. Generating jobs data...")
        jobs_df = self.generate_jobs_data(num_jobs)
        jobs_df.to_parquet(f"{self.output_dir}/jobs/spark_jobs.parquet", index=False)
        print(f"   Saved {len(jobs_df)} jobs to spark_jobs.parquet")

        # Generate operators data
        print("2. Generating operators data...")
        # Need to import json for dependencies
        import json
        operators_df = self.generate_operators_data(jobs_df)
        operators_df.to_parquet(f"{self.output_dir}/operators/spark_operators.parquet", index=False)
        print(f"   Saved {len(operators_df)} operators to spark_operators.parquet")

        # Generate errors data
        print("3. Generating errors data...")
        errors_df = self.generate_errors_data(jobs_df, operators_df)
        errors_df.to_parquet(f"{self.output_dir}/errors/spark_errors.parquet", index=False)
        print(f"   Saved {len(errors_df)} errors to spark_errors.parquet")

        # Generate summary statistics
        self.generate_summary_stats(jobs_df, operators_df, errors_df)

        print("\n✅ Synthetic data generation completed!")
        print(f"📁 Data saved in: {self.output_dir}/")
        print(f"📊 Summary:")
        print(f"   - {len(jobs_df)} jobs ({len(jobs_df[jobs_df['status'] == 'COMPLETED'])} completed, {len(jobs_df[jobs_df['status'] == 'FAILED'])} failed)")
        print(f"   - {len(operators_df)} operators")
        print(f"   - {len(errors_df)} error/warning records")

    def generate_summary_stats(self, jobs_df: pd.DataFrame, operators_df: pd.DataFrame, errors_df: pd.DataFrame):
        """Generate summary statistics"""
        stats = {
            "total_jobs": len(jobs_df),
            "completed_jobs": len(jobs_df[jobs_df['status'] == 'COMPLETED']),
            "failed_jobs": len(jobs_df[jobs_df['status'] == 'FAILED']),
            "total_operators": len(operators_df),
            "total_errors": len(errors_df),
            "avg_job_duration": jobs_df['duration_seconds'].mean(),
            "total_data_processed_mb": jobs_df['input_size_mb'].sum(),
            "job_types": jobs_df['job_type'].value_counts().to_dict(),
            "generation_timestamp": datetime.now().isoformat()
        }

        stats_df = pd.DataFrame([stats])
        stats_df.to_parquet(f"{self.output_dir}/summary_stats.parquet", index=False)


def main():
    """Main function to run the data generator"""
    generator = SparkJobDataGenerator()
    generator.generate_all_data(num_jobs=100000)

    # Display sample data
    print("\nSample data preview:")
    print("\nJobs sample:")
    jobs_df = pd.read_parquet(f"{generator.output_dir}/jobs/spark_jobs.parquet")
    print(jobs_df[['job_id', 'job_name', 'status', 'duration_seconds', 'num_executors']].head())

    print("\nOperators sample:")
    operators_df = pd.read_parquet(f"{generator.output_dir}/operators/spark_operators.parquet")
    print(operators_df[['job_id', 'operator_name', 'status', 'duration_seconds', 'records_processed']].head())


if __name__ == "__main__":
    main()
