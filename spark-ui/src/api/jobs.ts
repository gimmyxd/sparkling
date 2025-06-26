import {  keepPreviousData, useQuery } from "@tanstack/react-query";
import { useClient } from "./client";


export interface Job {
  job_id: string;
  job_name: string;
  job_type: string;
  status: string;
  start_time: string;
  end_time: string;
  duration_seconds: number;
  num_stages: number;
  num_tasks: number;
  num_executors: number;
  input_size_mb: number;
  output_size_mb: number;
  memory_per_executor_mb: number;
  total_memory_mb: number;
  cpu_cores_per_executor: number;
  shuffle_read_mb: number;
  shuffle_write_mb: number;
  gc_time_ms: number;
  duration_formatted: string;
}

export interface JobsResponse {
  jobs: Job[];
  pagination: {
    total: number;
    limit: number;
    offset: number;
  };
}

interface UseJobsParams {
  limit: number;
  offset: number;
  status?: string;
  job_type?: string;
}

export const useJobs = ({ limit, offset, status, job_type }: UseJobsParams) => {
  const client = useClient();

  return useQuery({
    queryKey: ["jobs", { limit, offset, status, job_type }],
    placeholderData: keepPreviousData,
    queryFn: async () => {
      const params: Record<string, string | number> = { limit, offset };
      if (status && status !== "ALL") params.status = status;
      if (job_type && job_type !== "ALL") params.job_type = job_type;
      const response = await client.get<JobsResponse>("/jobs", { params });
      return response.data;
    },
  });
};

export const useJobTypes = () => {
  const client = useClient()

  return useQuery({
    queryFn: async () => {
      const response = await client
        .get<{job_types: string[]}>('/job-types');
      return response.data;
    },
    queryKey: ["job-types"]
  })
}


