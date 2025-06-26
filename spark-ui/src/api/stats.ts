import {  useQuery } from "@tanstack/react-query";
import { useClient } from "./client";


interface StatsResponse {
  overview: {
    total_jobs: number;
    completed_jobs: number;
    failed_jobs: number;
    running_jobs: number;
    success_rate: number;
  };
  performance: {
    avg_duration_seconds: number;
    avg_duration_formatted: string;
    total_data_processed_mb: number;
    total_data_processed_gb: number;
  };
  job_types: Record<string, number>;
  recent_activity: {
    jobs_last_24h: number;
    failures_last_24h: number;
  };
  errors: {
    total_errors: number;
    total_warnings: number;
  };
  last_updated: string;
}

export const useStats = () => {
  const client = useClient()


  return useQuery({
    queryFn: async () => {
      const response = await client
        .get<StatsResponse>('/stats');
      return response.data;
    },


    queryKey: ["stats"]})
}
