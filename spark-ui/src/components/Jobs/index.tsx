import { CheckCircle, Info, PlayCircle, XCircle } from "lucide-react";
import { useMemo, useState } from "react";
import { useJobs, useJobTypes } from "../../api/jobs";

const Jobs = () => {
  const [jobStatusFilter, setJobStatusFilter] = useState<string>("ALL");
  const [jobTypeFilter, setJobTypeFilter] = useState<string>("ALL");
  const [jobPage, setJobPage] = useState<number>(0);
  const [jobPageSize, setJobPageSize] = useState<number>(20);

  const {
    data: jobsData,
    isLoading: jobsLoading,
    error,
  } = useJobs({
    limit: jobPageSize,
    offset: jobPage * jobPageSize,
    status: jobStatusFilter,
    job_type: jobTypeFilter,
  });

  const [jobs, jobsTotal] = useMemo(() => {
    return [jobsData?.jobs || [], jobsData?.pagination?.total || 0];
  }, [jobsData]);

  const totalPages = Math.ceil(jobsTotal / jobPageSize);
  const canPrev = jobPage > 0;
  const canNext = jobPage < totalPages - 1;

  const statusOptions = ["ALL", "COMPLETED", "FAILED", "RUNNING"];

  const { data } = useJobTypes();
  const jobTypeOptions = useMemo(() => {
    return ["ALL", ...(data?.job_types || [])];
  }, [data?.job_types]);

  const statusIcon = (status: string) => {
    switch (status) {
      case "COMPLETED":
        return <CheckCircle className="w-4 h-4 text-green-600" />;
      case "FAILED":
        return <XCircle className="w-4 h-4 text-red-600" />;
      case "RUNNING":
        return <PlayCircle className="w-4 h-4 text-yellow-600" />;
      default:
        return <Info className="w-4 h-4 text-gray-400" />;
    }
  };

  const statusColor = (status: string) => {
    switch (status) {
      case "COMPLETED":
        return "text-green-700";
      case "FAILED":
        return "text-red-700";
      case "RUNNING":
        return "text-yellow-700";
      default:
        return "text-gray-700";
    }
  };

  if (error)
    return (
      <div className="min-h-screen flex items-center justify-center text-red-600">
        {error.message || "No data"}
      </div>
    );
  return (
    <>
      <div
        className="pl-8 pr-8 pt-8 fixed top-14 left-0 w-full z-40 bg-white shadow flex flex-wrap items-center gap-4 mb-4 p-6 border-b border-gray-100"
        style={{ minHeight: 58 }}
      >
        <label className="text-sm font-medium text-gray-700">Status</label>
        <select
          className="border rounded px-2 py-1 text-sm"
          value={jobStatusFilter}
          onChange={(e) => {
            setJobStatusFilter(e.target.value);
            setJobPage(0);
          }}
        >
          {statusOptions.map((opt) => (
            <option key={opt} value={opt}>
              {opt === "ALL" ? "All Statuses" : opt}
            </option>
          ))}
        </select>
        <label className="text-sm font-medium text-gray-700">Job Type:</label>
        <select
          className="border rounded px-2 py-1 text-sm min-w-40"
          value={jobTypeFilter}
          onChange={(e) => {
            setJobTypeFilter(e.target.value);
            setJobPage(0);
          }}
        >
          {jobTypeOptions.map((opt) => (
            <option key={opt} value={opt}>
              {opt === "ALL" ? "All Types" : opt.replace(/_/g, " ")}
            </option>
          ))}
        </select>
        <label className="text-sm font-medium text-gray-700">Page Size:</label>
        <select
          className="border rounded px-2 py-1 text-sm"
          value={jobPageSize}
          onChange={(e) => {
            setJobPageSize(Number(e.target.value));
            setJobPage(0);
          }}
        >
          {[10, 20, 50, 100].map((size) => (
            <option key={size} value={size}>
              {size}
            </option>
          ))}
        </select>
        <span hidden={jobsLoading} className="ml-auto text-sm text-gray-500">
          Page {jobPage + 1} of {totalPages || 1}
        </span>
        <button
          hidden={jobsLoading}
          className="px-2 py-1 border rounded disabled:opacity-50"
          onClick={() => setJobPage(jobPage - 1)}
          disabled={!canPrev}
        >
          Prev
        </button>
        <button
          hidden={jobsLoading}
          className="px-2 py-1 border rounded disabled:opacity-50"
          onClick={() => setJobPage(jobPage + 1)}
          disabled={!canNext}
        >
          Next
        </button>
      </div>

      <div className="bg-white rounded-xl p-6 shadow-sm border border-gray-100 mt-26">
        <div className="overflow-x-auto h-[76vh] pt-4">
          <table className="w-full">
            <thead className="sticky top-0 bg-white">
              <tr className="border-b border-gray-200">
                <th className=" py-3 px-2 font-medium text-gray-500">
                  Job Name
                </th>
                <th className=" py-3 px-2 font-medium text-gray-500">
                  Job Type
                </th>
                <th className=" py-3 px-2 font-medium text-gray-500">Status</th>
                <th className=" py-3 px-2 font-medium text-gray-500">
                  Duration
                </th>
                <th className=" py-3 px-2 font-medium text-gray-500">
                  Start Time
                </th>
                <th className=" py-3 px-2 font-medium text-gray-500">
                  End Time
                </th>
              </tr>
            </thead>
            <tbody>
              {jobsLoading ? (
                <tr>
                  <td colSpan={6} className="text-center py-8 text-gray-500">
                    Loading jobs...
                  </td>
                </tr>
              ) : jobs.length === 0 ? (
                <tr>
                  <td colSpan={6} className="text-center py-8 text-gray-500">
                    No jobs found.
                  </td>
                </tr>
              ) : (
                jobs.map((job) => (
                  <tr
                    key={job.job_id}
                    className="border-b border-gray-100 hover:bg-gray-50"
                  >
                    <td className="py-4 px-2 font-medium text-gray-900">
                      {job.job_name}
                    </td>
                    <td className="py-4 px-2 text-gray-600">
                      {job.job_type.replace(/_/g, " ")}
                    </td>
                    <td
                      className={`py-4 px-2 font-semibold flex items-center gap-2 ${statusColor(
                        job.status
                      )}`}
                    >
                      {statusIcon(job.status)} {job.status}
                    </td>
                    <td className="py-4 px-2 text-gray-600">
                      {job.duration_formatted}
                    </td>
                    <td className="py-4 px-2 text-gray-600">
                      {new Date(job.start_time).toLocaleString()}
                    </td>
                    <td className="py-4 px-2 text-gray-600">
                      {new Date(job.end_time).toLocaleString()}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
};

export default Jobs;
