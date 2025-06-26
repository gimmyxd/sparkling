import {
  BarChart,
  Bar,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  ResponsiveContainer,
} from "recharts";
import {
  Activity,
  TrendingUp,
  Clock,
  AlertCircle,
  CheckCircle,
  XCircle,
  PlayCircle,
  HardDrive,
  AlertTriangle,
} from "lucide-react";
import { useStats } from "../../api/stats";
import StatCard from "../Atoms/Card";

const COLORS = [
  "#F7DC6F",
  "#85929E",
  "#F1948A",
  "#7FB3D5",
  "#82ca9d",
  "#FFBB28",
  "#FF8042",
];

const SparkDashboard = () => {
  const { data: stats, error, isLoading } = useStats();
  if (isLoading)
    return (
      <div className="min-h-screen flex items-center justify-center text-gray-600">
        Loading...
      </div>
    );
  if (error || !stats)
    return (
      <div className="min-h-screen flex items-center justify-center text-red-400">
        {error?.message || "No data"}
      </div>
    );

  const statusData = [
    {
      name: "Completed",
      value: stats.overview.completed_jobs,
      fill: "#85929E",
      icon: <CheckCircle className="w-4 h-4 text-green-600" />,
    },
    {
      name: "Running",
      value: stats.overview.running_jobs,
      fill: "#F7DC6F",
      icon: <PlayCircle className="w-4 h-4 text-yellow-600" />,
    },
    {
      name: "Failed",
      value: stats.overview.failed_jobs,
      fill: "#F1948A",
      icon: <XCircle className="w-4 h-4 text-red-400" />,
    },
  ];

  const jobTypeEntries = Object.entries(stats.job_types || {});
  const totalJobs = stats.overview.total_jobs;
  const jobTypePieData = jobTypeEntries.map(([type, count], i) => ({
    name: type.replace(/_/g, " "),
    value: count,
    fill: COLORS[i % COLORS.length],
  }));
  const topJobTypes = jobTypeEntries
    .map(([type, count]) => ({
      type: type.replace(/_/g, " "),
      count,
      percent: ((count / totalJobs) * 100).toFixed(1),
    }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 5);

  return (
    <div className="min-h-screen bg-gray-50 transition-all duration-300">
      <div className="px-6 py-6 pt-20">
        <div className="flex items-center justify-end mb-2">
          <span className="text-sm text-gray-500 p-4 right">
            Last updated: {new Date(stats.last_updated).toLocaleString()}
          </span>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
          <StatCard
            icon={Activity}
            title="Total Jobs"
            value={stats.overview.total_jobs.toLocaleString()}
            color="bg-yellow-100"
          />
          <StatCard
            icon={CheckCircle}
            title="Completed Jobs"
            value={stats.overview.completed_jobs.toLocaleString()}
            color="bg-green-100"
          />
          <StatCard
            icon={XCircle}
            title="Failed Jobs"
            value={stats.overview.failed_jobs.toLocaleString()}
            color="bg-red-100"
          />
          <StatCard
            icon={PlayCircle}
            title="Running Jobs"
            value={stats.overview.running_jobs.toLocaleString()}
            color="bg-blue-100"
          />
          <StatCard
            icon={TrendingUp}
            title="Success Rate"
            value={`${stats.overview.success_rate}%`}
            color="bg-green-50"
          />
          <StatCard
            icon={Clock}
            title="Avg Duration"
            value={stats.performance.avg_duration_formatted}
            color="bg-gray-100"
          />
          <StatCard
            icon={HardDrive}
            title="Total Data Processed"
            value={`${stats.performance.total_data_processed_gb} GB`}
            color="bg-blue-50"
          />
          <StatCard
            icon={AlertCircle}
            title="Errors"
            value={stats.errors.total_errors}
            color="bg-red-50"
          />
          <StatCard
            icon={AlertTriangle}
            title="Warnings"
            value={stats.errors.total_warnings}
            color="bg-yellow-50"
          />
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
          <div className="bg-white rounded-xl p-6 shadow-sm border border-gray-100">
            <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
              Job Status Distribution
            </h3>
            <ResponsiveContainer width="100%" height={250}>
              <BarChart
                data={statusData}
                margin={{ top: 10, right: 30, left: 0, bottom: 0 }}
              >
                <XAxis dataKey="name" />
                <YAxis allowDecimals={false} />
                <Bar dataKey="value">
                  {statusData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={entry.fill} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>

          <div className="bg-white rounded-xl p-6 shadow-sm border border-gray-100">
            <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
              Job Type Distribution
            </h3>
            <ResponsiveContainer width="100%" height={250}>
              <PieChart>
                <Pie
                  data={jobTypePieData}
                  dataKey="value"
                  nameKey="name"
                  cx="50%"
                  cy="50%"
                  outerRadius={90}
                  label={({ name, percent }) =>
                    `${name} (${((percent ?? 0) * 100).toFixed(0)}%)`
                  }
                >
                  {jobTypePieData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={entry.fill} />
                  ))}
                </Pie>
              </PieChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="bg-white rounded-xl p-6 shadow-sm border border-gray-100 mb-8">
          <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
            Top Job Types
          </h3>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-gray-200">
                  <th className="text-left py-3 px-2 text-sm font-medium text-gray-500">
                    Job Type
                  </th>
                  <th className="py-3 px-2 text-sm font-medium text-gray-500">
                    Count
                  </th>
                  <th className="py-3 px-2 text-sm font-medium text-gray-500">
                    % of Total
                  </th>
                </tr>
              </thead>
              <tbody>
                {topJobTypes.map((jobType, index) => (
                  <tr
                    key={index}
                    className="border-b border-gray-100 hover:bg-gray-50"
                  >
                    <td className="text-left py-4 px-2 font-medium text-gray-900">
                      {jobType.type}
                    </td>
                    <td className="py-4 px-2 text-gray-600">{jobType.count}</td>
                    <td className="py-4 px-2 text-gray-600">
                      {jobType.percent}%
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
};

export default SparkDashboard;
