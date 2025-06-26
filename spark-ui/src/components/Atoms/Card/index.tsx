import type { ComponentType, FC } from "react";

type StatCardProps = {
  icon: ComponentType<{ className?: string }>;
  title: string;
  value: string | number;
  color?: string;
};
const StatCard: FC<StatCardProps> = ({ icon: Icon, title, value, color }) => (
  <div
    className={`bg-white rounded-xl p-6 shadow-sm border border-gray-100 flex gap-14 items-center justify-between`}
  >
    <div className="flex items-center gap-3 justify-center">
      <div className={`p-2 rounded-lg ${color || "bg-yellow-100"}`}>
        {<Icon className="w-5 h-5" />}
      </div>
      <h3 className="text-sm font-medium text-gray-600">{title}</h3>
    </div>
    <div className=" font-bold text-gray-900">{value}</div>
  </div>
);

export default StatCard;
