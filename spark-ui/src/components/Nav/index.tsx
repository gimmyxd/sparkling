import { Zap } from "lucide-react";
import { NavLink } from "react-router-dom";

const Nav = () => {
  return (
    <nav className="fixed top-0 left-0 w-full z-50 bg-white border-b border-gray-200 px-6 py-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-8">
          <div className="flex items-center gap-3">
            <NavLink to="/dashboard" className="flex items-center gap-3">
              <div className="w-8 h-8 bg-yellow-400 rounded-lg flex items-center justify-center">
                <Zap className="w-5 h-5 text-black" />
              </div>
              <span className="text-xl font-bold text-gray-900">
                Spark Monitor
              </span>
            </NavLink>
          </div>
          <div className="flex items-center gap-6">
            <NavLink
              to="/dashboard"
              className={({ isActive }) =>
                isActive
                  ? "text-gray-900 font-medium border-b-2 border-yellow-400 pb-1"
                  : "text-gray-500 hover:text-gray-700 font-medium pb-1"
              }
            >
              Overview
            </NavLink>
            <NavLink
              to="/jobs"
              className={({ isActive }) =>
                isActive
                  ? "text-gray-900 font-medium border-b-2 border-yellow-400 pb-1"
                  : "text-gray-500 hover:text-gray-700 font-medium pb-1"
              }
            >
              Jobs
            </NavLink>
          </div>
        </div>
      </div>
    </nav>
  );
};

export default Nav;
