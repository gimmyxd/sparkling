import { Route, Routes } from "react-router";
import "./App.css";
import Dashboard from "./components/Dashboard";
import { Navigate } from "react-router";
import Jobs from "./components/Jobs";
import Nav from "./components/Nav";

function App() {
  return (
    <>
      <Nav />
      <Routes>
        <Route element={<Dashboard />} path="/dashboard" />
        <Route element={<Jobs />} path="/jobs" />
        <Route element={<Navigate replace to="dashboard" />} index />
      </Routes>
    </>
  );
}

export default App;
