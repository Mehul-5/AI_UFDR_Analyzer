import { Routes, Route } from 'react-router-dom';
import LoginPage from './pages/LoginPage';
import DashboardPage from './pages/DashboardPage';
import UploadPage from './pages/UploadPage';
import ProtectedRoute from './components/auth/ProtectedRoute';
import { JobProvider } from './context/JobContext'; 
import { GlobalJobTracker } from './components/GlobalJobTracker';

export default function App() {
  return (
    <JobProvider>
      <GlobalJobTracker />
      
      <Routes>
        {/* Public Routes */}
        <Route path="/login" element={<LoginPage />} />

        <Route element={<ProtectedRoute />}>
          <Route path="/" element={<UploadPage />} /> 
          <Route path="/dashboard/:caseId" element={<DashboardPage />} />
        </Route>
      </Routes>
    </JobProvider>
  );
}