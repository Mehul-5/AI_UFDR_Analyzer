import { Routes, Route } from 'react-router-dom';
import UploadPage from './pages/UploadPage';
import DashboardPage from './pages/DashboardPage';
import { JobProvider } from './context/JobContext';
import { GlobalJobTracker } from './components/GlobalJobTracker';

export default function App() {
  return (
    <JobProvider>
      <Routes>
        <Route path="/" element={<UploadPage />} />
        <Route path="/dashboard/:caseId" element={<DashboardPage />} />
      </Routes>
      
      {/* This renders the Toasts globally, regardless of which Route is active */}
      <GlobalJobTracker />
    </JobProvider>
  );
}