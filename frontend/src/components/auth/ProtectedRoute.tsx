import { Navigate, Outlet } from 'react-router-dom';

export default function ProtectedRoute() {
  const token = localStorage.getItem('auth_token');

  // If no token exists, bounce them straight back to the security gate
  if (!token) {
    return <Navigate to="/login" replace />;
  }

  // Otherwise, render the secure components safely
  return <Outlet />;
}