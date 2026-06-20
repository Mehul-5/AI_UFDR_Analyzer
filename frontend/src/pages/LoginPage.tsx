import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import api from '../services/api';

export default function LoginPage() {
  const [isLoginMode, setIsLoginMode] = useState(true);
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const navigate = useNavigate();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setSuccess('');
    setIsLoading(true);

    try {
      if (isLoginMode) {
        const formData = new URLSearchParams();
        formData.append('username', username);
        formData.append('password', password);

        const res = await api.post('/api/v1/auth/token', formData, {
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        });

        localStorage.setItem('auth_token', res.data.access_token);
        navigate('/'); 
      } else {
        await api.post('/api/v1/auth/register', {
          username: username,
          password: password
        });
        
        setSuccess('Operator account provisioned. Please initialize session.');
        setIsLoginMode(true); 
        setPassword(''); 
      }
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Authentication protocol failed.');
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="flex flex-col min-h-screen bg-[#F8FAFC] font-sans text-[#334155]">
      
      <header className="bg-[#1E293B] shadow-md z-20 shrink-0">
        <div className="px-6 flex justify-between items-center h-16">
          <h1 className="text-xl font-extrabold text-[#FFFFFF] tracking-tight">
            AI-UFDR<span className="text-[#C2761B]">Analyzer</span>
          </h1>
        </div>
      </header>

      {/* Body / Form Area */}
      <div className="flex-1 flex items-center justify-center p-4">
        <div className="w-full max-w-md bg-[#FFFFFF] border border-[#CBD5E1] rounded-lg shadow-lg p-8 mt-[-10vh]">
          
          <div className="text-center mb-8">
            <h2 className="text-2xl font-extrabold tracking-tight text-[#1E293B] uppercase">
              {isLoginMode ? 'Forensic Engine Access' : 'Register Operator'}
            </h2>
            <p className="text-xs text-[#64748B] mt-1 font-semibold uppercase tracking-wide">
              Authorized Investigator Personnel Only
            </p>
          </div>

          {error && (
            <div className="mb-5 p-3 rounded bg-[#FEF2F2] border border-[#FECACA] text-[#9B2C2C] text-xs font-bold text-center">
              {error}
            </div>
          )}
          {success && (
            <div className="mb-5 p-3 rounded bg-[#F0FDF4] border border-[#10B981] text-[#10B981] text-xs font-bold text-center">
              {success}
            </div>
          )}

          <form onSubmit={handleSubmit} className="space-y-6">
            <div>
              <label className="block text-xs font-bold text-[#64748B] uppercase tracking-wider mb-2">
                Operator ID
              </label>
              <input
                type="text"
                required
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                className="w-full px-4 py-3 bg-[#F8FAFC] border border-[#CBD5E1] rounded text-[#334155] text-sm focus:outline-none focus:ring-2 focus:ring-[#C2761B] transition-shadow"
                placeholder="operator_id"
              />
            </div>

            <div>
              <label className="block text-xs font-bold text-[#64748B] uppercase tracking-wider mb-2">
                Passcode
              </label>
              <input
                type="password"
                required
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className="w-full px-4 py-3 bg-[#F8FAFC] border border-[#CBD5E1] rounded text-[#334155] text-sm focus:outline-none focus:ring-2 focus:ring-[#C2761B] transition-shadow"
                placeholder="••••••••"
              />
            </div>

            <button
              type="submit"
              disabled={isLoading}
              className="w-full py-3 px-4 bg-[#C2761B] hover:bg-[#AD7B45] disabled:opacity-50 text-[#FFFFFF] font-bold text-sm rounded shadow transition-colors uppercase tracking-wider mt-2"
            >
              {isLoading 
                ? 'Processing...' 
                : isLoginMode 
                  ? 'Initialize Session' 
                  : 'Provision Account'
              }
            </button>
          </form>

          <div className="mt-6 text-center">
            <button
              type="button"
              onClick={() => {
                setIsLoginMode(!isLoginMode);
                setError('');
                setSuccess('');
              }}
              className="text-xs text-[#64748B] hover:text-[#C2761B] font-bold transition-colors"
            >
              {isLoginMode 
                ? "Need clearance? Register a new Operator ID." 
                : "Already have clearance? Initialize session here."
              }
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}