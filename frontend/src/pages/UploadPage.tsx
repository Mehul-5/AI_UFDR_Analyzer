import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import api from '../services/api';
import { useJobs } from '../context/JobContext';

export default function UploadPage() {
  const navigate = useNavigate();
  const { addJob } = useJobs();
  
  const [file, setFile] = useState<File | null>(null);
  const [caseId, setCaseId] = useState('');
  const [isUploading, setIsUploading] = useState(false);
  const [cases, setCases] = useState<string[]>([]);
  const [isLoadingCases, setIsLoadingCases] = useState(false);

  useEffect(() => {
    fetchCases();
  }, []);

  const fetchCases = async () => {
    setIsLoadingCases(true);
    try {
      const response = await api.get('/api/v1/cases');
      setCases(response.data.cases || []);
    } catch (error) {
      console.error("Failed to fetch workspaces:", error);
    } finally {
      setIsLoadingCases(false);
    }
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      setFile(e.target.files[0]);
    }
  };

  const handleUpload = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!file || !caseId.trim()) return;

    setIsUploading(true);
    const formData = new FormData();
    formData.append('file', file);
    const cleanCaseId = caseId.trim();

    try {
      const response = await api.post(`/api/v1/extract?case_id=${encodeURIComponent(cleanCaseId)}`, formData, {
        headers: { 'Content-Type': 'multipart/form-data' }
      });
      addJob(response.data.job_id);
      setFile(null);
      setCaseId('');
      navigate(`/dashboard/${encodeURIComponent(cleanCaseId)}`);
    } catch (error: any) {
      console.error("Ingestion pipeline failed:", error);
      alert(`Pipeline Error: ${error.response?.data?.message || error.message}`);
    } finally {
      setIsUploading(false);
    }
  };

  const handleLogout = () => {
    localStorage.removeItem('auth_token');
    navigate('/login');
  };

  return (
    <div className="min-h-screen bg-slate-50 font-sans flex flex-col">
      <header className="bg-slate-900 shadow-md z-20 shrink-0 relative border-b border-slate-700">
        <div className="px-6 flex justify-between items-center h-16">
          <h1 className="text-xl font-extrabold text-white tracking-tight">
            AI-UFDR<span className="text-amber-500">Analyzer</span>
          </h1>
          <button 
            onClick={handleLogout}
            className="text-sm font-semibold bg-slate-800 hover:bg-red-700 text-white py-2 px-4 rounded transition-colors shadow-sm border border-slate-600 hover:border-red-600"
          >
            End Session
          </button>
        </div>
      </header>

      <div className="flex-1 max-w-7xl w-full mx-auto p-6 grid grid-cols-1 lg:grid-cols-12 gap-8">
        
        <div className="lg:col-span-4 flex flex-col h-[80vh]">
          <div className="bg-white border border-slate-200 rounded-xl shadow-sm flex flex-col h-full overflow-hidden">
            <div className="p-5 bg-slate-900 border-b border-slate-700 shrink-0">
              <h2 className="text-sm font-bold text-slate-100 uppercase tracking-wider flex justify-between items-center">
                Active Workspaces
                {isLoadingCases && <span className="text-xs text-slate-400 animate-pulse">Syncing</span>}
              </h2>
            </div>
            
            <div className="flex-1 overflow-y-auto p-4 bg-slate-50/50">
              {cases.length === 0 && !isLoadingCases ? (
                <div className="p-6 text-sm text-slate-500 text-center border-2 border-dashed border-slate-200 rounded-lg">
                  No cases detected. Ingest an archive to initialize the graph.
                </div>
              ) : (
                <ul className="space-y-3">
                  {cases.map((id) => (
                    <li key={id} className="bg-white border border-slate-200 rounded-lg shadow-sm hover:shadow-md hover:border-amber-400 transition-all group">
                      <div className="p-4 flex flex-col">
                        <span className="font-mono text-sm font-bold text-slate-800 truncate mb-3" title={id}>
                          {id}
                        </span>
                        <button 
                          onClick={() => navigate(`/dashboard/${id}`)}
                          className="w-full bg-slate-100 hover:bg-slate-200 text-slate-700 font-semibold text-xs py-2.5 rounded transition-colors uppercase tracking-wide"
                        >
                          Open Workspace
                        </button>
                      </div>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </div>
        </div>

        <div className="lg:col-span-8">
          <div className="bg-white border border-slate-200 rounded-xl shadow-sm p-8 lg:p-10">
            <div className="mb-8 border-b border-slate-100 pb-6">
              <h2 className="text-2xl font-extrabold text-slate-900 uppercase tracking-wide mb-3">Ingest New Evidence</h2>
              <p className="text-sm text-slate-500 leading-relaxed">
                Initialize the extraction pipeline. The system will parse the UFDR manifest, construct the Neo4j relational topology, and embed semantic artifacts into the vector index.
              </p>
            </div>
            
            <form onSubmit={handleUpload} className="space-y-6">
              <div>
                <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider mb-2">Case Identifier</label>
                <input 
                  type="text" 
                  value={caseId} 
                  onChange={(e) => setCaseId(e.target.value)} 
                  required 
                  disabled={isUploading} 
                  placeholder="e.g., OP-THUNDER-001" 
                  className="w-full bg-slate-50 border border-slate-300 rounded-lg p-3.5 text-sm text-slate-900 font-medium focus:outline-none focus:ring-2 focus:ring-amber-500 focus:border-amber-500 transition-all"
                />
              </div>

              <div>
                <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider mb-2">UFDR Archive File</label>
                <div className="relative border-2 border-dashed border-slate-300 rounded-xl p-12 hover:border-amber-500 hover:bg-amber-50/30 transition-colors bg-slate-50 group">
                  <input 
                    type="file" 
                    accept=".ufdr,.zip" 
                    onChange={handleFileChange} 
                    required 
                    disabled={isUploading} 
                    className="absolute inset-0 w-full h-full opacity-0 cursor-pointer disabled:cursor-not-allowed"
                  />
                  <div className="text-center pointer-events-none">
                    <svg className="mx-auto h-12 w-12 text-slate-400 group-hover:text-amber-500 mb-4 transition-colors" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
                    </svg>
                    <span className="text-base font-bold text-slate-700 block mb-1">
                      {file ? file.name : "Select or Drop Archive Here"}
                    </span>
                    <span className="text-sm text-slate-500 font-medium">
                      {file ? `${(file.size / (1024 * 1024)).toFixed(2)} MB` : "Supports standard .ufdr zip format"}
                    </span>
                  </div>
                </div>
              </div>

              <div className="pt-6">
                <button 
                  type="submit" 
                  disabled={isUploading || !file || !caseId.trim()} 
                  className="w-full bg-amber-500 hover:bg-amber-600 text-white font-bold py-4 rounded-lg transition-colors disabled:opacity-50 disabled:cursor-not-allowed shadow-md text-sm uppercase tracking-widest"
                >
                  {isUploading ? 'Initializing Pipeline...' : 'Start Extraction Pipeline'}
                </button>
              </div>
            </form>
          </div>
        </div>

      </div>
    </div>
  );
}