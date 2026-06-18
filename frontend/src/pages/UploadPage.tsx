import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';
import { useUploadExtraction, useJobPolling } from '../api/hooks';

export default function UploadPage() {
  const [caseId, setCaseId] = useState('');
  const [file, setFile] = useState<File | null>(null);
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const [existingCases, setExistingCases] = useState<string[]>([]);

  const navigate = useNavigate();
  const uploadMutation = useUploadExtraction();
  const { data: jobStatus } = useJobPolling(activeJobId);

  useEffect(() => {
    if (jobStatus?.status) {
      console.log(` Pipeline State Transition: [${jobStatus.status}]`, jobStatus);
    }
  }, [jobStatus?.status]);

  useEffect(() => {
    axios.get('/api/v1/cases')
      .then(res => setExistingCases(res.data.cases || []))
      .catch(err => console.error("Failed to load cases", err));
  }, []);

  useEffect(() => {
    // FIX: Listen for Celery's native SUCCESS state
    if (jobStatus?.status === 'SUCCESS' || jobStatus?.status === 'COMPLETE') {
      navigate(`/case/${caseId}`);
    }
  }, [jobStatus?.status, navigate, caseId]);

  const handleUpload = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!file || !caseId) return;
    try {
      console.log(` Dispatching File for Case: ${caseId}`);
      const response = await uploadMutation.mutateAsync({ file, caseId });
      if (response.status === "CONFLICT") {
        alert(response.message);
        return;
      }
      setActiveJobId(response.job_id);
    } catch (error: any) {
      if (error.response?.status === 409) {
        alert("This exact file has already been uploaded to this Case ID.");
      } else {
        console.error("Upload failed:", error);
      }
    }
  };

  if (activeJobId) {
    const s = jobStatus?.status;
    return (
      <div className="flex h-screen items-center justify-center bg-[#F8FAFC]">
        <div className="w-96 bg-[#FFFFFF] p-8 rounded shadow-xl border border-[#E2E8F0] text-center">
          <h2 className="text-xl font-bold text-[#1E293B] mb-2 animate-pulse">Processing Extraction</h2>
          <p className="text-xs text-[#64748B] mb-6 font-mono border-b border-[#E2E8F0] pb-4">JOB: {activeJobId}</p>
          
          <div className="space-y-3 text-left text-sm font-medium">
            <div className={`p-2 border-l-4 ${s === 'PENDING' || s === 'QUEUED' ? 'border-[#C2761B] bg-[#FFFBEB] text-[#C2761B]' : 'border-transparent text-[#94A3B8]'}`}>1. Queued in Redis</div>
            <div className={`p-2 border-l-4 ${s === 'PARSING' ? 'border-[#C2761B] bg-[#FFFBEB] text-[#C2761B]' : 'border-transparent text-[#94A3B8]'}`}>2. Parsing UFDR Archive</div>
            <div className={`p-2 border-l-4 ${s === 'SQL_DONE' ? 'border-[#1F6F6E] bg-[#F0FDF4] text-[#1F6F6E]' : 'border-transparent text-[#94A3B8]'}`}>3. Persisting to PostgreSQL</div>
            <div className={`p-2 border-l-4 ${s === 'GRAPH_DONE' || s === 'EMBEDDING' ? 'border-[#1F6F6E] bg-[#F0FDF4] text-[#1F6F6E]' : 'border-transparent text-[#94A3B8]'}`}>4. Building Graph & Vectors</div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-screen bg-[#F8FAFC] font-sans">
      <header className="bg-[#1E293B] shadow-md z-10 shrink-0">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 flex justify-between items-center h-16">
          <h1 className="text-xl font-extrabold text-[#FFFFFF] tracking-tight">
            AI-UFDR<span className="text-[#C2761B]">Analyzer</span>
          </h1>
          <div className="flex items-center space-x-3">
            <span className="text-xs font-bold text-[#94A3B8] uppercase tracking-wider">Active Cases:</span>
            <select 
              className="bg-[#FFFFFF] border border-[#CBD5E1] text-[#1E293B] text-sm rounded py-1 px-2 outline-none focus:ring-2 focus:ring-[#C2761B] cursor-pointer"
              defaultValue=""
              onChange={(e) => { if (e.target.value) navigate(`/case/${e.target.value}`); }}
            >
              <option value="" disabled>Select an existing case...</option>
              {existingCases.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>
        </div>
      </header>

      <main className="flex-1 flex items-center justify-center p-4">
        <div className="w-full max-w-lg bg-[#FFFFFF] p-10 rounded-xl shadow-lg border border-[#E2E8F0]">
          <h2 className="text-2xl font-bold text-[#1E293B] mb-2">Ingest New Extraction</h2>
          <p className="text-sm text-[#64748B] mb-8">Upload a Universal Forensic Device Report (.ufdr) to initialize the graph and vector pipelines.</p>
          
          <form onSubmit={handleUpload} className="space-y-6">
            <div>
              <label className="block text-xs font-bold text-[#334155] uppercase tracking-wider mb-2">Target Case ID</label>
              <input 
                type="text" required value={caseId} onChange={(e) => setCaseId(e.target.value)} 
                className="w-full bg-[#FFFFFF] border border-[#CBD5E1] text-[#1E293B] rounded p-3 text-sm focus:ring-2 focus:ring-[#C2761B] outline-none transition-all shadow-sm" 
                placeholder="e.g. CASE-2026-001" 
              />
            </div>
            
            <div>
              <label className="block text-xs font-bold text-[#334155] uppercase tracking-wider mb-2">Extraction Archive</label>
              <input 
                type="file" required accept=".ufdr" onChange={(e) => setFile(e.target.files?.[0] || null)} 
                className="w-full bg-[#F8FAFC] border border-[#CBD5E1] text-[#334155] rounded p-2 text-sm file:mr-4 file:py-2 file:px-4 file:rounded file:border-0 file:text-sm file:font-bold file:bg-[#E2E8F0] file:text-[#1E293B] hover:file:bg-[#CBD5E1] cursor-pointer outline-none shadow-sm" 
              />
            </div>

            <button 
              type="submit" disabled={uploadMutation.isPending} 
              className="w-full bg-[#C2761B] hover:bg-[#AD7B45] text-[#FFFFFF] font-bold py-3 px-4 rounded shadow-md transition-all disabled:opacity-50 mt-4"
            >
              {uploadMutation.isPending ? 'Dispatching to Worker...' : 'Initialize Pipeline'}
            </button>
          </form>
        </div>
      </main>
    </div>
  );
}