import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';
import { useUploadExtraction } from '../api/hooks';
import { useJobs } from '../context/JobContext';

export default function UploadPage() {
  const [caseId, setCaseId] = useState('');
  const [file, setFile] = useState<File | null>(null);
  const [existingCases, setExistingCases] = useState<string[]>([]);

  const navigate = useNavigate();
  const uploadMutation = useUploadExtraction();
  const { addJob } = useJobs(); // Bring in our global context

  useEffect(() => {
    axios.get('/api/v1/cases')
      .then(res => setExistingCases(res.data.cases || []))
      .catch(err => console.error("Failed to load cases", err));
  }, []);

  const handleUpload = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!file || !caseId) return;
    try {
      const response = await uploadMutation.mutateAsync({ file, caseId });
      if (response.status === "CONFLICT") {
        alert(response.message);
        return;
      }
      
      // 1. Add the job to the global tracker
      addJob(response.job_id);
      
      // 2. IMMEDIATELY navigate to the dashboard
      navigate(`/case/${caseId}`);

    } catch (error: any) {
      if (error.response?.status === 409) {
        alert("This exact file has already been uploaded to this Case ID.");
      } else {
        console.error("Upload failed:", error);
      }
    }
  };

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
                className="w-full bg-[#F8FAFC] border border-[#CBD5E1] text-[#334155] rounded p-2 text-sm cursor-pointer outline-none shadow-sm" 
              />
            </div>

            <button 
              type="submit" disabled={uploadMutation.isPending} 
              className="w-full bg-[#C2761B] hover:bg-[#AD7B45] text-[#FFFFFF] font-bold py-3 px-4 rounded shadow-md transition-all disabled:opacity-50 mt-4"
            >
              {uploadMutation.isPending ? 'Sending to Server...' : 'Initialize Pipeline'}
            </button>
          </form>
        </div>
      </main>
    </div>
  );
}