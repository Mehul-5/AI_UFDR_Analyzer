import React, { useState, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';
import { useJobs } from '../context/JobContext';

export default function UploadPage() {
  const navigate = useNavigate();
  const { addJob } = useJobs();
  
  // Upload State
  const [file, setFile] = useState<File | null>(null);
  const [caseId, setCaseId] = useState('');
  const [isUploading, setIsUploading] = useState(false);

  // Case List State (for Dropdown)
  const [cases, setCases] = useState<string[]>([]);
  const [isLoadingCases, setIsLoadingCases] = useState(false);
  const [isDropdownOpen, setIsDropdownOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);

  // Fetch cases on component mount
  useEffect(() => {
    fetchCases();
  }, []);

  // Handle clicking outside the dropdown to close it
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsDropdownOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const fetchCases = async () => {
    setIsLoadingCases(true);
    try {
      const response = await axios.get('/api/v1/cases');
      setCases(response.data.cases || []);
    } catch (error) {
      console.error("Failed to fetch cases:", error);
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
      const response = await axios.post(`/api/v1/extract?case_id=${encodeURIComponent(cleanCaseId)}`, formData, {
        headers: { 'Content-Type': 'multipart/form-data' }
      });
      
      const { job_id } = response.data;
      
      // Register the job so the SSE Toast tracker picks it up globally
      addJob(job_id);
      
      // Reset form state
      setFile(null);
      setCaseId('');
      
      navigate(`/dashboard/${encodeURIComponent(cleanCaseId)}`);
      
    } catch (error: any) {
      console.error("Upload failed:", error);
      alert(`Upload Failed: ${error.response?.data?.message || error.response?.data?.detail || error.message}`);
    } finally {
      setIsUploading(false);
    }
  };

  const handleDeleteCase = async (caseIdToDelete: string, e: React.MouseEvent) => {
    e.stopPropagation(); // Prevent navigating to the dashboard when clicking delete
    const isConfirmed = window.confirm(`⚠️ WARNING: Are you sure you want to permanently delete Case "${caseIdToDelete}"?\n\nThis will purge all data from PostgreSQL, Neo4j, Qdrant, and MinIO. This action cannot be undone.`);
    
    if (!isConfirmed) return;

    try {
      await axios.delete(`/api/v1/cases/${encodeURIComponent(caseIdToDelete)}`);
      setCases((prev) => prev.filter(c => c !== caseIdToDelete));
      
      // Close dropdown if it's the last item being deleted
      if (cases.length <= 1) {
         setIsDropdownOpen(false);
      }
      
      alert(`Case ${caseIdToDelete} successfully purged.`);
    } catch (error: any) {
      console.error("Failed to delete case", error);
      alert(`Failed to delete case: ${error.response?.data?.detail || error.message}`);
    }
  };

  return (
    <div className="min-h-screen bg-[#F8FAFC] font-sans flex flex-col">
      {/* HEADER WITH DROPDOWN */}
      <header className="bg-[#1E293B] shadow-md z-20 shrink-0 relative">
        <div className="px-6 flex justify-between items-center h-16">
          <h1 className="text-xl font-extrabold text-[#FFFFFF] tracking-tight">
            AI-UFDR<span className="text-[#C2761B]">Analyzer</span>
          </h1>

          {/* Cases Dropdown Menu */}
          <div className="relative" ref={dropdownRef}>
            <button 
              onClick={() => {
                if (!isDropdownOpen) fetchCases(); // Optionally refresh when opening
                setIsDropdownOpen(!isDropdownOpen);
              }}
              className="flex items-center gap-2 text-sm font-bold bg-[#334155] hover:bg-[#475569] text-[#FFFFFF] py-2 px-4 rounded transition-colors"
            >
              Active Cases
              <svg className={`w-4 h-4 transition-transform ${isDropdownOpen ? 'rotate-180' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" /></svg>
            </button>

            {isDropdownOpen && (
              <div className="absolute right-0 mt-2 w-72 bg-white rounded-md shadow-xl border border-[#CBD5E1] z-50 overflow-hidden">
                <div className="bg-[#F8FAFC] px-4 py-2 border-b border-[#E2E8F0] flex justify-between items-center">
                  <span className="text-xs font-bold text-[#64748B] uppercase tracking-wider">Select Case to Analyze</span>
                  {isLoadingCases && <span className="text-xs text-[#3B82F6] animate-pulse">Loading...</span>}
                </div>
                
                <div className="max-h-96 overflow-y-auto">
                  {cases.length === 0 && !isLoadingCases ? (
                     <div className="p-4 text-sm text-[#94A3B8] italic text-center">No cases found.</div>
                  ) : (
                    <ul>
                      {cases.map((id) => (
                        <li 
                          key={id} 
                          className="border-b border-[#E2E8F0] last:border-0 hover:bg-[#F1F5F9] transition-colors group cursor-pointer"
                          onClick={() => navigate(`/dashboard/${id}`)}
                        >
                          <div className="px-4 py-3 flex justify-between items-center">
                             <span className="font-mono text-sm font-bold text-[#1E293B] truncate mr-2" title={id}>{id}</span>
                             <button 
                                onClick={(e) => handleDeleteCase(id, e)}
                                className="opacity-0 group-hover:opacity-100 text-[#DC2626] hover:text-[#991B1B] transition-opacity p-1"
                                title="Delete Case"
                             >
                               <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg>
                             </button>
                          </div>
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              </div>
            )}
          </div>
        </div>
      </header>

      {/* BODY - Centered Upload Form */}
      <div className="flex-1 flex items-center justify-center p-6">
        
        <div className="bg-[#FFFFFF] border border-[#CBD5E1] rounded-lg shadow-lg p-8 w-full max-w-md">
          <div className="text-center mb-6">
            <h2 className="text-2xl font-extrabold text-[#1E293B] uppercase tracking-wider mb-2">
              Ingest Evidence
            </h2>
            <p className="text-sm text-[#64748B]">Upload a UFDR archive to begin processing.</p>
          </div>
          
          <form onSubmit={handleUpload} className="space-y-6">
            <div>
              <label className="block text-sm font-bold text-[#64748B] uppercase mb-1">Case Identifier</label>
              <input 
                type="text" 
                value={caseId} 
                onChange={(e) => setCaseId(e.target.value)} 
                required
                disabled={isUploading}
                placeholder="e.g., CASE-2026-001"
                className="w-full bg-[#F8FAFC] border border-[#CBD5E1] rounded p-3 text-sm text-[#1E293B] focus:outline-none focus:ring-2 focus:ring-[#C2761B] transition-shadow"
              />
            </div>

            <div>
              <label className="block text-sm font-bold text-[#64748B] uppercase mb-1">UFDR Archive File</label>
              <div className="relative border-2 border-dashed border-[#CBD5E1] rounded-lg p-6 hover:border-[#94A3B8] transition-colors bg-[#F8FAFC]">
                <input 
                  type="file" 
                  accept=".ufdr" 
                  onChange={handleFileChange} 
                  required
                  disabled={isUploading}
                  className="absolute inset-0 w-full h-full opacity-0 cursor-pointer disabled:cursor-not-allowed"
                />
                <div className="text-center pointer-events-none">
                  <svg className="mx-auto h-12 w-12 text-[#94A3B8] mb-3" stroke="currentColor" fill="none" viewBox="0 0 48 48" aria-hidden="true">
                    <path d="M28 8H12a4 4 0 00-4 4v20m32-12v8m0 0v8a4 4 0 01-4 4H12a4 4 0 01-4-4v-4m32-4l-3.172-3.172a4 4 0 00-5.656 0L28 28M8 32l9.172-9.172a4 4 0 015.656 0L28 28m0 0l4 4m4-24h8m-4-4v8m-12 4h.02" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                  <span className="text-sm font-medium text-[#1E293B]">
                    {file ? file.name : (
                      <>
                        <span className="text-[#C2761B] hover:underline cursor-pointer">Upload a file</span> or drag and drop
                      </>
                    )}
                  </span>
                </div>
              </div>
            </div>

            <button 
              type="submit" 
              disabled={isUploading || !file || !caseId.trim()}
              className="w-full bg-[#C2761B] hover:bg-[#AD7B45] text-[#FFFFFF] font-bold py-3 rounded transition-colors disabled:opacity-50 disabled:cursor-not-allowed shadow-md text-base"
            >
              {isUploading ? 'Initializing Pipeline...' : 'Start Ingestion'}
            </button>
          </form>
        </div>

      </div>
    </div>
  );
}