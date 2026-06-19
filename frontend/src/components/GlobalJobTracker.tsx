import React, { useEffect } from 'react';
import { useJobs } from '../context/JobContext';
import { useJobPolling } from '../api/hooks';

const JobToast: React.FC<{ jobId: string }> = ({ jobId }) => {
  const { removeJob } = useJobs();
  const { data: jobStatus, isError } = useJobPolling(jobId);

  useEffect(() => {
    if (jobStatus?.status === 'SUCCESS' || jobStatus?.status === 'FAILED' || isError) {
      // Delay removal so the user can see the final status before it disappears
      const timer = setTimeout(() => {
        removeJob(jobId);
      }, 5000); 
      return () => clearTimeout(timer);
    }
  }, [jobStatus?.status, isError, jobId, removeJob]);

  const isComplete = jobStatus?.status === 'SUCCESS';
  const isFailed = jobStatus?.status === 'FAILED' || isError;

  return (
    <div className={`bg-white border-l-4 shadow-lg p-4 rounded mb-3 w-80 transition-all ${
      isComplete ? 'border-green-500' : isFailed ? 'border-red-500' : 'border-[#C2761B]'
    }`}>
      <div className="flex justify-between items-center mb-1">
        <span className="font-bold text-[#1E293B] text-sm">
          {isComplete ? 'Ingestion Complete' : isFailed ? 'Ingestion Failed' : 'Processing Extraction'}
        </span>
        <span className="text-xs text-gray-400 font-mono">{jobId.substring(0, 8)}...</span>
      </div>
      <div className="text-xs text-[#64748B] flex items-center gap-2">
        {!isComplete && !isFailed && (
          <span className="flex h-2 w-2 relative">
            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-[#C2761B] opacity-75"></span>
            <span className="relative inline-flex rounded-full h-2 w-2 bg-[#C2761B]"></span>
          </span>
        )}
        <span className={!isComplete && !isFailed ? 'animate-pulse font-medium' : 'font-medium'}>
          {jobStatus?.status || 'INITIALIZING...'}
        </span>
      </div>
    </div>
  );
};

export const GlobalJobTracker: React.FC = () => {
  const { activeJobIds } = useJobs();

  if (activeJobIds.length === 0) return null;

  return (
    <div className="fixed bottom-6 right-6 z-50 flex flex-col items-end">
      {activeJobIds.map((id) => (
        <JobToast key={id} jobId={id} />
      ))}
    </div>
  );
};