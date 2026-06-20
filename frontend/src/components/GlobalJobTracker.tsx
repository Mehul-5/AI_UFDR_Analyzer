import React, { useEffect, useState } from 'react';
import { useJobs } from '../context/JobContext';

const JobToast: React.FC<{ jobId: string }> = ({ jobId }) => {
  const { removeJob } = useJobs();
  const [jobStatus, setJobStatus] = useState<any>({ status: 'CONNECTING...' });

  useEffect(() => {
    // Open a persistent Server-Sent Events connection
    const sse = new EventSource(`http://${window.location.hostname}:8000/api/v1/jobs/${jobId}/stream`);
    
    sse.onmessage = (event) => {
      const data = JSON.parse(event.data);
      
      console.log(
        `%c[CELERY WORKER - Job ${jobId}]`, 'color: #C2761B; font-weight: bold;', 
        `\nPhase: ${data.phase || 'INIT'}\nStatus: ${data.status}\nMessage: ${data.error_message || 'Processing...'}`
      );
      
      setJobStatus(data);
      
      if (data.status === 'SUCCESS' || data.status === 'FAILED') {
        sse.close();
        setTimeout(() => removeJob(jobId), 5000); 
      }
    };

    sse.onerror = () => {
      setJobStatus({ status: 'CONNECTION_LOST', error_message: 'Lost connection to worker stream.' });
      sse.close();
    };

    return () => sse.close(); // Cleanup on unmount
  }, [jobId, removeJob]);

  const isComplete = jobStatus?.status === 'SUCCESS';
  // FIX: We no longer have `isError` from react-query. We evaluate the status string directly.
  const isFailed = jobStatus?.status === 'FAILED' || jobStatus?.status === 'CONNECTION_LOST';

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