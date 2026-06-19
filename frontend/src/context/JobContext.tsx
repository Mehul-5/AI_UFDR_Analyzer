import React, { createContext, useContext, useState } from 'react';

interface JobContextType {
  activeJobIds: string[];
  addJob: (jobId: string) => void;
  removeJob: (jobId: string) => void;
}

const JobContext = createContext<JobContextType | undefined>(undefined);

export const JobProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [activeJobIds, setActiveJobIds] = useState<string[]>([]);

  const addJob = (jobId: string) => {
    // Prevent duplicate IDs from being added
    setActiveJobIds((prev) => prev.includes(jobId) ? prev : [...prev, jobId]);
  };

  const removeJob = (jobId: string) => {
    setActiveJobIds((prev) => prev.filter((id) => id !== jobId));
  };

  return (
    <JobContext.Provider value={{ activeJobIds, addJob, removeJob }}>
      {children}
    </JobContext.Provider>
  );
};

export const useJobs = () => {
  const context = useContext(JobContext);
  if (!context) throw new Error('useJobs must be used within a JobProvider');
  return context;
};