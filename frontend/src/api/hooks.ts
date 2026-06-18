import { useQuery, useMutation } from '@tanstack/react-query';
import axios from 'axios';

export interface JobStatus {
  job_id: string;
  status: 'QUEUED' | 'PENDING' | 'PARSING' | 'SQL_DONE' | 'GRAPH_DONE' | 'EMBEDDING' | 'COMPLETE' | 'SUCCESS' | 'FAILED';
  phase?: string;
  error_message?: string;
}

export const useJobPolling = (jobId: string | null) => {
  return useQuery<JobStatus, Error>({
    queryKey: ['jobStatus', jobId],
    queryFn: async () => {
      const response = await axios.get(`/api/v1/jobs/${jobId}/status`);
      return response.data;
    },
    enabled: !!jobId,
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      if (status === 'COMPLETE' || status === 'FAILED') return false; 
      return 3000; 
    },
  });
};

export const useUploadExtraction = () => {
  return useMutation({
    mutationFn: async ({ file, caseId }: { file: File; caseId: string }) => {
      const formData = new FormData();
      formData.append('file', file);
      const response = await axios.post(`/api/v1/extract?case_id=${caseId}`, formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
      });
      return response.data;
    },
  });
};