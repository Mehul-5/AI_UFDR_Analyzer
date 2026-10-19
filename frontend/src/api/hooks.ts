import { useQuery, useMutation } from '@tanstack/react-query';
import api from '../services/api';

export interface JobStatus {
  job_id: string;
  status: 'QUEUED' | 'PENDING' | 'PARSING' | 'SQL_DONE' | 'GRAPH_DONE' | 'EMBEDDING' | 'COMPLETE' | 'SUCCESS' | 'FAILED';
  phase?: string;
  error_message?: string;
}

// NOTE: the backend currently exposes job progress via SSE (/api/v1/jobs/{id}/stream),
// not /status. If nothing imports this hook, it can be deleted.
export const useJobPolling = (jobId: string | null) => {
  return useQuery<JobStatus, Error>({
    queryKey: ['jobStatus', jobId],
    queryFn: async () => {
      const response = await api.get(`/api/v1/jobs/${jobId}/status`);
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
      // Goes through the shared api instance: correct base URL and Bearer token.
      // Content-Type is left unset so the browser adds the multipart boundary itself.
      const response = await api.post('/api/v1/extract', formData, {
        params: { case_id: caseId },
      });
      return response.data;
    },
  });
};