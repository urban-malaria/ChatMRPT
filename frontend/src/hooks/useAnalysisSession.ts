import { useState } from 'react';
import toast from 'react-hot-toast';
import { useChatStore } from '@/stores/chatStore';
import { useAnalysisStore } from '@/stores/analysisStore';
import api from '@/services/api';
import storage from '@/utils/storage';

export const SAMPLE_DATASET = {
  title: 'Kwara State DHIS2 malaria export',
  description: '6,948 monthly health-facility records of malaria testing (RDT and microscopy) across Kwara State wards.',
  nextStep: 'start the TPR workflow',
};

/**
 * Starts a Data Analysis session once a data file is on the server: switches the chat to the new
 * upload session, turns on Data Analysis mode and asks the assistant to describe the data.
 * Used by the upload window and by the sample-data buttons.
 */
export function useAnalysisSession() {
  const [isLoadingSample, setIsLoadingSample] = useState(false);
  const setUploadedFiles = useChatStore((state) => state.setUploadedFiles);
  const addMessage = useChatStore((state) => state.addMessage);
  const updateSession = useChatStore((state) => state.updateSession);
  const setDataAnalysisMode = useAnalysisStore((state) => state.setDataAnalysisMode);

  const startAnalysis = async (backendSessionId: string, filename: string, notice: string) => {
    setUploadedFiles(filename, undefined);
    updateSession({ sessionId: backendSessionId, preserveMessages: true });
    await api.analysis.activateMode(backendSessionId);

    addMessage({
      id: `msg_${Date.now()}`,
      type: 'system' as const,
      content: notice,
      timestamp: new Date(),
      sessionId: backendSessionId,
    });

    // Ask the Data Analysis agent to describe the file, as after a manual upload.
    try {
      const chatResponse = await fetch('/api/v1/data-analysis/chat', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Conversation-ID': storage.ensureConversationId(),
        },
        body: JSON.stringify({ message: 'analyze uploaded data', session_id: backendSessionId }),
      });
      if (!chatResponse.ok) {
        console.error('Failed to trigger data analysis:', chatResponse.status, await chatResponse.text());
        return;
      }
      const responseData = await chatResponse.json();
      if (responseData.success && responseData.message) {
        addMessage({
          id: `msg_${Date.now() + 2}`,
          type: 'regular' as const,
          sender: 'assistant' as const,
          content: responseData.message,
          timestamp: new Date(),
          sessionId: backendSessionId,
          visualizations: responseData.visualizations,
        });
        // Later messages go to the Data Analysis endpoint.
        setDataAnalysisMode(true);
      }
    } catch (error) {
      console.error('Error triggering data analysis:', error);
    }
  };

  const loadSample = async (): Promise<boolean> => {
    if (isLoadingSample) return false;
    setIsLoadingSample(true);
    const toastId = toast.loading('Loading the Kwara sample dataset...');
    try {
      const response = await api.analysis.loadSample();
      if (response.data?.status !== 'success') {
        toast.error(response.data?.message || 'Could not load the sample dataset', { id: toastId });
        return false;
      }
      toast.success('Sample dataset loaded', { id: toastId });
      await startAnalysis(
        response.data.session_id,
        response.data.filename,
        `Sample data loaded: ${SAMPLE_DATASET.title}. To calculate ward malaria burden, type "${SAMPLE_DATASET.nextStep}".`
      );
      return true;
    } catch (error: any) {
      toast.error(error?.response?.data?.message || 'Could not load the sample dataset', { id: toastId });
      return false;
    } finally {
      setIsLoadingSample(false);
    }
  };

  return { startAnalysis, loadSample, isLoadingSample };
}

export default useAnalysisSession;
