import { useState } from 'react';

export function useFileUpload() {
  const [isUploading, setIsUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [uploadStatus, setUploadStatus] = useState('');

  async function executeUpload<T>(
    uploadFn: (
      onProgress: (percent: number) => void,
      onRetry: (attempt: number) => void
    ) => Promise<T>
  ): Promise<T> {
    setIsUploading(true);
    setUploadProgress(0);
    setUploadStatus('Uploading...');
    try {
      return await uploadFn(
        (percent) => {
          setUploadProgress(percent);
          setUploadStatus(percent < 100 ? 'Uploading...' : 'Processing...');
        },
        (attempt) => {
          setUploadStatus(`Connection issue, retrying... (${attempt}/3)`);
        }
      );
    } catch (err: any) {
      if (err.code === 'ECONNABORTED') {
        setUploadStatus('Upload timed out. File may be too large for your connection speed. Please try again.');
      }
      throw err;
    } finally {
      // Only clear the spinner — caller controls progress reset via resetProgress()
      setIsUploading(false);
    }
  }

  function resetProgress() {
    setUploadProgress(0);
    setUploadStatus('');
  }

  return { isUploading, uploadProgress, uploadStatus, executeUpload, resetProgress };
}
