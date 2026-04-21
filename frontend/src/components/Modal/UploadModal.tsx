import React, { useState } from 'react';
import { useChatStore } from '@/stores/chatStore';
import storage from '@/utils/storage';
import { useAnalysisStore } from '@/stores/analysisStore';
import useMessageStreaming from '@/hooks/useMessageStreaming';
import api from '@/services/api';
import { useFileUpload } from '@/hooks/useFileUpload';
import toast from 'react-hot-toast';

const CHUNK_THRESHOLD = 5 * 1024 * 1024;
const CHUNK_SIZE = 1024 * 1024;

async function uploadInChunks(
  csvFile: File,
  shapeFile: File,
  onProgress: (percent: number) => void,
  onRetry: (attempt: number) => void
) {
  // crypto.randomUUID() is safe: production runs on HTTPS, localhost is treated as secure context
  const uploadId = crypto.randomUUID();
  const totalBytes = csvFile.size + shapeFile.size;
  let sentBytes = 0;

  async function sendChunks(file: File, fileType: 'csv' | 'shapefile') {
    const total = Math.ceil(file.size / CHUNK_SIZE);
    for (let i = 0; i < total; i++) {
      const chunk = file.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE);
      let attempt = 0;
      while (true) {
        try {
          await api.upload.uploadChunk(uploadId, fileType, i, total, chunk);
          sentBytes += chunk.size;
          onProgress(Math.round((sentBytes / totalBytes) * 95));
          break;
        } catch (err: any) {
          const isRetryable = !err.response || err.response?.status >= 500;
          if (isRetryable && attempt < 3) {
            attempt++;
            onRetry(attempt);
            await new Promise((r) => setTimeout(r, 1000 * Math.pow(2, attempt - 1)));
          } else {
            throw err;
          }
        }
      }
    }
    return Math.ceil(file.size / CHUNK_SIZE);
  }

  const csvTotalChunks = await sendChunks(csvFile, 'csv');
  const shpTotalChunks = await sendChunks(shapeFile, 'shapefile');

  onProgress(97);
  const response = await api.upload.finalizeChunkedUpload({
    upload_id: uploadId,
    csv_filename: csvFile.name,
    shapefile_filename: shapeFile.name,
    csv_total_chunks: csvTotalChunks,
    shp_total_chunks: shpTotalChunks,
  });
  onProgress(100);
  return response;
}

interface UploadModalProps {
  isOpen: boolean;
  onClose: () => void;
}

const UploadModal: React.FC<UploadModalProps> = ({ isOpen, onClose }) => {
  const [activeTab, setActiveTab] = useState<'standard' | 'analysis'>('standard');
  const [analysisType, setAnalysisType] = useState('');
  const [selectedOption, setSelectedOption] = useState('');
  const session = useChatStore((state) => state.session);
  const setUploadedFiles = useChatStore((state) => state.setUploadedFiles);
  const addMessage = useChatStore((state) => state.addMessage);
  const updateSession = useChatStore((state) => state.updateSession);
  const { setCsvFile, setShapeFile, setDataAnalysisMode } = useAnalysisStore();
  const { sendMessage } = useMessageStreaming();
  const { isUploading, uploadProgress, uploadStatus, executeUpload, resetProgress } = useFileUpload();

  const processingLabel = activeTab === 'analysis' ? 'Analysing your data...' : 'Processing files...';
  const isRetrying = uploadStatus.includes('retrying');

  const progressBar = isUploading && (
    <div className="w-full mt-3">
      {uploadProgress < 100 ? (
        <>
          <div className="w-full bg-gray-200 rounded-full h-2.5">
            <div
              className="bg-blue-600 h-2.5 rounded-full transition-all duration-300"
              style={{ width: `${Math.max(5, uploadProgress)}%` }}
            />
          </div>
          <div className="text-xs text-left mt-1">
            {isRetrying
              ? <span className="text-amber-600">{uploadStatus}</span>
              : <span className="text-gray-500">Uploading... {uploadProgress}%</span>}
          </div>
        </>
      ) : (
        <>
          <div className="w-full bg-gray-200 rounded-full h-2.5 overflow-hidden">
            <div className="h-2.5 w-1/3 bg-blue-500 rounded-full animate-shimmer" />
          </div>
          <div className="text-xs text-gray-500 text-left mt-1">{processingLabel}</div>
        </>
      )}
    </div>
  );

  if (!isOpen) return null;

  return (
    <>
      {/* Background overlay */}
      <div
        className="fixed inset-0 bg-gray-500 bg-opacity-75 z-[9998]"
        onClick={onClose}
      />

      {/* Modal container */}
      <div className="fixed inset-0 z-[9999] overflow-y-auto">
        <div className="flex min-h-full items-center justify-center p-4">
          {/* Modal panel */}
          <div className="relative bg-white dark:bg-dark-bg-secondary rounded-lg shadow-xl w-full max-w-2xl">
            <div className="bg-white dark:bg-dark-bg-secondary px-4 pt-5 pb-4 sm:p-6 sm:pb-4">
            {/* Modal Header */}
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-dark-text">Upload Files</h3>
              <button
                onClick={onClose}
                className="text-gray-400 dark:text-dark-text-secondary hover:text-gray-500 dark:hover:text-dark-text focus:outline-none"
              >
                <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>

            {/* Tabs */}
            <div className="border-b border-gray-200 dark:border-dark-border mb-4">
              <nav className="-mb-px flex space-x-8">
                <button
                  onClick={() => setActiveTab('standard')}
                  disabled={isUploading}
                  className={`py-2 px-1 border-b-2 font-medium text-sm disabled:opacity-50 disabled:cursor-not-allowed ${
                    activeTab === 'standard'
                      ? 'border-blue-500 text-blue-600 dark:text-blue-400'
                      : 'border-transparent text-gray-500 dark:text-dark-text-secondary hover:text-gray-700 dark:hover:text-dark-text hover:border-gray-300 dark:hover:border-dark-border'
                  }`}
                >
                  I Have Complete Data
                </button>
                <button
                  onClick={() => setActiveTab('analysis')}
                  disabled={isUploading}
                  className={`py-2 px-1 border-b-2 font-medium text-sm disabled:opacity-50 disabled:cursor-not-allowed ${
                    activeTab === 'analysis'
                      ? 'border-blue-500 text-blue-600 dark:text-blue-400'
                      : 'border-transparent text-gray-500 dark:text-dark-text-secondary hover:text-gray-700 dark:hover:text-dark-text hover:border-gray-300 dark:hover:border-dark-border'
                  }`}
                >
                  I Have TPR Data Only
                </button>
              </nav>
            </div>

            {/* Tab Content */}
            <div className="mt-4">
              {/* Standard Upload Tab */}
              {activeTab === 'standard' && (
                <div>
                  <p className="text-sm text-gray-600 dark:text-dark-text-secondary mb-4">
                    CSV with environmental variables + Shapefile
                  </p>

                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    {/* CSV/Excel Upload */}
                    <div className="border border-gray-200 dark:border-dark-border rounded-lg p-4">
                      <div className="text-center">
                        <svg className="mx-auto h-10 w-10 text-green-500 mb-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                        </svg>
                        <h5 className="font-medium text-gray-900 dark:text-dark-text mb-1">CSV / Excel Data</h5>
                        <p className="text-xs text-gray-500 dark:text-dark-text-secondary mb-3">Ward-level data (e.g., environmental factors, demographics)</p>
                        <input
                          type="file"
                          className="w-full text-sm text-gray-500 dark:text-dark-text-secondary file:mr-4 file:py-2 file:px-4 file:rounded-full file:border-0 file:text-sm file:font-semibold file:bg-green-50 dark:file:bg-green-900/30 file:text-green-700 dark:file:text-green-400 hover:file:bg-green-100 dark:hover:file:bg-green-900/50"
                          id="csv-upload"
                          accept=".csv,.xlsx,.xls"
                          onChange={(e) => {
                            const file = e.target.files?.[0];
                            if (file) {
                              setCsvFile(file);
                            }
                          }}
                        />
                      </div>
                    </div>

                    {/* Shapefile Upload */}
                    <div className="border border-gray-200 dark:border-dark-border rounded-lg p-4">
                      <div className="text-center">
                        <svg className="mx-auto h-10 w-10 text-blue-500 mb-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 20l-5.447-2.724A1 1 0 013 16.382V5.618a1 1 0 011.447-.894L9 7m0 13l6-3m-6 3V7m6 10l4.553 2.276A1 1 0 0021 18.382V7.618a1 1 0 00-.553-.894L15 4m0 13V4m0 0L9 7" />
                        </svg>
                        <h5 className="font-medium text-gray-900 dark:text-dark-text mb-1">Shapefile (ZIP)</h5>
                        <p className="text-xs text-gray-500 dark:text-dark-text-secondary mb-3">Geographical boundaries of wards (must be a .zip archive)</p>
                        <input
                          type="file"
                          className="w-full text-sm text-gray-500 dark:text-dark-text-secondary file:mr-4 file:py-2 file:px-4 file:rounded-full file:border-0 file:text-sm file:font-semibold file:bg-blue-50 dark:file:bg-blue-900/30 file:text-blue-700 dark:file:text-blue-400 hover:file:bg-blue-100 dark:hover:file:bg-blue-900/50"
                          id="shapefile-upload"
                          accept=".zip"
                          onChange={(e) => {
                            const file = e.target.files?.[0];
                            if (file) {
                              setShapeFile(file);
                            }
                          }}
                        />
                      </div>
                    </div>
                  </div>

                  {/* Upload Button */}
                  <div className="mt-4 text-center">
                    <button
                      onClick={async () => {
                        const csvInput = document.getElementById('csv-upload') as HTMLInputElement;
                        const shapeInput = document.getElementById('shapefile-upload') as HTMLInputElement;
                        const csvFile = csvInput?.files?.[0];
                        const shapeFile = shapeInput?.files?.[0];

                        if (!csvFile || !shapeFile) {
                          toast.error('Please select both CSV/Excel and Shapefile');
                          return;
                        }

                        const formData = new FormData();
                        formData.append('csv_file', csvFile);
                        formData.append('shapefile', shapeFile);
                        // DON'T send session_id - backend will generate a fresh one
                        // formData.append('session_id', session.sessionId); // REMOVED to fix concurrent user data bleed

                        try {
                          const useChunked = (csvFile.size + shapeFile.size) > CHUNK_THRESHOLD;
                          console.log(`Starting ${useChunked ? 'chunked' : 'standard'} upload`);

                          const response = await executeUpload((onProgress, onRetry) =>
                            useChunked
                              ? uploadInChunks(csvFile, shapeFile, onProgress, onRetry)
                              : api.upload.uploadBothFiles(formData, onProgress, onRetry)
                          );
                          console.log('Upload response:', response);

                          if (response.data.status === 'success') {
                            toast.success('Files uploaded successfully!');

                            // Use the backend's session ID from the response if provided
                            const backendSessionId = response.data.session_id || session.sessionId;
                            if (response.data.session_id) {
                              console.log('Using backend session ID for standard upload:', backendSessionId);
                              updateSession({ sessionId: backendSessionId, preserveMessages: true });
                            }

                            setUploadedFiles(csvFile.name, shapeFile.name);

                            // Add system message
                            const systemMessage = {
                              id: `msg_${Date.now()}`,
                              type: 'system' as const,
                              content: `Files uploaded successfully: ${csvFile.name} and ${shapeFile.name}`,
                              timestamp: new Date(),
                              sessionId: backendSessionId
                            };
                            addMessage(systemMessage);

                            onClose();
                            resetProgress();

                            // Send the special trigger message silently (not shown to user)
                            setTimeout(() => {
                              sendMessage('__DATA_UPLOADED__', { silent: true });
                            }, 500);
                          } else {
                            console.error('Upload response not successful:', response.data);
                            toast.error(response.data.message || 'Upload did not return success status');
                            resetProgress();
                          }
                        } catch (error) {
                          console.error('Upload error:', error);
                          const err: any = error as any;
                          const message = err?.response?.data?.message || err?.message || 'Failed to upload files';
                          toast.error(message);
                          resetProgress();
                        }
                      }}
                      disabled={isUploading}
                      className="w-full px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-blue-400 disabled:cursor-not-allowed transition-colors flex items-center justify-center"
                    >
                      <>
                        <svg className="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
                        </svg>
                        Upload Files
                      </>
                    </button>

                    {progressBar}
                    <div id="files-upload-status" className="mt-2"></div>
                  </div>
                </div>
              )}

              {/* Data Analysis Tab */}
              {activeTab === 'analysis' && (
                <div>
                  <div className="mb-4">
                    <h4 className="text-lg font-bold text-gray-900 dark:text-dark-text">Upload Facility Testing Data</h4>
                    <p className="text-sm text-gray-600 dark:text-dark-text-secondary mt-1">
                      Facility testing data only - We'll guide you through TPR calculation and add environmental data
                    </p>
                  </div>

                  <div className="max-w-md mx-auto">
                    <div className="text-center">
                      <div className="mb-4">
                        <svg className="mx-auto h-16 w-16 text-blue-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                        </svg>
                      </div>

                      <h5 className="text-base font-medium text-gray-900 dark:text-dark-text mb-2">Select Data File</h5>
                      <p className="text-sm text-gray-500 dark:text-dark-text-secondary mb-4">CSV, Excel, or JSON format</p>

                      <div className="mb-4">
                        <input
                          type="file"
                          className="w-full px-3 py-2 border border-gray-300 dark:border-dark-border bg-white dark:bg-dark-bg-tertiary text-gray-900 dark:text-dark-text rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                          id="data-analysis-file"
                          accept=".csv,.xlsx,.xls,.json"
                          onChange={(e) => {
                            const file = e.target.files?.[0];
                            if (file) {
                              setAnalysisType(file.name);
                            }
                          }}
                        />
                      </div>

                      <button
                        onClick={async () => {
                          const fileInput = document.getElementById('data-analysis-file') as HTMLInputElement;
                          const file = fileInput?.files?.[0];

                          if (!file) {
                            toast.error('Please select a file first');
                            return;
                          }

                          const formData = new FormData();
                          formData.append('file', file);
                          // DON'T send session_id - backend will generate a fresh one
                          // formData.append('session_id', session?.sessionId || ''); // REMOVED to fix concurrent user data bleed

                          try {
                            console.log('Starting data analysis upload with formData:', formData);
                            const response = await executeUpload((onProgress, onRetry) =>
                              api.analysis.uploadFiles(formData, onProgress, onRetry)
                            );
                            console.log('Data analysis upload response:', response);
                            console.log('Response data:', response.data);
                            console.log('Response data status:', response.data?.status);

                            if (response.data.status === 'success') {
                              toast.success('File uploaded successfully for analysis!');

                              // Use the backend's session ID from the response
                              const backendSessionId = response.data.session_id;
                              console.log('Using backend session ID:', backendSessionId);

                              // Store file info for the chat to know we're in data analysis mode
                              setUploadedFiles(file.name, undefined);

                              // Update session with backend's ID
                              updateSession({ sessionId: backendSessionId, preserveMessages: true });

                              // Activate data analysis mode with backend session
                              await api.analysis.activateMode(backendSessionId);

                              // Add system message for user feedback
                              const systemMessage = {
                                id: `msg_${Date.now()}`,
                                type: 'system' as const,
                                content: `Data file uploaded: ${file.name}. Results will appear below.`,
                                timestamp: new Date(),
                                sessionId: backendSessionId
                              };
                              addMessage(systemMessage);

                              onClose();
                              resetProgress();

                              // Automatically trigger Data Analysis V3 agent after a short delay
                              setTimeout(async () => {
                                console.log('Triggering data analysis chat API with session:', backendSessionId);
                                try {
                                  const chatResponse = await fetch('/api/v1/data-analysis/chat', {
                                    method: 'POST',
                                    headers: {
                                      'Content-Type': 'application/json',
                                      'X-Conversation-ID': storage.ensureConversationId(),
                                    },
                                    body: JSON.stringify({
                                      message: 'analyze uploaded data',
                                      session_id: backendSessionId
                                    })
                                  });

                                  console.log('Data analysis chat response:', chatResponse.status);

                                  if (!chatResponse.ok) {
                                    console.error('Failed to trigger data analysis:', chatResponse.status);
                                    const errorText = await chatResponse.text();
                                    console.error('Error response:', errorText);
                                  } else {
                                    const responseData = await chatResponse.json();
                                    console.log('Data analysis triggered successfully:', responseData);

                                    // Add the analysis results to the chat
                                    if (responseData.success && responseData.message) {
                                      const analysisMessage = {
                                        id: `msg_${Date.now() + 2}`,
                                        type: 'regular' as const,
                                        sender: 'assistant' as const,
                                        content: responseData.message,
                                        timestamp: new Date(),
                                        sessionId: backendSessionId,
                                        visualizations: responseData.visualizations
                                      };
                                      addMessage(analysisMessage);

                                      // Set data analysis mode so subsequent messages go to the right endpoint
                                      setDataAnalysisMode(true);
                                      console.log('Data analysis mode activated');
                                    }
                                  }
                                } catch (error) {
                                  console.error('Error triggering data analysis:', error);
                                }
                              }, 500);


                            } else {
                              console.error("Data analysis upload not successful. Status:", response.data?.status);
                              console.error("Full response data:", response.data);
                              toast.error(response.data.message || "Upload failed");
                              resetProgress();
                            }
                          } catch (error: any) {
                            console.error('Upload error:', error);
                            toast.error(error.response?.data?.message || 'Failed to upload file');
                            resetProgress();
                          }
                        }}
                        disabled={isUploading}
                        className="w-full px-4 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 transition-colors flex items-center justify-center disabled:opacity-50 disabled:cursor-not-allowed"
                      >
                        <>
                          <svg className="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
                          </svg>
                          Upload for Analysis
                        </>
                      </button>

                      {progressBar}

                      <div id="data-analysis-upload-status" className="mt-3"></div>
                    </div>
                  </div>
                </div>
              )}

            </div>
          </div>
        </div>
      </div>
    </div>
    </>
  );
};

export default UploadModal;
