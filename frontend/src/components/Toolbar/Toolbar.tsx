import React, { useState, useRef, useEffect } from 'react';
import { useChatStore } from '@/stores/chatStore';
import { useTheme } from '@/contexts/ThemeContext';
import api from '@/services/api';
import toast from 'react-hot-toast';
import storage from '@/utils/storage';

interface DownloadFile {
  name: string;
  description: string;
  url: string;
  filename: string;
  category: string;
}

const Toolbar: React.FC = () => {
  const [showClearConfirm, setShowClearConfirm] = useState(false);
  const [isClearing, setIsClearing] = useState(false);
  const [isDownloadOpen, setIsDownloadOpen] = useState(false);
  const [availableDownloads, setAvailableDownloads] = useState<DownloadFile[]>([]);
  const [isLoadingDownloads, setIsLoadingDownloads] = useState(false);
  const downloadRef = useRef<HTMLDivElement>(null);
  const clearMessages = useChatStore((state) => state.clearMessages);
  const resetSession = useChatStore((state) => state.resetSession);
  const updateSession = useChatStore((state) => state.updateSession);
  const hasMessages = useChatStore((state) => state.messages.length > 0);
  const session = useChatStore((state) => state.session);
  const { theme, toggleTheme } = useTheme();

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (downloadRef.current && !downloadRef.current.contains(event.target as Node)) {
        setIsDownloadOpen(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // Fetch downloads when dropdown opens
  useEffect(() => {
    if (isDownloadOpen && session.sessionId) {
      setIsLoadingDownloads(true);
      fetch(`/export/list/${session.sessionId}`, {
        headers: {
          'X-Conversation-ID': storage.ensureConversationId(),
        },
      })
        .then(res => res.json())
        .then(data => {
          if (data.success) {
            setAvailableDownloads(data.files || []);
          }
          setIsLoadingDownloads(false);
        })
        .catch(err => {
          console.error('Error fetching downloads:', err);
          setIsLoadingDownloads(false);
        });
    }
  }, [isDownloadOpen, session.sessionId]);

  const categoryLabels: Record<string, string> = {
    tpr: 'TPR Analysis',
    itn: 'ITN Distribution',
    analysis: 'Risk Analysis'
  };

  const handleClearChat = () => {
    setShowClearConfirm(true);
  };

  const confirmClear = async () => {
    setIsClearing(true);

    try {
      // First, clear backend session data
      const response = await api.session.clearSession();

      if (response.data.status === 'success') {
        // Clear frontend state after successful backend clear
        clearMessages();

        // Get the new session ID from backend
        const newSessionId = response.data.new_session_id;

        if (newSessionId) {
          // Don't call resetSession, instead manually reset with the backend's ID
          updateSession({
            sessionId: newSessionId,
            startTime: new Date(),
            messageCount: 0,
            hasUploadedFiles: false,
            uploadedFiles: undefined
          });
        } else {
          // Fallback if backend doesn't provide new ID
          resetSession();
        }

        setShowClearConfirm(false);
        toast.success('New chat started');
      } else {
        throw new Error(response.data.message || 'Failed to clear session');
      }
    } catch (error) {
      console.error('Error clearing session:', error);
      toast.error('Failed to clear session. Please try again.');

      // Even if backend fails, we can still clear frontend for better UX
      // But notify user that backend might have issues
      if (window.confirm('Backend clear failed. Clear frontend data anyway?')) {
        clearMessages();
        resetSession();
        setShowClearConfirm(false);
        toast('Frontend cleared, but server data may persist', {
          icon: '⚠️',
          style: { background: '#FEF3C7', color: '#92400E' }
        });
      }
    } finally {
      setIsClearing(false);
    }
  };

  const cancelClear = () => {
    if (!isClearing) {
      setShowClearConfirm(false);
    }
  };
  
  return (
    <>
      <div className="sticky top-0 z-50 flex items-center justify-between px-4 py-3 bg-white dark:bg-dark-bg-secondary border-b border-gray-200 dark:border-dark-border">
        <div className="flex items-center space-x-2">
          <h1 className="text-lg font-semibold text-gray-900 dark:text-dark-text">ChatMRPT</h1>
          <span className="px-2 py-1 text-xs font-medium text-blue-700 dark:text-blue-300 bg-blue-100 dark:bg-blue-900/30 rounded-full">
            Malaria Risk Analysis
          </span>
        </div>
        
        <div className="flex items-center space-x-2">
          {/* New Chat Button */}
          <button
            onClick={handleClearChat}
            className="flex items-center px-3 py-2 text-sm font-medium text-gray-700 dark:text-dark-text bg-white dark:bg-dark-bg-tertiary border border-gray-300 dark:border-dark-border rounded-lg hover:bg-gray-50 dark:hover:bg-dark-border focus:outline-none focus:ring-2 focus:ring-blue-500"
            title="Start new chat"
          >
            <svg className="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
            </svg>
            New Chat
          </button>
          
          {/* Download Dropdown */}
          <div className="relative" ref={downloadRef}>
            <button
              onClick={() => setIsDownloadOpen(!isDownloadOpen)}
              className="flex items-center px-3 py-2 text-sm font-medium text-gray-700 dark:text-dark-text bg-white dark:bg-dark-bg-tertiary border border-gray-300 dark:border-dark-border rounded-lg hover:bg-gray-50 dark:hover:bg-dark-border focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <svg className="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4 4m0 0l-4-4m4 4V4" />
              </svg>
              Download
              <svg className={`w-4 h-4 ml-2 transition-transform ${isDownloadOpen ? 'rotate-180' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
              </svg>
            </button>

            {isDownloadOpen && (
              <div className="absolute right-0 z-10 w-72 mt-2 origin-top-right bg-white dark:bg-dark-bg-secondary rounded-lg shadow-lg ring-1 ring-black ring-opacity-5 dark:ring-dark-border">
                <div className="py-2 px-3">
                  <p className="text-xs font-semibold text-gray-500 dark:text-dark-text-secondary uppercase tracking-wider mb-2">
                    Available Files
                  </p>

                  {isLoadingDownloads ? (
                    <div className="flex justify-center py-4">
                      <svg className="animate-spin h-6 w-6 text-blue-600" fill="none" viewBox="0 0 24 24">
                        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                      </svg>
                    </div>
                  ) : availableDownloads.length > 0 ? (
                    <div className="max-h-64 overflow-y-auto">
                      {['tpr', 'itn', 'analysis'].map(category => {
                        const categoryFiles = availableDownloads.filter(f => f.category === category);
                        if (categoryFiles.length === 0) return null;

                        return (
                          <div key={category} className="mb-3">
                            <p className="text-xs font-medium text-gray-600 dark:text-dark-text-secondary mb-1">
                              {categoryLabels[category] || category}
                            </p>
                            {categoryFiles.map((file, index) => (
                              <a
                                key={index}
                                href={file.url}
                                download={file.filename}
                                className="flex items-center justify-between p-2 rounded hover:bg-gray-100 dark:hover:bg-dark-bg-tertiary group"
                                onClick={() => {
                                  toast.success(`Downloading ${file.name}...`);
                                  setIsDownloadOpen(false);
                                }}
                              >
                                <div className="flex-1 min-w-0">
                                  <p className="text-sm font-medium text-gray-900 dark:text-dark-text truncate">{file.name}</p>
                                  <p className="text-xs text-gray-500 dark:text-dark-text-secondary truncate">{file.description}</p>
                                </div>
                                <svg className="w-4 h-4 text-gray-400 dark:text-dark-text-secondary group-hover:text-blue-600 ml-2 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4 4m0 0l-4-4m4 4V4" />
                                </svg>
                              </a>
                            ))}
                          </div>
                        );
                      })}
                    </div>
                  ) : (
                    <div className="text-center py-4">
                      <svg className="mx-auto h-8 w-8 text-gray-400 dark:text-dark-text-secondary mb-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 21h10a2 2 0 002-2V9.414a1 1 0 00-.293-.707l-5.414-5.414A1 1 0 0012.586 3H7a2 2 0 00-2 2v14a2 2 0 002 2z" />
                      </svg>
                      <p className="text-sm text-gray-500 dark:text-dark-text-secondary">No files available</p>
                      <p className="text-xs text-gray-400 dark:text-dark-text-secondary mt-1">Complete an analysis to download results</p>
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>

          {/* Theme Toggle Button */}
          <button
            onClick={toggleTheme}
            className="relative flex items-center justify-center w-10 h-10 text-gray-700 dark:text-dark-text bg-white dark:bg-dark-bg-tertiary border border-gray-300 dark:border-dark-border rounded-lg hover:bg-gray-50 dark:hover:bg-dark-border focus:outline-none focus:ring-2 focus:ring-blue-500"
            title={theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
          >
            {/* Sun Icon - visible in dark mode */}
            <svg
              className={`w-5 h-5 transition-all duration-300 ${
                theme === 'dark' ? 'rotate-0 scale-100' : 'rotate-90 scale-0 absolute'
              }`}
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z"
              />
            </svg>
            {/* Moon Icon - visible in light mode */}
            <svg
              className={`w-5 h-5 transition-all duration-300 ${
                theme === 'light' ? 'rotate-0 scale-100' : '-rotate-90 scale-0 absolute'
              }`}
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z"
              />
            </svg>
          </button>

        </div>
      </div>
      
      {/* Clear Confirmation Dialog */}
      {showClearConfirm && (
        <div className="fixed inset-0 z-50 overflow-y-auto">
          <div className="flex items-center justify-center min-h-screen px-4 pt-4 pb-20 text-center">
            {/* Backdrop */}
            <div
              className="fixed inset-0 bg-black bg-opacity-50 transition-opacity"
              onClick={cancelClear}
            />
            
            {/* Dialog */}
            <div className="relative inline-block p-6 overflow-hidden text-left align-middle transition-all transform bg-white dark:bg-dark-bg-secondary rounded-lg shadow-xl">
              <div className="flex items-center mb-4">
                <div className="flex items-center justify-center w-12 h-12 bg-red-100 dark:bg-red-900/30 rounded-full">
                  <svg className="w-6 h-6 text-red-600 dark:text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                  </svg>
                </div>
                <h3 className="ml-3 text-lg font-medium text-gray-900 dark:text-dark-text">
                  Start New Chat
                </h3>
              </div>

              <p className="text-sm text-gray-500 dark:text-dark-text-secondary">
                This will clear your current chat and start fresh. Are you sure you want to continue?
              </p>

              <div className="flex justify-end mt-6 space-x-3">
                <button
                  onClick={cancelClear}
                  disabled={isClearing}
                  className="px-4 py-2 text-sm font-medium text-gray-700 dark:text-dark-text bg-white dark:bg-dark-bg-tertiary border border-gray-300 dark:border-dark-border rounded-lg hover:bg-gray-50 dark:hover:bg-dark-border focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Cancel
                </button>
                <button
                  onClick={confirmClear}
                  disabled={isClearing}
                  className="px-4 py-2 text-sm font-medium text-white bg-red-600 rounded-lg hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-red-500 disabled:opacity-50 disabled:cursor-not-allowed flex items-center"
                >
                  {isClearing ? (
                    <>
                      <svg className="animate-spin -ml-1 mr-2 h-4 w-4 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                      </svg>
                      Starting...
                    </>
                  ) : (
                    'Start New Chat'
                  )}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </>
  );
};

export default Toolbar;
