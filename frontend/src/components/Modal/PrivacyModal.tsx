import React, { useEffect } from 'react';

interface PrivacyModalProps {
  isOpen: boolean;
  onAccept: () => void;
}

const PrivacyModal: React.FC<PrivacyModalProps> = ({ isOpen, onAccept }) => {
  // Prevent closing with Escape key
  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && isOpen) {
        e.preventDefault();
      }
    };
    
    if (isOpen) {
      document.addEventListener('keydown', handleEscape);
      // Lock body scroll when modal is open
      document.body.style.overflow = 'hidden';
    }
    
    return () => {
      document.removeEventListener('keydown', handleEscape);
      document.body.style.overflow = '';
    };
  }, [isOpen]);
  
  if (!isOpen) return null;
  
  return (
    <>
      {/* Backdrop - not clickable */}
      <div className="fixed inset-0 bg-gray-900 bg-opacity-75 z-[10000]" />
      
      {/* Modal */}
      <div className="fixed inset-0 z-[10001] overflow-y-auto">
        <div className="flex min-h-full items-center justify-center p-4">
          <div className="relative bg-white dark:bg-dark-bg-secondary rounded-lg shadow-2xl w-full max-w-2xl">
            {/* Header */}
            <div className="bg-gradient-to-r from-blue-600 to-blue-700 px-6 py-4 rounded-t-lg">
              <h2 className="text-xl font-semibold text-white flex items-center">
                <svg className="w-6 h-6 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z" />
                </svg>
                Privacy Notice
              </h2>
            </div>
            
            {/* Content */}
            <div className="px-6 py-4 max-h-[60vh] overflow-y-auto">
              <div className="space-y-4 text-gray-700 dark:text-dark-text-secondary">
                <div>
                  <h3 className="font-semibold text-gray-900 dark:text-dark-text mb-2">Welcome to ChatMRPT</h3>
                  <p className="text-sm">
                    ChatMRPT is an AI-powered tool designed to assist with malaria risk analysis and
                    prioritization. Before you begin, please review how we handle your data.
                  </p>
                </div>

                <div>
                  <h4 className="font-semibold text-gray-900 dark:text-dark-text mb-2">Data Collection & Usage</h4>
                  <ul className="text-sm space-y-2 list-disc list-inside">
                    <li>
                      <strong>Your account:</strong> the name and email address you sign in with, used to keep
                      your chats together.
                    </li>
                    <li>
                      <strong>Uploaded files and results:</strong> files you upload (CSV, Excel, shapefiles) and
                      the maps, tables and downloads made from them are stored on ChatMRPT's servers so you can
                      return to your work. They are not deleted automatically when you close the page.
                    </li>
                    <li>
                      <strong>Chat history:</strong> your conversations are saved so you can reopen them from
                      Chats. Messages are also kept in a usage log that the ChatMRPT team uses to improve the
                      tool and for research.
                    </li>
                    <li>
                      <strong>AI services:</strong> to answer you, your messages and the relevant parts of your
                      data (such as column names, summary figures and ward values) are sent to OpenAI. General
                      malaria questions shown as a comparison of two answers are also sent to models hosted by Groq.
                    </li>
                    <li>
                      <strong>Backups:</strong> the usage log and saved chats are backed up daily to secure cloud
                      storage (Amazon Web Services).
                    </li>
                  </ul>
                </div>
                
                <div>
                  <h4 className="font-semibold text-gray-900 dark:text-dark-text mb-2">Data Security</h4>
                  <ul className="text-sm space-y-2 list-disc list-inside">
                    <li>All data is sent over encrypted connections (HTTPS)</li>
                    <li>Each user's data is kept separate; other users cannot see it</li>
                    <li>Please upload aggregated facility or ward data only, not personal or patient-level information</li>
                    <li>Settings, then Data &amp; Privacy, then Clear All Local Data removes the chats stored in this browser.
                      To have your data deleted from ChatMRPT's servers, email the ChatMRPT team at{' '}
                      <a href="mailto:urbanmalaria@gmail.com" className="underline text-blue-600 dark:text-blue-400">urbanmalaria@gmail.com</a>.</li>
                  </ul>
                </div>

                <div>
                  <h4 className="font-semibold text-gray-900 dark:text-dark-text mb-2">AI Usage Disclosure</h4>
                  <p className="text-sm">
                    ChatMRPT uses advanced AI models to analyze data and generate insights. While we
                    strive for accuracy, AI-generated content should be reviewed by domain experts before
                    making critical decisions.
                  </p>
                </div>

                <div className="bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800 rounded-lg p-3">
                  <p className="text-sm text-amber-800 dark:text-amber-300">
                    <strong>Important:</strong> This tool is designed for research and planning purposes.
                    Always consult with public health professionals for official malaria intervention strategies.
                  </p>
                </div>

                <div>
                  <h4 className="font-semibold text-gray-900 dark:text-dark-text mb-2">Your Consent</h4>
                  <p className="text-sm">
                    By clicking "Accept and Continue", you acknowledge that you have read and understood
                    this privacy notice and agree to the processing of your data as described above.
                  </p>
                </div>
              </div>
            </div>
            
            {/* Footer */}
            <div className="bg-gray-50 dark:bg-dark-bg-tertiary px-6 py-4 rounded-b-lg">
              <div className="flex items-center justify-between">
                <p className="text-xs text-gray-500 dark:text-dark-text-secondary">
                  This notice is shown once per browser.
                </p>
                <button
                  onClick={onAccept}
                  className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 transition-colors font-medium"
                >
                  Accept and Continue
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </>
  );
};

export default PrivacyModal;