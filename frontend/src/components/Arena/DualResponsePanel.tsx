import React from 'react';
import ReactMarkdown from 'react-markdown';

interface DualResponsePanelProps {
  responseA: string;
  responseB: string;
  modelA?: string | null;
  modelB?: string | null;
  isLoading: boolean;
  labelA?: string;  // Custom label for Response A
  labelB?: string;  // Custom label for Response B
}

const DualResponsePanel: React.FC<DualResponsePanelProps> = ({
  responseA,
  responseB,
  modelA,
  modelB,
  isLoading,
  labelA = 'Response A',
  labelB = 'Response B',
}) => {
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
      {/* Response A */}
      <div className="border border-gray-200 dark:border-dark-border rounded-lg p-4 bg-gray-50 dark:bg-dark-bg-tertiary">
        <div className="mb-3 flex justify-between items-center">
          <span className="font-semibold text-gray-700 dark:text-dark-text">{labelA}</span>
          {modelA && (
            <span className="text-sm bg-blue-100 dark:bg-blue-900/30 text-blue-800 dark:text-blue-300 px-2 py-1 rounded">
              {modelA}
            </span>
          )}
        </div>
        <div className="prose prose-sm dark:prose-invert max-w-none min-h-[200px] max-h-80 overflow-y-auto text-gray-800 dark:text-dark-text">
          {isLoading && !responseA ? (
            <div className="flex items-center justify-center h-32">
              <svg className="animate-spin h-6 w-6 text-gray-400 dark:text-dark-text-secondary" viewBox="0 0 24 24">
                <circle
                  className="opacity-25"
                  cx="12"
                  cy="12"
                  r="10"
                  stroke="currentColor"
                  strokeWidth="4"
                  fill="none"
                />
                <path
                  className="opacity-75"
                  fill="currentColor"
                  d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                />
              </svg>
            </div>
          ) : (
            <ReactMarkdown>{responseA || 'Waiting for response...'}</ReactMarkdown>
          )}
        </div>
      </div>

      {/* Response B */}
      <div className="border border-gray-200 dark:border-dark-border rounded-lg p-4 bg-gray-50 dark:bg-dark-bg-tertiary">
        <div className="mb-3 flex justify-between items-center">
          <span className="font-semibold text-gray-700 dark:text-dark-text">{labelB}</span>
          {modelB && (
            <span className="text-sm bg-green-100 dark:bg-green-900/30 text-green-800 dark:text-green-300 px-2 py-1 rounded">
              {modelB}
            </span>
          )}
        </div>
        <div className="prose prose-sm dark:prose-invert max-w-none min-h-[200px] max-h-80 overflow-y-auto text-gray-800 dark:text-dark-text">
          {isLoading && !responseB ? (
            <div className="flex items-center justify-center h-32">
              <svg className="animate-spin h-6 w-6 text-gray-400 dark:text-dark-text-secondary" viewBox="0 0 24 24">
                <circle
                  className="opacity-25"
                  cx="12"
                  cy="12"
                  r="10"
                  stroke="currentColor"
                  strokeWidth="4"
                  fill="none"
                />
                <path
                  className="opacity-75"
                  fill="currentColor"
                  d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                />
              </svg>
            </div>
          ) : (
            <ReactMarkdown>{responseB || 'Waiting for response...'}</ReactMarkdown>
          )}
        </div>
      </div>
    </div>
  );
};

export default DualResponsePanel;
