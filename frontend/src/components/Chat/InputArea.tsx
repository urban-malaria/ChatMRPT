import React, { useRef, useEffect, useState } from 'react';
import { useChatStore } from '@/stores/chatStore';
import { useAnalysisStore } from '@/stores/analysisStore';
import UploadModal from '../Modal/UploadModal';

interface InputAreaProps {
  value: string;
  onChange: (value: string) => void;
  onSend: () => void;
  onStop?: () => void;
  isLoading: boolean;
  placeholder?: string;
}

const InputArea: React.FC<InputAreaProps> = ({
  value,
  onChange,
  onSend,
  onStop,
  isLoading,
  placeholder = "Type your message...",
}) => {
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const [showSuggestions, setShowSuggestions] = useState(false); // Disabled for now
  const [showUploadModal, setShowUploadModal] = useState(false);
  const messages = useChatStore((state) => state.messages);
  const hasUploadedFiles = useChatStore((state) => state.session.hasUploadedFiles);
  const analysisResults = useAnalysisStore((state) => state.analysisResults);
  
  // Debug log
  console.log('InputArea - showUploadModal:', showUploadModal);
  
  // Auto-resize textarea
  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto';
      textareaRef.current.style.height = `${textareaRef.current.scrollHeight}px`;
    }
  }, [value]);
  
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      onSend();
    }
  };
  
  // Generate context-aware suggestions
  const getSuggestions = () => {
    // No messages yet - actionable first steps
    if (messages.length === 0) {
      return [
        "Load Kano sample dataset",
        "Calculate test positivity rate",
        "What analyses can you perform?",
        "Help me rank wards by malaria risk"
      ];
    }

    // Has uploaded files but no analysis
    if (hasUploadedFiles && analysisResults.length === 0) {
      return [
        "Run full risk analysis",
        "Calculate composite vulnerability scores",
        "Show data summary statistics",
        "List available indicators"
      ];
    }

    // Has analysis results
    if (analysisResults.length > 0) {
      return [
        "Show highest risk areas",
        "Generate risk map",
        "Export results to CSV",
        "Explain the methodology"
      ];
    }

    // Default - conversation ongoing
    return [
      "Run another analysis",
      "Upload different data",
      "Compare analysis methods",
      "Summarize findings"
    ];
  };
  
  const suggestions = getSuggestions();
  
  const handleSuggestionClick = (suggestion: string) => {
    onChange(suggestion);
    setShowSuggestions(false);
    textareaRef.current?.focus();
  };
  
  return (
    <div className="border-t border-gray-200 dark:border-dark-border bg-white dark:bg-dark-bg-secondary">
      {/* Suggestion Buttons */}
      {showSuggestions && suggestions.length > 0 && value.length === 0 && (
        <div className="px-4 pt-3 pb-2 animate-fadeIn">
          <div className="flex items-center justify-between mb-2">
            <span className="text-xs text-gray-500 dark:text-dark-text-secondary font-medium">Suggestions</span>
            <button
              onClick={() => setShowSuggestions(false)}
              className="text-xs text-gray-400 dark:text-dark-text-secondary hover:text-gray-600 dark:hover:text-dark-text"
            >
              Hide
            </button>
          </div>
          <div className="flex flex-wrap gap-2">
            {suggestions.map((suggestion, index) => (
              <button
                key={index}
                onClick={() => handleSuggestionClick(suggestion)}
                className="px-3 py-1.5 text-sm bg-gray-100 dark:bg-dark-bg-tertiary hover:bg-gray-200 dark:hover:bg-dark-border text-gray-700 dark:text-dark-text rounded-full transition-all duration-200 hover:scale-105 animate-slideIn"
                style={{ animationDelay: `${index * 50}ms` }}
              >
                {suggestion}
              </button>
            ))}
          </div>
        </div>
      )}
      
      <div className="p-4">
        <div className="flex items-end space-x-3">
        {/* Input Field */}
        <div className="flex-1">
          <textarea
            ref={textareaRef}
            value={value}
            onChange={(e) => onChange(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder={placeholder}
            disabled={isLoading}
            rows={1}
            className="w-full resize-none rounded-lg border border-gray-300 dark:border-dark-border bg-white dark:bg-dark-bg-tertiary text-gray-900 dark:text-dark-text px-4 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent disabled:bg-gray-50 dark:disabled:bg-dark-bg disabled:text-gray-500 dark:disabled:text-dark-text-secondary placeholder-gray-400 dark:placeholder-gray-500"
            style={{ maxHeight: '200px' }}
          />
        </div>
        
        {/* Action Buttons */}
        <div className="flex space-x-2">
          {/* Upload Button */}
          <button
            type="button"
            onClick={(e) => {
              e.preventDefault();
              e.stopPropagation();
              console.log('Upload button clicked, opening modal...');
              setShowUploadModal(true);
            }}
            disabled={isLoading}
            className="p-2 text-gray-500 dark:text-dark-text-secondary hover:text-gray-700 dark:hover:text-dark-text disabled:opacity-50 transition-colors"
            title="Upload files"
          >
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M15.172 7l-6.586 6.586a2 2 0 102.828 2.828l6.414-6.586a4 4 0 00-5.656-5.656l-6.415 6.585a6 6 0 108.486 8.486L20.5 13"
              />
            </svg>
          </button>

          {/* Stop Button - shown when loading */}
          {isLoading && onStop && (
            <button
              onClick={onStop}
              className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-red-500 transition-colors"
              title="Stop generation"
            >
              <div className="flex items-center">
                <svg className="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
                Stop
              </div>
            </button>
          )}

          {/* Send Button */}
          <button
            onClick={onSend}
            disabled={isLoading || !value.trim()}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:bg-gray-300 disabled:cursor-not-allowed transition-colors"
          >
            {isLoading ? (
              <div className="flex items-center">
                <svg className="animate-spin h-4 w-4 mr-2" viewBox="0 0 24 24">
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
                Sending...
              </div>
            ) : (
              <div className="flex items-center">
                <span>Send</span>
                <svg className="w-4 h-4 ml-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M12 19l9 2-9-18-9 18 9-2zm0 0v-8"
                  />
                </svg>
              </div>
            )}
          </button>
        </div>
        </div>
        
        {/* Helper Text */}
        <div className="mt-2 text-xs text-gray-500 dark:text-dark-text-secondary">
          Press Enter to send, Shift+Enter for new line
        </div>
      </div>
      
      {/* Upload Modal */}
      <UploadModal
        isOpen={showUploadModal}
        onClose={() => setShowUploadModal(false)}
      />
    </div>
  );
};

export default InputArea;
