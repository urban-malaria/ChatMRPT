import React, { useEffect, useRef, useState } from 'react';
import MessageList from './MessageList';
import MessageInput from './MessageInput';
import { useApp } from '../../store/AppContext';
import { useChat } from '../../hooks/useChat';
import { PaperClipIcon, DocumentTextIcon } from '@heroicons/react/24/outline';

interface ChatContainerProps {
  onUploadClick: () => void;
  onReportClick: () => void;
}

const ChatContainer: React.FC<ChatContainerProps> = ({ 
  onUploadClick, 
  onReportClick 
}) => {
  const { state } = useApp();
  const { sendMessage, isStreaming } = useChat();
  const [isInitialState, setIsInitialState] = useState(true);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    // Transition from initial state when first message is added
    if (state.messages.length > 0 && isInitialState) {
      setIsInitialState(false);
    }
  }, [state.messages.length, isInitialState]);

  const handleSendMessage = async (message: string) => {
    await sendMessage(message);
  };

  return (
    <div
      ref={containerRef}
      className={`flex flex-col h-screen pt-16 transition-all duration-500 ${
        isInitialState ? 'justify-center' : ''
      }`}
    >
      <div className="flex-1 overflow-hidden">
        <MessageList 
          messages={state.messages}
          isInitialState={isInitialState}
          isStreaming={isStreaming}
        />
      </div>

      <div className="border-t dark:border-gray-700 bg-white dark:bg-gray-800">
        <div className="max-w-4xl mx-auto p-4">
          <div className="flex items-end space-x-2">
            <div className="flex space-x-2">
              <button
                onClick={onUploadClick}
                className="p-2 rounded-md hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
                title="Upload Files"
              >
                <PaperClipIcon className="h-5 w-5 text-gray-600 dark:text-gray-400" />
              </button>
              <button
                onClick={onReportClick}
                className="p-2 rounded-md hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
                title="Generate Report"
              >
                <DocumentTextIcon className="h-5 w-5 text-gray-600 dark:text-gray-400" />
              </button>
            </div>
            
            <MessageInput 
              onSendMessage={handleSendMessage}
              disabled={isStreaming}
              placeholder={
                isInitialState 
                  ? "Start by uploading your data or asking a question..."
                  : "Ask me about malaria risk analysis..."
              }
            />
          </div>
        </div>
      </div>
    </div>
  );
};

export default ChatContainer;