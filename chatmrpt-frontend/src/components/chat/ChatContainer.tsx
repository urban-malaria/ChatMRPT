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
      className={`relative flex flex-col h-screen overflow-hidden transition-all duration-500 ${
        isInitialState ? 'justify-center' : ''
      }`}
    >
      <div className="flex-1 overflow-y-auto">
        <MessageList 
          messages={state.messages}
          isInitialState={isInitialState}
          isStreaming={isStreaming}
        />
      </div>

      <div className="absolute bottom-0 left-0 w-full border-t md:border-t-0 dark:border-white/20 md:border-transparent md:dark:border-transparent bg-white dark:bg-gray-800 md:!bg-transparent">
        <form className="stretch mx-2 flex flex-row gap-3 last:mb-2 md:mx-4 md:last:mb-6 lg:mx-auto lg:max-w-2xl xl:max-w-3xl">
          <div className="relative flex h-full flex-1 items-stretch md:flex-col">
            <div className="flex flex-col w-full py-2 flex-grow md:py-3 md:pl-4 relative border border-black/10 bg-white dark:border-gray-900/50 dark:text-white dark:bg-gray-700 rounded-md shadow-[0_0_10px_rgba(0,0,0,0.10)] dark:shadow-[0_0_15px_rgba(0,0,0,0.10)]">
              <div className="flex items-center space-x-2 px-3">
                <button
                  type="button"
                  onClick={onUploadClick}
                  className="p-1 rounded-md hover:bg-gray-100 dark:hover:bg-gray-600 transition-colors"
                  title="Upload Files"
                >
                  <PaperClipIcon className="h-4 w-4 text-gray-600 dark:text-gray-400" />
                </button>
                <button
                  type="button"
                  onClick={onReportClick}
                  className="p-1 rounded-md hover:bg-gray-100 dark:hover:bg-gray-600 transition-colors"
                  title="Generate Report"
                >
                  <DocumentTextIcon className="h-4 w-4 text-gray-600 dark:text-gray-400" />
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
        </form>
      </div>
    </div>
  );
};

export default ChatContainer;