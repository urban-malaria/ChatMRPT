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
  const { state, dispatch } = useApp();
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
      className="relative flex flex-col h-full bg-white dark:bg-[#212121]"
    >
      <div className="flex-1 overflow-y-auto pb-32 scrollbar-thin">
        <MessageList 
          messages={state.messages}
          isInitialState={isInitialState}
          isStreaming={isStreaming}
        />
      </div>

      <div className="absolute bottom-0 left-0 right-0 bg-white dark:bg-[#212121]">
        <div className="mx-auto max-w-2xl px-4 py-4">
          <div className="relative flex items-center">
            <MessageInput 
              onSendMessage={handleSendMessage}
              disabled={isStreaming}
              placeholder="Message ChatMRPT..."
              className="pr-24"
            />
            
            <div className="absolute right-2 flex items-center space-x-1 z-10">
              <button
                type="button"
                onClick={onUploadClick}
                className="p-1.5 rounded-md hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors group"
                title="Upload Files"
                aria-label="Upload files"
              >
                <PaperClipIcon className="h-5 w-5 text-gray-400 group-hover:text-gray-600 dark:text-gray-400 dark:group-hover:text-gray-200 transition-colors" />
              </button>
              
              <button
                type="button"
                onClick={onReportClick}
                className="p-1.5 rounded-md hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors group"
                title="Generate Report"
              >
                <DocumentTextIcon className="h-5 w-5 text-gray-400 group-hover:text-gray-600 dark:text-gray-400 dark:group-hover:text-gray-200 transition-colors" />
              </button>
            </div>
          </div>
          
          <div className="text-center mt-2">
            <p className="text-xs text-gray-500 dark:text-gray-400">
              ChatMRPT can make mistakes. Consider checking important information.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ChatContainer;