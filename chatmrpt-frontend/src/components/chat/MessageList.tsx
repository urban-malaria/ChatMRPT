import React, { useEffect, useRef } from 'react';
import { ChatMessage } from '../../utils/types';
import Message from './Message';

interface MessageListProps {
  messages: ChatMessage[];
  isInitialState: boolean;
  isStreaming: boolean;
}

const MessageList: React.FC<MessageListProps> = ({ 
  messages, 
  isInitialState,
  isStreaming 
}) => {
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  if (isInitialState) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="text-center max-w-2xl px-4">
          <h1 className="text-4xl font-bold text-gray-900 dark:text-white mb-4">
            Welcome to ChatMRPT
          </h1>
          <p className="text-lg text-gray-600 dark:text-gray-400 mb-8">
            Upload your malaria risk data to begin analysis, or ask me any questions
            about malaria risk prioritization.
          </p>
          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <div className="bg-blue-50 dark:bg-blue-900/20 p-4 rounded-lg">
              <h3 className="font-semibold text-blue-900 dark:text-blue-300 mb-2">
                Quick Start
              </h3>
              <p className="text-sm text-blue-700 dark:text-blue-400">
                Upload your CSV data and shapefile to get started with analysis
              </p>
            </div>
            <div className="bg-green-50 dark:bg-green-900/20 p-4 rounded-lg">
              <h3 className="font-semibold text-green-900 dark:text-green-300 mb-2">
                Sample Data
              </h3>
              <p className="text-sm text-green-700 dark:text-green-400">
                Try our sample dataset to explore the tool's capabilities
              </p>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full overflow-y-auto">
      <div className="flex-1 max-w-4xl mx-auto w-full p-4">
        {messages.map((message) => (
          <Message 
            key={message.id} 
            message={message}
            isStreaming={isStreaming && message.streaming}
          />
        ))}
        <div ref={messagesEndRef} />
      </div>
    </div>
  );
};

export default MessageList;