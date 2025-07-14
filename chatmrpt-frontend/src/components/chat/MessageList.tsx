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
        <div className="text-center max-w-lg px-4">
          <h1 className="text-3xl font-semibold text-gray-800 dark:text-gray-100 mb-2">
            ChatMRPT
          </h1>
          <p className="text-gray-600 dark:text-gray-400">
            How can I help you today?
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="flex-1 overflow-y-auto">
      {messages.map((message) => (
        <Message 
          key={message.id} 
          message={message}
          isStreaming={isStreaming && message.streaming}
        />
      ))}
      <div ref={messagesEndRef} className="h-32" />
    </div>
  );
};

export default MessageList;