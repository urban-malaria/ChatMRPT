import React, { useState, useRef, useEffect } from 'react';
import { PaperAirplaneIcon } from '@heroicons/react/24/solid';

interface MessageInputProps {
  onSendMessage: (message: string) => void;
  disabled?: boolean;
  placeholder?: string;
  className?: string;
}

const MessageInput: React.FC<MessageInputProps> = ({ 
  onSendMessage, 
  disabled = false,
  placeholder = "Type your message...",
  className = ""
}) => {
  const [message, setMessage] = useState('');
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto';
      textareaRef.current.style.height = `${textareaRef.current.scrollHeight}px`;
    }
  }, [message]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (message.trim() && !disabled) {
      onSendMessage(message.trim());
      setMessage('');
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmit(e);
    }
  };

  return (
    <div className={`relative w-full ${className}`}>
      <textarea
        ref={textareaRef}
        value={message}
        onChange={(e) => setMessage(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder={placeholder}
        disabled={disabled}
        rows={1}
        className="m-0 w-full resize-none border border-gray-200 dark:border-gray-600 rounded-xl bg-gray-50 dark:bg-[#2f2f2f] py-3 pr-10 text-sm text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-500 focus:outline-none focus:border-gray-300 dark:focus:border-gray-500 transition-colors scrollbar-thin"
        style={{ maxHeight: '200px' }}
      />
      <button
        onClick={handleSubmit}
        disabled={disabled || !message.trim()}
        className="absolute bottom-3 right-2 p-1 rounded-md text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 disabled:hover:text-gray-400 disabled:opacity-40 transition-colors"
        title="Send message"
      >
        <PaperAirplaneIcon className="h-4 w-4 rotate-90" />
      </button>
    </div>
  );
};

export default MessageInput;