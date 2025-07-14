import React from 'react';
import { ChatMessage } from '../../utils/types';
import ReactMarkdown from 'react-markdown';

interface MessageProps {
  message: ChatMessage;
  isStreaming?: boolean;
}

const Message: React.FC<MessageProps> = ({ message, isStreaming }) => {
  const isUser = message.role === 'user';

  return (
    <div className="group w-full">
      <div className="text-base m-auto md:max-w-2xl lg:max-w-2xl xl:max-w-3xl p-4 md:py-6">
        <div className="flex gap-3">
          <div className="flex-1 overflow-hidden">
            {isUser ? (
              <div className="text-gray-800 dark:text-gray-100 font-semibold mb-1">You</div>
            ) : (
              <div className="text-gray-800 dark:text-gray-100 font-semibold mb-1">ChatMRPT</div>
            )}
            <div className="text-gray-800 dark:text-gray-100">
              {isUser ? (
                <div>{message.content}</div>
              ) : (
                <>
                  <ReactMarkdown>
                    {message.content}
                  </ReactMarkdown>
                  {isStreaming && (
                    <span className="inline-block w-2 h-4 bg-gray-400 dark:bg-gray-500 animate-pulse ml-1" />
                  )}
                </>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Message;