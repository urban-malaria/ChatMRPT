import React from 'react';
import { ChatMessage } from '../../utils/types';
import { UserIcon, SparklesIcon } from '@heroicons/react/24/solid';
import ReactMarkdown from 'react-markdown';

interface MessageProps {
  message: ChatMessage;
  isStreaming?: boolean;
}

const Message: React.FC<MessageProps> = ({ message, isStreaming }) => {
  const isUser = message.role === 'user';

  return (
    <div className={`group w-full text-gray-800 dark:text-gray-100 border-b border-gray-200 dark:border-gray-700 ${
      isUser ? 'bg-white dark:bg-gray-800' : 'bg-gray-50 dark:bg-gray-900'
    }`}>
      <div className="max-w-3xl mx-auto md:px-5 lg:px-4 xl:px-5">
        <div className="flex p-4 gap-4 text-base md:gap-6 md:py-6">
          <div className="flex-shrink-0 flex flex-col relative items-end">
            <div className={`w-8 h-8 rounded-sm flex items-center justify-center ${
              isUser
                ? 'bg-gray-600 text-white'
                : 'bg-green-600 text-white'
            }`}>
              {isUser ? (
                <UserIcon className="h-5 w-5" />
              ) : (
                <SparklesIcon className="h-5 w-5" />
              )}
            </div>
          </div>
          
          <div className="relative flex w-[calc(100%-50px)] flex-col gap-1 md:gap-3 lg:w-[calc(100%-115px)]">
            <div className="flex flex-grow flex-col gap-3">
              <div className="min-h-[20px] flex flex-col items-start gap-4 whitespace-pre-wrap">
                {isUser ? (
                  <div>{message.content}</div>
                ) : (
                  <div className="markdown prose w-full break-words dark:prose-invert">
                    <ReactMarkdown>{message.content}</ReactMarkdown>
                    {isStreaming && (
                      <span className="inline-block w-2 h-4 bg-current animate-pulse ml-1" />
                    )}
                  </div>
                )}
              </div>
            </div>
            <div className="flex justify-between lg:block">
              <div className="text-gray-400 flex self-end lg:self-center justify-center mt-2 gap-2 md:gap-3 lg:gap-1 lg:absolute lg:top-0 lg:translate-x-full lg:right-0 lg:mt-0 lg:pl-2 visible">
                {/* Add copy/edit buttons here if needed */}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Message;