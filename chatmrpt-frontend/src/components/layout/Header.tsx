import React from 'react';
import { ChevronDownIcon } from '@heroicons/react/24/outline';
import { useApp } from '../../store/AppContext';

const Header: React.FC = () => {
  const { state } = useApp();

  return (
    <header className="fixed top-0 left-0 right-0 h-14 bg-white dark:bg-[#212121] border-b border-gray-200 dark:border-gray-600 z-50">
      <div className="flex items-center justify-between h-full px-4">
        {/* Left side - Logo and Title */}
        <div className="flex items-center">
          <div className="flex items-center space-x-2">
            <div className="w-7 h-7 rounded-sm bg-black dark:bg-white flex items-center justify-center">
              <span className="text-white dark:text-black font-bold text-sm">M</span>
            </div>
            <h1 className="text-lg font-semibold text-gray-800 dark:text-gray-100">
              ChatMRPT
            </h1>
            <ChevronDownIcon className="h-4 w-4 text-gray-500 dark:text-gray-400" />
          </div>
        </div>

        {/* Center - Status */}
        <div className="flex items-center">
          <span className="text-sm text-gray-500 dark:text-gray-400">
            {state.messages.length === 0 ? 'Ready for analysis' : 'Analyzing malaria data'}
          </span>
        </div>

        {/* Right side - Actions */}
        <div className="flex items-center space-x-3">
          <button className="p-2 rounded-md hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors">
            <svg className="h-5 w-5 text-gray-600 dark:text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12" />
            </svg>
          </button>
          
          <div className="w-8 h-8 rounded-full bg-teal-600 flex items-center justify-center cursor-pointer">
            <span className="text-white text-sm font-medium">U</span>
          </div>
        </div>
      </div>
    </header>
  );
};

export default Header;