import React from 'react';
import { Bars3Icon } from '@heroicons/react/24/outline';
import { useApp } from '../../store/AppContext';

const Header: React.FC = () => {
  const { dispatch } = useApp();

  const handleSidebarToggle = () => {
    dispatch({ type: 'TOGGLE_SIDEBAR' });
  };

  return (
    <header className="sticky top-0 z-10 flex items-center justify-center border-b border-gray-200 bg-white dark:bg-gray-800 dark:border-gray-700 px-4 py-2 h-14">
      <div className="flex items-center justify-between w-full max-w-3xl">
        <div className="flex items-center space-x-3">
          <button
            onClick={handleSidebarToggle}
            className="p-1.5 rounded-md hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
            aria-label="Toggle sidebar"
          >
            <Bars3Icon className="h-5 w-5 text-gray-700 dark:text-gray-300" />
          </button>
          
          <h1 className="text-lg font-medium text-gray-900 dark:text-white">
            ChatMRPT
          </h1>
        </div>
        
        <div className="flex items-center">
          <span className="text-sm text-gray-500 dark:text-gray-400">
            Malaria Risk Analysis
          </span>
        </div>
      </div>
    </header>
  );
};

export default Header;