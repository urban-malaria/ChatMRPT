import React from 'react';
import { Bars3Icon } from '@heroicons/react/24/outline';
import { useApp } from '../../store/AppContext';

const Header: React.FC = () => {
  const { dispatch } = useApp();

  const handleSidebarToggle = () => {
    dispatch({ type: 'TOGGLE_SIDEBAR' });
  };

  return (
    <header className="fixed top-0 left-0 right-0 z-40 bg-white dark:bg-gray-800 shadow-sm">
      <div className="flex items-center justify-between h-16 px-4">
        <div className="flex items-center space-x-4">
          <button
            onClick={handleSidebarToggle}
            className="p-2 rounded-md hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
            aria-label="Toggle sidebar"
          >
            <Bars3Icon className="h-6 w-6 text-gray-700 dark:text-gray-300" />
          </button>
          
          <div className="flex flex-col">
            <h1 className="text-xl font-semibold text-gray-900 dark:text-white">
              ChatMRPT
            </h1>
            <span className="text-xs text-gray-500 dark:text-gray-400">
              Malaria Risk Analysis
            </span>
          </div>
        </div>
        
        <div className="flex items-center space-x-4">
          {/* Additional header controls can go here */}
        </div>
      </div>
    </header>
  );
};

export default Header;