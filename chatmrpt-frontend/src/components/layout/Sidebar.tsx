import React from 'react';
import { XMarkIcon, SunIcon, MoonIcon } from '@heroicons/react/24/outline';
import { useApp } from '../../store/AppContext';

interface SidebarProps {
  isOpen: boolean;
  onClose: () => void;
}

const Sidebar: React.FC<SidebarProps> = ({ isOpen, onClose }) => {
  const { state, dispatch } = useApp();

  const toggleTheme = () => {
    dispatch({ 
      type: 'SET_THEME', 
      payload: state.theme === 'light' ? 'dark' : 'light' 
    });
  };

  const changeLanguage = (language: string) => {
    dispatch({ 
      type: 'SET_SESSION', 
      payload: { currentLanguage: language } 
    });
  };

  return (
    <>
      {/* Backdrop */}
      {isOpen && (
        <div
          className="fixed inset-0 bg-black bg-opacity-50 z-40 lg:hidden"
          onClick={onClose}
        />
      )}

      {/* Sidebar */}
      <div
        className={`fixed top-0 left-0 h-full w-80 bg-gray-50 dark:bg-[#171717] border-r border-gray-200 dark:border-gray-700 transform transition-transform duration-300 z-50 ${
          isOpen ? 'translate-x-0' : '-translate-x-full'
        }`}
      >
        <div className="flex flex-col h-full">
          {/* Header */}
          <div className="flex items-center justify-between p-4 border-b dark:border-gray-700">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
              Settings
            </h3>
            <button
              onClick={onClose}
              className="p-1 rounded-md hover:bg-gray-100 dark:hover:bg-gray-700"
              aria-label="Close sidebar"
            >
              <XMarkIcon className="h-6 w-6 text-gray-700 dark:text-gray-300" />
            </button>
          </div>

          {/* Content */}
          <div className="flex-1 overflow-y-auto p-4">
            {/* Appearance Section */}
            <div className="mb-6">
              <h4 className="text-sm font-medium text-gray-900 dark:text-white mb-3">
                Appearance
              </h4>
              <div className="flex items-center justify-between">
                <span className="text-sm text-gray-700 dark:text-gray-300">
                  Dark Mode
                </span>
                <button
                  onClick={toggleTheme}
                  className="relative inline-flex h-6 w-11 items-center rounded-full bg-gray-200 dark:bg-gray-600 transition-colors"
                  role="switch"
                  aria-checked={state.theme === 'dark'}
                >
                  <span className="sr-only">Toggle dark mode</span>
                  <span
                    className={`${
                      state.theme === 'dark' ? 'translate-x-6' : 'translate-x-1'
                    } inline-block h-4 w-4 transform rounded-full bg-white transition-transform`}
                  />
                </button>
              </div>
            </div>

            {/* Language Section */}
            <div className="mb-6">
              <h4 className="text-sm font-medium text-gray-900 dark:text-white mb-3">
                Language
              </h4>
              <select
                value={state.session.currentLanguage}
                onChange={(e) => changeLanguage(e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
              >
                <option value="en">English</option>
                <option value="ha">Hausa</option>
                <option value="yo">Yoruba</option>
                <option value="ig">Igbo</option>
                <option value="fr">French</option>
                <option value="ar">Arabic</option>
              </select>
            </div>

            {/* About Section */}
            <div className="mb-6">
              <h4 className="text-sm font-medium text-gray-900 dark:text-white mb-3">
                About
              </h4>
              <p className="text-sm text-gray-600 dark:text-gray-400 mb-2">
                ChatMRPT helps identify high-risk malaria areas for targeted
                interventions and resource allocation.
              </p>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                <strong>Version:</strong> 3.0 (React)
              </p>
            </div>
          </div>
        </div>
      </div>
    </>
  );
};

export default Sidebar;