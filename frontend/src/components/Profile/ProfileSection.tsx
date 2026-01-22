import React, { useState, useRef, useEffect } from 'react';
import { useUserStore } from '@/stores/userStore';
import { authService } from '@/services/auth';
import UserAvatar from './UserAvatar';

interface ProfileSectionProps {
  onLoginClick: () => void;
  onSignupClick: () => void;
}

const ProfileSection: React.FC<ProfileSectionProps> = ({ onLoginClick, onSignupClick }) => {
  const { user, isAuthenticated } = useUserStore();
  const [isMenuOpen, setIsMenuOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);

  // Close menu when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(event.target as Node)) {
        setIsMenuOpen(false);
      }
    };

    if (isMenuOpen) {
      document.addEventListener('mousedown', handleClickOutside);
    }

    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, [isMenuOpen]);

  const handleLogout = async () => {
    setIsMenuOpen(false);
    await authService.logout();
  };

  if (!isAuthenticated || !user) {
    // Not authenticated - show sign in button
    return (
      <div className="p-4">
        <button
          onClick={onLoginClick}
          className="w-full px-4 py-2.5 bg-gradient-to-r from-blue-600 to-blue-700 text-white rounded-xl hover:from-blue-700 hover:to-blue-800 transition-all duration-200 font-medium shadow-sm hover:shadow-md transform hover:-translate-y-0.5"
        >
          Sign In
        </button>
        <button
          onClick={onSignupClick}
          className="w-full mt-3 px-4 py-2.5 bg-white dark:bg-dark-bg-tertiary text-gray-700 dark:text-dark-text border border-gray-200 dark:border-dark-border rounded-xl hover:bg-gray-50 dark:hover:bg-dark-border hover:border-gray-300 dark:hover:border-dark-text-secondary transition-all duration-200 font-medium shadow-sm"
        >
          Sign Up
        </button>
      </div>
    );
  }

  // Authenticated - show profile
  return (
    <div className="p-4 relative" ref={menuRef}>
      <button
        onClick={() => setIsMenuOpen(!isMenuOpen)}
        className="w-full flex items-center space-x-3 p-3 rounded-xl hover:bg-gray-100 dark:hover:bg-dark-bg-tertiary transition-all duration-200 group"
      >
        <UserAvatar username={user.username} size="md" />
        <div className="flex-1 text-left min-w-0">
          <div className="font-semibold text-gray-900 dark:text-dark-text truncate text-sm">{user.username}</div>
          <div className="text-xs text-gray-500 dark:text-dark-text-secondary truncate">{user.email}</div>
        </div>
        <svg
          className={`w-4 h-4 text-gray-400 dark:text-dark-text-secondary transition-transform duration-200 group-hover:text-gray-600 dark:group-hover:text-dark-text ${isMenuOpen ? 'rotate-180' : ''}`}
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      {/* Dropdown Menu */}
      {isMenuOpen && (
        <div className="absolute left-4 right-4 bottom-full mb-2 bg-white dark:bg-dark-bg-secondary border border-gray-100 dark:border-dark-border rounded-xl shadow-xl z-50 overflow-hidden animate-fadeIn">
          <button
            onClick={handleLogout}
            className="w-full px-4 py-3 text-left hover:bg-red-50 dark:hover:bg-red-900/20 transition-all duration-150 flex items-center space-x-3 text-red-600 dark:text-red-400 group"
          >
            <svg className="w-4 h-4 group-hover:translate-x-0.5 transition-transform" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1" />
            </svg>
            <span className="text-sm font-semibold">Sign Out</span>
          </button>
        </div>
      )}
    </div>
  );
};

export default ProfileSection;
