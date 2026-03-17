import React, { useState } from 'react';
import { useChatStore } from '@/stores/chatStore';
import { useUserStore } from '@/stores/userStore';
import api from '@/services/api';
import toast from 'react-hot-toast';
import ProfileSection from '@/components/Profile/ProfileSection';
import UserAvatar from '@/components/Profile/UserAvatar';
import LoginModal from '@/components/Auth/LoginModal';
import SignupModal from '@/components/Auth/SignupModal';
import ConversationList from '@/components/Sidebar/ConversationList';

interface SidebarProps {
  onOpenSettings?: () => void;
}

const Sidebar: React.FC<SidebarProps> = ({ onOpenSettings }) => {
  const [isCollapsed, setIsCollapsed] = useState(true);  // Start collapsed
  const [activeSection, setActiveSection] = useState<'history' | 'samples'>('history');
  const [showLoginModal, setShowLoginModal] = useState(false);
  const [showSignupModal, setShowSignupModal] = useState(false);

  const session = useChatStore((state) => state.session);
  const setUploadedFiles = useChatStore((state) => state.setUploadedFiles);
  const { user, isAuthenticated } = useUserStore();
  
  if (isCollapsed) {
    return (
      <>
        {/* Auth Modals - needed for collapsed state too */}
        <LoginModal
          isOpen={showLoginModal}
          onClose={() => setShowLoginModal(false)}
          onSwitchToSignup={() => {
            setShowLoginModal(false);
            setShowSignupModal(true);
          }}
        />
        <SignupModal
          isOpen={showSignupModal}
          onClose={() => setShowSignupModal(false)}
          onSwitchToLogin={() => {
            setShowSignupModal(false);
            setShowLoginModal(true);
          }}
        />

        <div className="w-16 bg-gray-50 dark:bg-dark-bg-secondary border-r border-gray-200 dark:border-dark-border flex flex-col items-center py-4 transition-all duration-300 ease-in-out h-full overflow-hidden">
          {/* Top Section */}
          <div className="flex-shrink-0">
            <button
              onClick={() => setIsCollapsed(false)}
              className="p-2 hover:bg-gray-200 dark:hover:bg-dark-border rounded-lg transition-all duration-200 group text-gray-600 dark:text-dark-text-secondary"
              title="Expand sidebar"
            >
              <svg className="w-5 h-5 group-hover:translate-x-0.5 transition-transform" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
              </svg>
            </button>

            {/* Quick Actions in Collapsed State */}
            <div className="mt-8 space-y-4">
              <button
                className="p-2 hover:bg-gray-200 dark:hover:bg-dark-border rounded-lg transition-colors"
                title="Chats"
                onClick={() => {
                  setIsCollapsed(false);
                  setActiveSection('history');
                }}
              >
                <svg className="w-5 h-5 text-gray-600 dark:text-dark-text-secondary" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
              </button>

              <button
                className="p-2 hover:bg-gray-200 dark:hover:bg-dark-border rounded-lg transition-colors"
                title="Sample Data"
                onClick={() => {
                  setIsCollapsed(false);
                  setActiveSection('samples');
                }}
              >
                <svg className="w-5 h-5 text-gray-600 dark:text-dark-text-secondary" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" />
                </svg>
              </button>
            </div>
          </div>

          {/* Spacer to push bottom section down */}
          <div className="flex-1" />

          {/* Bottom Section - Settings & Profile (collapsed) - Pinned to bottom */}
          <div className="flex-shrink-0 border-t border-gray-200 dark:border-dark-border pt-4 pb-4 space-y-3 w-full flex flex-col items-center">
            {/* Settings Icon */}
            <button
              onClick={onOpenSettings}
              className="p-2 hover:bg-gray-200 dark:hover:bg-dark-border rounded-lg transition-colors"
              title="Settings"
            >
              <svg className="w-5 h-5 text-gray-600 dark:text-dark-text-secondary" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
              </svg>
            </button>

            {/* Profile/Sign In Icon */}
            {isAuthenticated && user ? (
              <button
                onClick={() => setIsCollapsed(false)}
                className="p-1 hover:bg-gray-200 dark:hover:bg-dark-border rounded-lg transition-colors"
                title={user.username}
              >
                <UserAvatar username={user.username} size="sm" />
              </button>
            ) : (
              <button
                onClick={() => setShowLoginModal(true)}
                className="p-2 hover:bg-gray-200 dark:hover:bg-dark-border rounded-lg transition-colors"
                title="Sign In"
              >
                <svg className="w-5 h-5 text-gray-600 dark:text-dark-text-secondary" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" />
                </svg>
              </button>
            )}
          </div>
        </div>
      </>
    );
  }
  
  return (
    <>
      {/* Auth Modals */}
      <LoginModal
        isOpen={showLoginModal}
        onClose={() => setShowLoginModal(false)}
        onSwitchToSignup={() => {
          setShowLoginModal(false);
          setShowSignupModal(true);
        }}
      />
      <SignupModal
        isOpen={showSignupModal}
        onClose={() => setShowSignupModal(false)}
        onSwitchToLogin={() => {
          setShowSignupModal(false);
          setShowLoginModal(true);
        }}
      />

      <div className="w-80 bg-gray-50 dark:bg-dark-bg-secondary border-r border-gray-200 dark:border-dark-border flex flex-col transition-all duration-300 ease-in-out h-full overflow-hidden">
        {/* Sidebar Header - Fixed at top */}
        <div className="flex-shrink-0 p-4 border-b border-gray-200 dark:border-dark-border">
        <div className="flex justify-between items-center mb-4">
          <h3 className="font-semibold text-gray-900 dark:text-dark-text">Data Management</h3>
          <button
            onClick={() => setIsCollapsed(true)}
            className="p-1 hover:bg-gray-200 dark:hover:bg-dark-border rounded-lg transition-all duration-200 group text-gray-600 dark:text-dark-text-secondary"
            title="Collapse sidebar"
          >
            <svg className="w-5 h-5 group-hover:-translate-x-0.5 transition-transform" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
            </svg>
          </button>
        </div>

        {/* Section Tabs */}
        <div className="flex space-x-1 p-1 bg-gray-100 dark:bg-dark-bg-tertiary rounded-lg">
          <button
            onClick={() => setActiveSection('history')}
            className={`flex-1 px-3 py-1.5 text-sm font-medium rounded transition-all duration-200 ${
              activeSection === 'history'
                ? 'bg-white dark:bg-dark-bg text-blue-600 dark:text-blue-400 shadow-sm'
                : 'text-gray-600 dark:text-dark-text-secondary hover:text-gray-900 dark:hover:text-dark-text'
            }`}
          >
            Chats
          </button>
          <button
            onClick={() => setActiveSection('samples')}
            className={`flex-1 px-3 py-1.5 text-sm font-medium rounded transition-all duration-200 ${
              activeSection === 'samples'
                ? 'bg-white dark:bg-dark-bg text-blue-600 dark:text-blue-400 shadow-sm'
                : 'text-gray-600 dark:text-dark-text-secondary hover:text-gray-900 dark:hover:text-dark-text'
            }`}
          >
            Samples
          </button>
        </div>
      </div>
      
      {/* Content Sections */}
      <div className="p-4 flex-1 overflow-y-auto">
        {/* Chats Section */}
        {activeSection === 'history' && (
          <div className="animate-fadeIn">
            <ConversationList />
          </div>
        )}
        
        {/* Samples Section */}
        {activeSection === 'samples' && (
          <div className="animate-fadeIn">
            <h4 className="text-sm font-medium text-gray-700 dark:text-dark-text-secondary mb-3">Available Sample Datasets</h4>
            <div className="space-y-2">
              <button
                onClick={async () => {
                  try {
                    await api.upload.loadSampleData('kano', session.sessionId);
                    toast.success('Kano sample data loaded');
                    setUploadedFiles('kano_sample.csv', 'kano_boundaries.zip');
                  } catch (error) {
                    toast.error('Failed to load sample data');
                  }
                }}
                className="w-full p-3 bg-white dark:bg-dark-bg-tertiary border border-gray-200 dark:border-dark-border rounded-lg hover:bg-blue-50 dark:hover:bg-blue-900/20 hover:border-blue-300 dark:hover:border-blue-700 transition-all duration-200 group text-left"
              >
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm font-medium text-gray-900 dark:text-dark-text group-hover:text-blue-600 dark:group-hover:text-blue-400">Kano State</p>
                    <p className="text-xs text-gray-500 dark:text-dark-text-secondary">Ward-level malaria risk data</p>
                  </div>
                  <svg className="w-5 h-5 text-gray-400 dark:text-dark-text-secondary group-hover:text-blue-500 dark:group-hover:text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                  </svg>
                </div>
              </button>

              <button
                disabled
                className="w-full p-3 bg-gray-100 dark:bg-dark-bg-tertiary border border-gray-200 dark:border-dark-border rounded-lg opacity-50 cursor-not-allowed text-left"
              >
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm font-medium text-gray-500 dark:text-dark-text-secondary">Lagos State</p>
                    <p className="text-xs text-gray-400 dark:text-gray-500">Coming soon</p>
                  </div>
                  <svg className="w-5 h-5 text-gray-300 dark:text-gray-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z" />
                  </svg>
                </div>
              </button>
            </div>
          </div>
        )}
      </div>

        {/* Bottom Section - Settings & Profile - Pinned to bottom */}
        <div className="flex-shrink-0 border-t border-gray-200 dark:border-dark-border">
          {/* Settings Button */}
          <button
            onClick={onOpenSettings}
            className="w-full flex items-center px-6 py-3 text-sm font-medium text-gray-700 dark:text-dark-text hover:bg-gray-100 dark:hover:bg-dark-bg-tertiary transition-colors"
          >
            <svg className="w-5 h-5 mr-3 text-gray-500 dark:text-dark-text-secondary" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
            </svg>
            Settings
          </button>

          {/* Profile Section */}
          <ProfileSection
            onLoginClick={() => setShowLoginModal(true)}
            onSignupClick={() => setShowSignupModal(true)}
          />
        </div>
      </div>
    </>
  );
};

export default Sidebar;