import React, { useState, useEffect } from 'react';
import { useChatStore } from '@/stores/chatStore';
import ChatContainer from './Chat/ChatContainer';
import Sidebar from './Sidebar/Sidebar';
import Toolbar from './Toolbar/Toolbar';
import SettingsModal from './Modal/SettingsModal';
import PrivacyModal from './Modal/PrivacyModal';

const MainInterface: React.FC = () => {
  const [showSettings, setShowSettings] = useState(false);
  const [showPrivacyModal, setShowPrivacyModal] = useState(false);
  
  // Check for first-time user on mount
  useEffect(() => {
    const hasAcceptedPrivacy = localStorage.getItem('chatmrpt_privacy_accepted');
    if (!hasAcceptedPrivacy) {
      setShowPrivacyModal(true);
    }
  }, []);
  
  const handlePrivacyAccept = () => {
    localStorage.setItem('chatmrpt_privacy_accepted', 'true');
    localStorage.setItem('chatmrpt_privacy_accepted_date', new Date().toISOString());
    setShowPrivacyModal(false);
  };
  
  return (
    <div className="h-screen bg-gray-50 dark:bg-dark-bg flex flex-col overflow-hidden">
      {/* Toolbar with New Chat, Export, Theme Toggle */}
      <Toolbar />

      {/* Main Content Area - Fixed height container */}
      <div className="flex-1 flex overflow-hidden min-h-0">
        {/* Sidebar - Fixed position, doesn't scroll with content */}
        <div className="flex-shrink-0 h-full">
          <Sidebar onOpenSettings={() => setShowSettings(true)} />
        </div>

        {/* Chat Container - This is the only scrollable area */}
        <main className="flex-1 flex flex-col min-w-0 overflow-hidden">
          <ChatContainer />
        </main>
      </div>
      
      {/* Settings Modal */}
      <SettingsModal
        isOpen={showSettings}
        onClose={() => setShowSettings(false)}
      />
      
      {/* Privacy Modal - First Run */}
      <PrivacyModal
        isOpen={showPrivacyModal}
        onAccept={handlePrivacyAccept}
      />
    </div>
  );
};

export default MainInterface;
