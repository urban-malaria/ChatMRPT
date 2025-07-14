import React, { useState, useEffect } from 'react';
import Layout from '../components/layout/Layout';
import ChatContainer from '../components/chat/ChatContainer';
import { useSession } from '../hooks/useSession';

const Home: React.FC = () => {
  const [showUploadModal, setShowUploadModal] = useState(false);
  const [showReportModal, setShowReportModal] = useState(false);
  const { session } = useSession();

  // Initialize session on mount
  useEffect(() => {
    console.log('Session initialized:', session.sessionId);
  }, [session.sessionId]);

  const handleUploadClick = () => {
    setShowUploadModal(true);
    // TODO: Implement upload modal
  };

  const handleReportClick = () => {
    setShowReportModal(true);
    // TODO: Implement report modal
  };

  return (
    <Layout>
      <ChatContainer 
        onUploadClick={handleUploadClick}
        onReportClick={handleReportClick}
      />
      
      {/* TODO: Add modals here */}
    </Layout>
  );
};

export default Home;