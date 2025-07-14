import React, { useState, useEffect } from 'react';
import Layout from '../components/layout/Layout';
import ChatContainer from '../components/chat/ChatContainer';
import FileUploadModal from '../components/upload/FileUploadModal';
import VisualizationModal from '../components/visualization/VisualizationModal';
import { useSession } from '../hooks/useSession';
import { useApp } from '../store/AppContext';

const Home: React.FC = () => {
  const [showUploadModal, setShowUploadModal] = useState(false);
  const [showReportModal, setShowReportModal] = useState(false);
  const [showVisualizationModal, setShowVisualizationModal] = useState(false);
  const { session } = useSession();
  const { state } = useApp();

  // Initialize session on mount
  useEffect(() => {
    console.log('Session initialized:', session.sessionId);
  }, [session.sessionId]);

  // Watch for visualization data in app state
  useEffect(() => {
    if (state.visualizationData) {
      setShowVisualizationModal(true);
    }
  }, [state.visualizationData]);

  const handleUploadClick = () => {
    console.log('Upload button clicked!');
    setShowUploadModal(true);
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
      
      <FileUploadModal 
        isOpen={showUploadModal}
        onClose={() => setShowUploadModal(false)}
      />
      
      <VisualizationModal
        isOpen={showVisualizationModal}
        onClose={() => setShowVisualizationModal(false)}
        visualizationData={state.visualizationData}
        mapUrl={state.visualizationData?.mapUrl}
        mapType={state.visualizationData?.mapType}
      />
      
      {/* TODO: Add report modal here */}
    </Layout>
  );
};

export default Home;