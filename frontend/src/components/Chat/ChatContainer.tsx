import React, { useRef, useEffect, useState } from 'react';
import { useChatStore } from '@/stores/chatStore';
import MessageList from './MessageList';
import InputArea from './InputArea';
import useMessageStreaming from '@/hooks/useMessageStreaming';
import api from '@/services/api';
import axios from 'axios';
import Icon from '../Icons/WelcomeIcons';
import '@/styles/animations.css';
import UploadModal from '../Modal/UploadModal';
import storage from '@/utils/storage';

const ChatContainer: React.FC = () => {
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const messages = useChatStore((state) => state.messages);
  const isLoading = useChatStore((state) => state.isLoading);
  const [inputValue, setInputValue] = useState('');
  const [welcomeContent, setWelcomeContent] = useState<any>(null);
  const [showUploadModal, setShowUploadModal] = useState(false);
  const setUploadedFiles = useChatStore((s) => s.setUploadedFiles);

  const { sendMessage } = useMessageStreaming();
  const updateSession = useChatStore((state) => state.updateSession);
  
  // Auto-scroll to bottom when new messages arrive
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  // Get dynamic greeting based on time of day
  const getGreeting = () => {
    const hour = new Date().getHours();
    if (hour >= 5 && hour < 12) return "Good morning";
    if (hour >= 12 && hour < 17) return "Good afternoon";
    if (hour >= 17 && hour < 22) return "Good evening";
    return "Hello";
  };

  // Fetch welcome content dynamically
  useEffect(() => {
    (async () => {
      try {
        // Try to fetch dynamic welcome content
        const welcomeRes = await axios.get('/get_welcome_content');
        if (welcomeRes.data) {
          // Limit capabilities to first 3 items only
          const caps = welcomeRes.data.capabilities || [];
          const limitedCaps = caps.slice(0, 3);

          setWelcomeContent({
            ...welcomeRes.data,
            greeting: getGreeting(), // Override backend greeting with client-side time
            capabilities: limitedCaps
          });
        }
      } catch {
        // Fallback to default welcome content
        setWelcomeContent({
          greeting: getGreeting(),
          title: "Welcome to ChatMRPT",
          subtitle: "Your AI assistant for malaria intervention planning",
          capabilities: [
            "Calculate Test Positivity Rates from your data",
            "Analyze malaria risk and create vulnerability maps",
            "Plan ITN distribution based on evidence"
          ]
        });
      }
    })();
  }, []);

  // Open upload modal on request (from Quick Start modal)
  useEffect(() => {
    try {
      if (localStorage.getItem('chatmrpt_open_upload_modal') === '1') {
        localStorage.removeItem('chatmrpt_open_upload_modal');
        setShowUploadModal(true);
      }
    } catch {}
  }, []);

  // On mount, align frontend sessionId with backend session
  useEffect(() => {
    (async () => {
      try {
        const res = await api.session.getInfo();
        const backendSessionId = res.data?.session_id;
        if (backendSessionId) {
          updateSession({ sessionId: backendSessionId });
          storage.setSessionId(backendSessionId);
        }
      } catch {
        // ignore; fallback to existing client session id
      }
    })();
  }, [updateSession]);
  
  const handleSendMessage = async () => {
    if (!inputValue.trim() || isLoading) return;
    await sendMessage(inputValue);
    setInputValue(''); // Clear input after sending
  };
  
  return (
    <div className="flex flex-col h-full bg-white dark:bg-dark-bg">
      {/* Messages Area */}
      <div className="flex-1 overflow-y-auto bg-gray-50 dark:bg-dark-bg animated-gradient">
        {messages.length === 0 ? (
          <div className="flex items-center justify-center h-full p-8">
            <div className="max-w-4xl w-full">

              {/* Dynamic Greeting */}
              <div className="text-center mb-16">
                <p className="text-lg text-gray-500 dark:text-dark-text-secondary font-light mb-3">
                  {welcomeContent?.greeting || getGreeting()}
                </p>

                <h1 className="text-5xl font-light text-gray-900 dark:text-dark-text mb-4">
                  {welcomeContent?.title || "Welcome to ChatMRPT"}
                </h1>

                <p className="text-lg text-gray-600 dark:text-dark-text-secondary max-w-2xl mx-auto font-light">
                  {welcomeContent?.subtitle || "Your AI assistant for malaria intervention planning"}
                </p>
              </div>

              {/* What ChatMRPT Does - Clean Bullet Points */}
              <div className="max-w-2xl mx-auto mb-12">
                <p className="text-base font-medium text-gray-700 dark:text-dark-text mb-4">
                  ChatMRPT helps you:
                </p>
                <ul className="space-y-3">
                  {(welcomeContent?.capabilities || [
                    "Calculate Test Positivity Rates from your data",
                    "Analyze malaria risk and create vulnerability maps",
                    "Plan ITN distribution based on evidence"
                  ]).map((capability: any, index: number) => {
                    // Handle both string and object formats
                    const text = typeof capability === 'string' ? capability : capability.title;
                    return (
                      <li key={index} className="flex items-start text-gray-600 dark:text-dark-text-secondary">
                        <span className="text-gray-900 dark:text-dark-text mr-3 mt-1">•</span>
                        <span className="text-base font-light">{text}</span>
                      </li>
                    );
                  })}
                </ul>
              </div>

              {/* Call to Action */}
              <div className="text-center mb-8">
                <p className="text-base text-gray-600 dark:text-dark-text-secondary mb-6 font-light">
                  To begin, upload your malaria data (CSV file)
                </p>
                <button
                  onClick={() => setShowUploadModal(true)}
                  className="px-8 py-3 bg-blue-600 text-white rounded-lg font-medium text-base hover:bg-blue-700 transition-colors shadow-sm"
                >
                  Upload Data
                </button>
              </div>

              {/* Secondary Action */}
              <div className="text-center mt-8 pt-8 border-t border-gray-200 dark:border-dark-border max-w-2xl mx-auto">
                <p className="text-sm text-gray-500 dark:text-dark-text-secondary font-light">
                  Or ask me anything about malaria analysis
                </p>
              </div>
            </div>
          </div>
        ) : (
          <div className="px-4 py-6">
            <MessageList messages={messages} />
            <div ref={messagesEndRef} />
          </div>
        )}
      </div>
      
      {/* Input Area */}
      <div className="flex-shrink-0 border-t border-gray-200 dark:border-dark-border sticky bottom-0 z-40 bg-white dark:bg-dark-bg-secondary">
        <InputArea 
          value={inputValue}
          onChange={setInputValue}
          onSend={handleSendMessage} 
          isLoading={isLoading} 
        />
      </div>

      {/* Upload Modal (global) */}
      <UploadModal isOpen={showUploadModal} onClose={() => setShowUploadModal(false)} />
    </div>
  );
};

export default ChatContainer;
