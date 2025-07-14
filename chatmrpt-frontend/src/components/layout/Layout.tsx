import React from 'react';
import Header from './Header';
import Sidebar from './Sidebar';
import { useApp } from '../../store/AppContext';

interface LayoutProps {
  children: React.ReactNode;
}

const Layout: React.FC<LayoutProps> = ({ children }) => {
  const { state, dispatch } = useApp();

  const handleSidebarClose = () => {
    dispatch({ type: 'SET_SIDEBAR_OPEN', payload: false });
  };

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
      <Header />
      
      <Sidebar 
        isOpen={state.sidebarOpen} 
        onClose={handleSidebarClose}
      />
      
      <main className="transition-all duration-300">
        {children}
      </main>
    </div>
  );
};

export default Layout;