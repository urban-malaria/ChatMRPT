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
    <div className="relative flex h-screen bg-white dark:bg-[#212121]">
      <Header />
      <Sidebar 
        isOpen={state.sidebarOpen} 
        onClose={handleSidebarClose}
      />
      
      <div className="flex flex-col flex-1 overflow-hidden bg-white dark:bg-[#212121] pt-14">
        <main className="flex-1 relative">
          {children}
        </main>
      </div>
    </div>
  );
};

export default Layout;