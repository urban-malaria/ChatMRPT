import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { Toaster } from 'react-hot-toast';
import { useEffect } from 'react';
import toast from 'react-hot-toast';
import { ThemeProvider } from './contexts/ThemeContext';
import MainInterface from './components/MainInterface';
import LandingPage from './components/Auth/LandingPage';
import { authService } from './services/auth';
import { useUserStore } from './stores/userStore';
import storage from '@/utils/storage';

// Create a client
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 5 * 60 * 1000, // 5 minutes
      retry: 1,
    },
  },
});

function App() {
  const { isAuthenticated, isLoading, user } = useUserStore();

  console.log('🔵 APP: Render - isAuthenticated:', isAuthenticated, 'isLoading:', isLoading, 'user:', user);

  // Check authentication status on app load
  useEffect(() => {
    console.log('🔵 APP: useEffect triggered');

    // Ensure conversation ID exists and sync to URL
    const conversationId = storage.ensureConversationId();
    const urlParams = new URLSearchParams(window.location.search);
    const token = urlParams.get('token');
    const username = urlParams.get('user');

    // Check if conversation_id is already in URL, if not add it
    const urlConvId = urlParams.get('conversation_id');
    if (!urlConvId && conversationId && !token) {
      // Add conversation_id to URL without reloading
      const newUrl = new URL(window.location.href);
      newUrl.searchParams.set('conversation_id', conversationId);
      window.history.replaceState({}, document.title, newUrl.toString());
      console.log('🔵 APP: Added conversation_id to URL:', conversationId);
    } else if (urlConvId && urlConvId !== conversationId) {
      // URL has a different conversation_id, use it (for sharing/bookmarking)
      storage.setConversationId(urlConvId);
      console.log('🔵 APP: Using conversation_id from URL:', urlConvId);
    }

    const runAuthCheck = async () => {
      try {
        if (token) {
          console.log('🔵 APP: OAuth callback detected - storing token');
          storage.setAuthToken(token);
        }

        const success = await authService.checkAuth();
        console.log('🔵 APP: checkAuth result:', success);

        if (success && token && username) {
          toast.success(`Welcome back, ${username}`);
        }
      } finally {
        if (token || username) {
          // After OAuth, preserve conversation_id but remove token/user params
          const cleanUrl = new URL(window.location.href);
          cleanUrl.searchParams.delete('token');
          cleanUrl.searchParams.delete('user');
          // Ensure conversation_id is still in URL
          if (!cleanUrl.searchParams.get('conversation_id')) {
            cleanUrl.searchParams.set('conversation_id', storage.ensureConversationId());
          }
          window.history.replaceState({}, document.title, cleanUrl.toString());
        }
      }
    };

    runAuthCheck();
  }, []);

  console.log('🔵 APP: Rendering UI - isLoading:', isLoading, 'isAuthenticated:', isAuthenticated);

  return (
    <QueryClientProvider client={queryClient}>
      <ThemeProvider>
        {/* Show loading spinner during initial auth check */}
        {isLoading ? (
          (() => {
            console.log('🔵 APP: Showing loading spinner');
            return (
              <div className="min-h-screen flex items-center justify-center bg-white">
                <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-gray-900"></div>
              </div>
            );
          })()
        ) : isAuthenticated ? (
          (() => {
            console.log('🔵 APP: Showing main interface (authenticated)');
            return <MainInterface />;
          })()
        ) : (
          (() => {
            console.log('🔵 APP: Showing landing page (not authenticated)');
            return <LandingPage />;
          })()
        )}
      
      {/* Toast notifications */}
      <Toaster
        position="top-right"
        toastOptions={{
          duration: 4000,
          style: {
            background: '#363636',
            color: '#fff',
          },
          success: {
            style: {
              background: '#10b981',
            },
          },
          error: {
            style: {
              background: '#ef4444',
            },
          },
        }}
      />
      </ThemeProvider>
    </QueryClientProvider>
  );
}

export default App;
