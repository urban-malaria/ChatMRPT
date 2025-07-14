import React, { createContext, useContext, useReducer, ReactNode } from 'react';
import { SessionState, ChatMessage, Theme } from '../utils/types';

// App state interface
export interface AppState {
  session: SessionState;
  messages: ChatMessage[];
  theme: Theme;
  isLoading: boolean;
  error: string | null;
  sidebarOpen: boolean;
}

// Action types
export type AppAction =
  | { type: 'SET_SESSION'; payload: Partial<SessionState> }
  | { type: 'ADD_MESSAGE'; payload: ChatMessage }
  | { type: 'UPDATE_MESSAGE'; payload: { id: string; content: string } }
  | { type: 'CLEAR_MESSAGES' }
  | { type: 'SET_THEME'; payload: Theme }
  | { type: 'SET_LOADING'; payload: boolean }
  | { type: 'SET_ERROR'; payload: string | null }
  | { type: 'TOGGLE_SIDEBAR' }
  | { type: 'SET_SIDEBAR_OPEN'; payload: boolean };

// Initial state
const initialState: AppState = {
  session: {
    sessionId: '',
    csvLoaded: false,
    shapefileLoaded: false,
    analysisComplete: false,
    dataLoaded: false,
    currentLanguage: 'en',
  },
  messages: [],
  theme: (localStorage.getItem('chatmrpt-theme') as Theme) || 'light',
  isLoading: false,
  error: null,
  sidebarOpen: false,
};

// Reducer
const appReducer = (state: AppState, action: AppAction): AppState => {
  switch (action.type) {
    case 'SET_SESSION':
      return {
        ...state,
        session: { ...state.session, ...action.payload },
      };
    
    case 'ADD_MESSAGE':
      return {
        ...state,
        messages: [...state.messages, action.payload],
      };
    
    case 'UPDATE_MESSAGE':
      return {
        ...state,
        messages: state.messages.map(msg =>
          msg.id === action.payload.id
            ? { ...msg, content: action.payload.content }
            : msg
        ),
      };
    
    case 'CLEAR_MESSAGES':
      return {
        ...state,
        messages: [],
      };
    
    case 'SET_THEME':
      localStorage.setItem('chatmrpt-theme', action.payload);
      return {
        ...state,
        theme: action.payload,
      };
    
    case 'SET_LOADING':
      return {
        ...state,
        isLoading: action.payload,
      };
    
    case 'SET_ERROR':
      return {
        ...state,
        error: action.payload,
      };
    
    case 'TOGGLE_SIDEBAR':
      return {
        ...state,
        sidebarOpen: !state.sidebarOpen,
      };
    
    case 'SET_SIDEBAR_OPEN':
      return {
        ...state,
        sidebarOpen: action.payload,
      };
    
    default:
      return state;
  }
};

// Context
const AppContext = createContext<{
  state: AppState;
  dispatch: React.Dispatch<AppAction>;
} | undefined>(undefined);

// Provider component
export const AppProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  const [state, dispatch] = useReducer(appReducer, initialState);

  return (
    <AppContext.Provider value={{ state, dispatch }}>
      {children}
    </AppContext.Provider>
  );
};

// Custom hook to use the app context
export const useApp = () => {
  const context = useContext(AppContext);
  if (context === undefined) {
    throw new Error('useApp must be used within an AppProvider');
  }
  return context;
};