import { create } from 'zustand';
import { devtools, persist, createJSONStorage } from 'zustand/middleware';
import storage from '@/utils/storage';
import type {
  Message, 
  RegularMessage,
  ArenaMessage, 
  SystemMessage,
  Vote,
  SessionData,
  ModelName
} from '@/types';

interface ChatState {
  // Messages
  messages: Message[];
  isLoading: boolean;
  streamingContent: string;
  
  // Session
  session: SessionData;
  
  // Current Arena Battle (if active)
  currentArena: {
    battleId: string;
    round: number;
    isComplete: boolean;
  } | null;
  
  // Actions - Messages
  addMessage: (message: Message) => void;
  updateMessage: (id: string, updates: Partial<Message>) => void;
  updateStreamingContent: (messageId: string, content: string) => void;
  clearMessages: () => void;
  setLoading: (loading: boolean) => void;
  
  // Actions - Arena
  startArenaBattle: (battleId: string, userMessage: string, initialMatchup: ArenaMessage['currentMatchup']) => void;
  updateArenaAfterVote: (battleId: string, updates: Partial<ArenaMessage>) => void;
  completeArenaBattle: () => void;
  
  // Actions - Session
  updateSession: (updates: Partial<SessionData> & { preserveMessages?: boolean }) => void;
  incrementMessageCount: () => void;
  setUploadedFiles: (csv?: string, shapefile?: string) => void;
  resetSession: () => void;
}

export const useChatStore = create<ChatState>()(
  devtools(
    persist(
      (set) => ({
        // Initial state
        messages: [],
        isLoading: false,
        streamingContent: '',
        session: {
          sessionId: `session_${Date.now()}`,
          startTime: new Date(),
          messageCount: 0,
          hasUploadedFiles: false,
        },
        currentArena: null,
        
        // Message actions
        addMessage: (message) =>
          set((state) => ({
            messages: [...state.messages, message],
            session: {
              ...state.session,
              messageCount: state.session.messageCount + 1,
            },
          })),
        
        updateMessage: (id, updates) =>
          set((state) => ({
            messages: state.messages.map((msg) => {
              if (msg.id !== id) return msg;
              
              // Type-safe update based on message type
              if (msg.type === 'regular' && updates.type === 'regular') {
                return { ...msg, ...updates } as RegularMessage;
              } else if (msg.type === 'arena' && updates.type === 'arena') {
                return { ...msg, ...updates } as ArenaMessage;
              } else if (msg.type === 'system' && updates.type === 'system') {
                return { ...msg, ...updates } as SystemMessage;
              }
              
              return msg;
            }),
          })),
        
        updateStreamingContent: (messageId, content) =>
          set((state) => ({
            messages: state.messages.map((msg) =>
              msg.id === messageId && msg.type === 'regular'
                ? { ...msg, content }  // Update content only, preserve isStreaming state
                : msg
            ),
          })),
        
        clearMessages: () =>
          set({ 
            messages: [], 
            streamingContent: '',
            currentArena: null,
          }),
        
        setLoading: (loading) =>
          set({ isLoading: loading }),
        
        // Arena actions
        startArenaBattle: (battleId, userMessage, initialMatchup) =>
          set((state) => {
            const arenaMessage: ArenaMessage = {
              id: `arena_${Date.now()}`,
              type: 'arena',
              battleId,
              userMessage,
              round: 1,
              currentMatchup: initialMatchup,
              eliminatedModels: [],
              winnerChain: [],
              remainingModels: ['mistral:7b', 'llama3.1:8b', 'qwen3:8b'],
              modelsRevealed: false,
              isComplete: false,
              timestamp: new Date(),
              sessionId: state.session.sessionId,
            };
            
            return {
              messages: [...state.messages, arenaMessage],
              currentArena: {
                battleId,
                round: 1,
                isComplete: false,
              },
            };
          }),
        
        updateArenaAfterVote: (battleId, updates) =>
          set((state) => {
            const updatedMessages = state.messages.map((msg) => {
              if (msg.type === 'arena' && msg.battleId === battleId) {
                return { ...msg, ...updates } as ArenaMessage;
              }
              return msg;
            });
            
            // Update current arena state
            const updatedArena = state.currentArena && state.currentArena.battleId === battleId
              ? {
                  ...state.currentArena,
                  round: updates.round || state.currentArena.round,
                  isComplete: updates.isComplete || false,
                }
              : state.currentArena;
            
            return {
              messages: updatedMessages,
              currentArena: updatedArena,
            };
          }),
        
        completeArenaBattle: () =>
          set({ currentArena: null }),
        
        // Session actions
        updateSession: (updates) =>
          set((state) => {
            if (updates.sessionId) {
              storage.setSessionId(updates.sessionId);
            }

            const currentId = state.session.sessionId;
            const incomingId = updates.sessionId;
            const sessionIdSwapped = Boolean(incomingId && currentId && incomingId !== currentId);
            const wasAnonymous = Boolean(incomingId && !currentId && state.messages.length);
            const sessionChanged = sessionIdSwapped || wasAnonymous;

            const nextState: Partial<ChatState> = {
              session: {
                ...state.session,
                ...updates,
              },
            };

            if (sessionChanged && !updates.preserveMessages) {
              nextState.messages = [];
              nextState.currentArena = null;

              try {
                sessionStorage.removeItem('chat-storage');
              } catch (error) {
                console.warn('Failed to clear chat storage', error);
              }
            }

            return nextState;
          }),
        
        incrementMessageCount: () =>
          set((state) => ({
            session: {
              ...state.session,
              messageCount: state.session.messageCount + 1,
            },
          })),
        
        setUploadedFiles: (csv, shapefile) =>
          set((state) => ({
            session: {
              ...state.session,
              hasUploadedFiles: !!(csv || shapefile),
              uploadedFiles: {
                csv,
                shapefile,
              },
            },
          })),
        
        resetSession: () => {
          // Clear old conversation ID and generate new one
          storage.clearConversationId();
          const newConversationId = storage.ensureConversationId();

          // Clear persisted chat storage to start fresh
          try {
            localStorage.removeItem('chat-storage');
          } catch (e) {
            console.warn('Failed to clear chat storage:', e);
          }

          return set({
            session: {
              sessionId: `session_${Date.now()}`,
              startTime: new Date(),
              messageCount: 0,
              hasUploadedFiles: false,
              conversationId: newConversationId,
            },
            messages: [],
            currentArena: null,
          });
        },
      }),
      {
        name: 'chat-storage',
        storage: createJSONStorage(() => localStorage),
        partialize: (state) => ({
          messages: state.messages.slice(-50), // Keep last 50 messages
          session: {
            sessionId: state.session.sessionId,
            startTime: state.session.startTime,
            messageCount: state.session.messageCount,
            hasUploadedFiles: state.session.hasUploadedFiles,
            uploadedFiles: state.session.uploadedFiles,
          },
        }),
      }
    )
  )
);
