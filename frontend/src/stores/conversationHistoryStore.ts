import { create } from 'zustand';
import api from '@/services/api';
import type { ConversationSummary } from '@/types';

interface ConversationHistoryState {
  conversations: ConversationSummary[];
  isLoading: boolean;
  activeSessionId: string | null;

  fetchConversations: () => Promise<void>;
  setActiveSessionId: (id: string | null) => void;
}

export const useConversationHistoryStore = create<ConversationHistoryState>()(
  (set) => ({
    conversations: [],
    isLoading: false,
    activeSessionId: null,

    fetchConversations: async () => {
      set({ isLoading: true });
      try {
        const response = await api.conversations.list();
        if (response.data.success) {
          set({ conversations: response.data.conversations ?? [] });
        }
      } catch (error) {
        // Silently fail — sidebar just shows empty list
        console.debug('Could not fetch conversations:', error);
      } finally {
        set({ isLoading: false });
      }
    },

    setActiveSessionId: (id) => set({ activeSessionId: id }),
  })
);
