import React, { useEffect } from 'react';
import { useConversationHistoryStore } from '@/stores/conversationHistoryStore';
import { useChatStore } from '@/stores/chatStore';
import api from '@/services/api';
import storage from '@/utils/storage';
import toast from 'react-hot-toast';
import type { ConversationSummary } from '@/types';
import { useAnalysisStore } from '@/stores/analysisStore';

/** Bucket conversations by relative date. */
function groupByDate(conversations: ConversationSummary[]) {
  const now = Date.now();
  const DAY = 86_400_000;
  const todayStart = new Date().setHours(0, 0, 0, 0);
  const yesterdayStart = todayStart - DAY;
  const weekStart = todayStart - 7 * DAY;

  const groups: { label: string; items: ConversationSummary[] }[] = [
    { label: 'Today', items: [] },
    { label: 'Yesterday', items: [] },
    { label: 'Previous 7 days', items: [] },
    { label: 'Older', items: [] },
  ];

  for (const conv of conversations) {
    const ts = parseFloat(conv.last_activity) * 1000; // unix seconds -> ms
    if (ts >= todayStart) groups[0].items.push(conv);
    else if (ts >= yesterdayStart) groups[1].items.push(conv);
    else if (ts >= weekStart) groups[2].items.push(conv);
    else groups[3].items.push(conv);
  }

  return groups.filter((g) => g.items.length > 0);
}

/** Format a unix-seconds timestamp as a short relative string. */
function relativeTime(unixSeconds: string): string {
  const diffMs = Date.now() - parseFloat(unixSeconds) * 1000;
  const minutes = Math.floor(diffMs / 60_000);
  if (minutes < 1) return 'just now';
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 7) return `${days}d ago`;
  return new Date(parseFloat(unixSeconds) * 1000).toLocaleDateString();
}

const ConversationList: React.FC = () => {
  const { conversations, isLoading, activeSessionId, fetchConversations, setActiveSessionId } =
    useConversationHistoryStore();
  const clearMessages = useChatStore((s) => s.clearMessages);
  const updateSession = useChatStore((s) => s.updateSession);
  const addMessage = useChatStore((s) => s.addMessage);
  const setDataAnalysisMode = useAnalysisStore((s) => s.setDataAnalysisMode);
  const clearAnalysisResults = useAnalysisStore((s) => s.clearAnalysisResults);

  // Fetch on mount
  useEffect(() => {
    fetchConversations();
  }, [fetchConversations]);

  const handleResume = async (sessionId: string) => {
    if (sessionId === activeSessionId) return;

    try {
      const response = await api.conversations.resume(sessionId);
      if (!response.data.success) {
        toast.error(response.data.message || 'Could not resume conversation');
        return;
      }

      const { messages, session_state } = response.data;
      // Backend returns the effective session_id (upload child if exists, else base).
      // Use this for API calls so the backend can find uploaded files.
      const effectiveSessionId = response.data.session_id || sessionId;

      // Clear current chat state
      clearMessages();

      // Set analysis mode based on whether TPR workflow was active in this session
      setDataAnalysisMode(session_state.tpr_active ?? false);
      clearAnalysisResults();

      // Update session info — use the effective session_id for API calls
      updateSession({
        sessionId: effectiveSessionId,
        hasUploadedFiles: session_state.has_files ?? false,
        uploadedFiles: {
          csv: session_state.csv_filename ?? undefined,
          shapefile: session_state.shapefile_filename ?? undefined,
        },
        preserveMessages: true, // Don't double-clear
      });

      // Update browser-side session tracking
      storage.setSessionId(effectiveSessionId);

      // Replay messages into the chat store (including visualizations)
      for (const msg of messages) {
        addMessage({
          id: `restored_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
          type: 'regular',
          sender: msg.type === 'user' ? 'user' : 'assistant',
          content: msg.content,
          timestamp: new Date(msg.timestamp),
          sessionId,
          ...(msg.visualizations?.length ? { visualizations: msg.visualizations } : {}),
        });
      }

      setActiveSessionId(sessionId);
      toast.success('Conversation restored');
    } catch (error) {
      console.error('Resume failed:', error);
      toast.error('Failed to resume conversation');
    }
  };

  // Loading state
  if (isLoading && conversations.length === 0) {
    return (
      <div className="flex justify-center py-8">
        <svg className="animate-spin h-5 w-5 text-gray-400" fill="none" viewBox="0 0 24 24">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
        </svg>
      </div>
    );
  }

  // Empty state
  if (conversations.length === 0) {
    return (
      <p className="text-sm text-gray-500 dark:text-dark-text-secondary text-center py-8">
        No conversations yet
      </p>
    );
  }

  const groups = groupByDate(conversations);

  return (
    <div className="space-y-4">
      {groups.map((group) => (
        <div key={group.label}>
          <h4 className="text-xs uppercase tracking-wider text-gray-500 dark:text-dark-text-secondary font-medium mb-1.5 px-1">
            {group.label}
          </h4>
          <div className="space-y-0.5">
            {group.items.map((conv) => {
              const isActive = conv.session_id === activeSessionId;
              return (
                <button
                  key={conv.session_id}
                  onClick={() => handleResume(conv.session_id)}
                  className={`w-full text-left px-3 py-2.5 rounded-lg transition-colors duration-150 ${
                    isActive
                      ? 'bg-blue-50 dark:bg-blue-900/20 border-l-2 border-blue-500'
                      : 'hover:bg-gray-100 dark:hover:bg-dark-bg-tertiary'
                  }`}
                >
                  <p className="text-sm font-medium text-gray-900 dark:text-dark-text truncate">
                    {conv.title || 'New conversation'}
                  </p>
                  {conv.preview && (
                    <p className="text-xs text-gray-500 dark:text-dark-text-secondary truncate mt-0.5">
                      {conv.preview}
                    </p>
                  )}
                  <span className="text-xs text-gray-400 dark:text-gray-500">
                    {relativeTime(conv.last_activity)}
                    {conv.has_files === '1' && ' \u00b7 files attached'}
                  </span>
                </button>
              );
            })}
          </div>
        </div>
      ))}
    </div>
  );
};

export default ConversationList;
