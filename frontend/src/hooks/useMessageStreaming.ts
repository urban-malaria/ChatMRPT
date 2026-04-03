import { useCallback, useRef } from 'react';
import { useChatStore } from '@/stores/chatStore';
import { useAnalysisStore } from '@/stores/analysisStore';
import type { RegularMessage, ModelName } from '@/types';
import toast from 'react-hot-toast';
import storage from '@/utils/storage';

const useMessageStreaming = () => {
  const {
    addMessage,
    updateStreamingContent,
    setLoading,
    session,
    startArenaBattle,
  } = useChatStore();

  const abortControllerRef = useRef<AbortController | null>(null);

  const stopGeneration = useCallback(() => {
    if (abortControllerRef.current) {
      console.log('🛑 Aborting generation...');
      abortControllerRef.current.abort();
      setLoading(false);
      toast('Generation stopped', { icon: '🛑' });
    }
  }, [setLoading]);
  
  const sendMessage = useCallback(async (message: string, opts?: { silent?: boolean }) => {
    // Check if we're in data analysis mode
    const dataAnalysisMode = useAnalysisStore.getState().dataAnalysisMode;
    const setDataAnalysisMode = useAnalysisStore.getState().setDataAnalysisMode;
    
    // DEBUG LOGGING
    console.log('🎯 FRONTEND: sendMessage called');
    console.log('  📝 Message:', message);
    console.log('  🔍 Data Analysis Mode:', dataAnalysisMode);
    console.log('  🆔 Session ID:', session.sessionId);
    
    // Optionally add user message (silent triggers should not appear in chat)
    if (!opts?.silent) {
      const userMessage: RegularMessage = {
        id: `msg_${Date.now()}`,
        type: 'regular',
        sender: 'user',
        content: message,
        timestamp: new Date(),
        sessionId: session.sessionId,
      };
      addMessage(userMessage);
    }
    setLoading(true);

    // Create abort controller for this request
    abortControllerRef.current = new AbortController();
    const signal = abortControllerRef.current.signal;

    try {
      // Determine endpoint based on mode
      const endpoint = dataAnalysisMode
        ? '/api/v1/data-analysis/chat'
        : '/send_message_streaming';
      
      console.log('🎯 FRONTEND: Endpoint Selection');
      console.log(`  🌐 Using endpoint: ${endpoint}`);
      console.log(`  🔍 Data Analysis Mode: ${dataAnalysisMode}`);
      
      // Use different handling for data analysis vs streaming
      if (dataAnalysisMode) {
        console.log('═══════════════════════════════════════════════════════════');
        console.log('📊 DATA ANALYSIS V3 REQUEST');
        console.log('  Message:', message.substring(0, 80));
        console.log('  Session ID:', session.sessionId);
        console.log('  Endpoint:', endpoint);
        console.log('  Timestamp:', new Date().toISOString());
        console.log('═══════════════════════════════════════════════════════════');

        // Data analysis endpoint - JSON response
        const response = await fetch(endpoint, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Conversation-ID': storage.ensureConversationId(),
          },
          body: JSON.stringify({
            message,
            session_id: session.sessionId,
          }),
          signal,
        });

        if (!response.ok) {
          console.error('❌ DATA ANALYSIS V3 ERROR:', response.status, response.statusText);
          throw new Error(`HTTP error! status: ${response.status}`);
        }

        const responseData = await response.json();
        console.log('───────────────────────────────────────────────────────────');
        console.log('✅ DATA ANALYSIS V3 RESPONSE');
        console.log('  Success:', responseData.success);
        console.log('  Visualizations:', responseData.visualizations?.length || 0);
        console.log('  Has Message:', !!responseData.message);
        console.log('  Has Redirect Message:', !!responseData.redirect_message);
        console.log('  Workflow:', responseData.workflow);
        console.log('  Stage:', responseData.stage);
        console.log('  Message Preview:', responseData.message?.substring(0, 120));
        console.log('───────────────────────────────────────────────────────────');

        // ONE-BRAIN: No mode exit — always stay in V3 agent mode after upload
        if (responseData.success && responseData.message) {
          // Add the response as an assistant message
          const assistantMessage: RegularMessage = {
            id: `msg_${Date.now() + 1}`,
            type: 'regular',
            sender: 'assistant',
            content: responseData.message,
            timestamp: new Date(),
            sessionId: session.sessionId,
            visualizations: responseData.visualizations,
          };
          addMessage(assistantMessage);
        }
        
        setLoading(false);
        return;
      }
      
      // Regular streaming endpoint
      console.log('═══════════════════════════════════════════════════════════');
      console.log('🌊 STREAMING REQUEST (Standard Mode)');
      console.log('  Message:', message.substring(0, 80));
      console.log('  Session ID:', session.sessionId);
      console.log('  Endpoint:', endpoint);
      console.log('  Data Analysis Mode:', dataAnalysisMode, '(should be FALSE)');
      console.log('  Timestamp:', new Date().toISOString());
      console.log('═══════════════════════════════════════════════════════════');

      const response = await fetch(endpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Conversation-ID': storage.ensureConversationId(),
        },
        body: JSON.stringify({
          message,
          session_id: session.sessionId,
        }),
        signal,
      });

      if (!response.ok) {
        console.error('❌ STREAMING ERROR:', response.status, response.statusText);
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      console.log('✅ Streaming response received, starting to read chunks...');
      
      const reader = response.body?.getReader();
      const decoder = new TextDecoder();
      
      if (!reader) {
        throw new Error('No response body');
      }
      
      let assistantMessageId: string | null = null;
      let streamingContent = '';
      let isArenaResponse = false;
      let buffer = '';
      let downloadLinks: any[] = [];
      let visualizations: any[] = [];
      let arenaBattleId: string | null = null;
      let arenaAccumA = '';
      let arenaAccumB = '';
      
      while (true) {
        const { done, value } = await reader.read();
        
        if (done) {
          break;
        }
        
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';
        
        for (const line of lines) {
          if (line.startsWith('data: ')) {
            try {
              const data = JSON.parse(line.slice(6));
              
              // Check if this is a clarification request
              if (data.needs_clarification) {
                // Create a clarification message
                const clarificationId = `msg_${Date.now() + 1}`;
                const clarificationMessage: any = {
                  id: clarificationId,
                  type: 'clarification',
                  sender: 'assistant',
                  content: data.message,
                  options: data.options,
                  originalMessage: data.original_message,
                  timestamp: new Date(),
                  sessionId: session.sessionId,
                };
                addMessage(clarificationMessage);
                setLoading(false);
                continue; // Skip other processing
              }
              
              // Check if this is an Arena response (backend sends arena_mode: true)
              if (data.arena_mode === true) {

                isArenaResponse = true;

                // Only initialize arena battle once per battle_id
                if (data.battle_id && !arenaBattleId) {
                  arenaBattleId = data.battle_id; // Set immediately to prevent duplicates

                  // Handle tournament-style Arena mode with complete responses
                  if (data.model_a && data.model_b) {
                    console.log('🎭 Arena Init - Response A:', data.response_a ? `${data.response_a.substring(0, 50)}...` : 'MISSING');
                    console.log('🎭 Arena Init - Response B:', data.response_b ? `${data.response_b.substring(0, 50)}...` : 'MISSING');

                    const initialMatchup = {
                      modelA: data.model_a as ModelName,
                      modelB: data.model_b as ModelName,
                      // CRITICAL: Use responses from backend, or empty string as last resort
                      responseA: data.response_a || '',
                      responseB: data.response_b || '',
                    };

                    // Warn if responses are missing
                    if (!data.response_a || !data.response_b) {
                      console.warn('⚠️ Arena responses missing from backend init event!');
                    }

                    // Basic sanity check
                    if (data.response_a === data.response_b && data.response_a) {
                      console.error('❌ Duplicate arena responses detected');
                    }

                    // Start the Arena battle with initial matchup
                    startArenaBattle(data.battle_id, message, initialMatchup);
                    setLoading(false);
                  }
                } else {
                  // already initialized for this battle_id
                }
                // Handle streaming chunk updates
                if (data.stream === true && data.battle_id && data.delta && data.side) {
                  if (data.side === 'a') arenaAccumA += data.delta;
                  if (data.side === 'b') arenaAccumB += data.delta;
                  const state = useChatStore.getState();
                  const arenaMsg = [...state.messages].reverse().find(
                    (m: any) => m.type === 'arena' && m.battleId === data.battle_id
                  ) as any;
                  if (arenaMsg) {
                    state.updateMessage(arenaMsg.id, {
                      type: 'arena',
                      currentMatchup: {
                        ...arenaMsg.currentMatchup,
                        responseA: arenaAccumA || arenaMsg.currentMatchup.responseA,
                        responseB: arenaAccumB || arenaMsg.currentMatchup.responseB,
                      },
                    } as any);
                  }
                }
                // Completion event
                if (data.arena_complete === true || data.done === true) {
                  setLoading(false);
                }
                continue; // Skip other processing for arena responses
              }
              
              // Handle regular streaming responses
              if (!assistantMessageId && !isArenaResponse) {
                // Initialize assistant message on first chunk
                assistantMessageId = `msg_${Date.now() + 1}`;
                const assistantMessage: RegularMessage = {
                  id: assistantMessageId,
                  type: 'regular',
                  sender: 'assistant',
                  content: '',
                  isStreaming: true,
                  timestamp: new Date(),
                  sessionId: session.sessionId,
                };
                addMessage(assistantMessage);
              }
              
              // Update content if we have any
              if (data.content && assistantMessageId && !isArenaResponse) {
                streamingContent += data.content;
                updateStreamingContent(assistantMessageId, streamingContent);
              }
              
              // Capture download links if present
              if (data.download_links && data.download_links.length > 0) {
                downloadLinks = data.download_links;
              }

              // Capture visualizations if present
              if (data.visualizations && data.visualizations.length > 0) {
                visualizations = data.visualizations;
                console.log('Captured visualizations from streaming:', visualizations);
              }
              
              // Check if response is complete
              if (data.done) {
                console.log('───────────────────────────────────────────────────────────');
                console.log('✅ STREAMING COMPLETE');
                console.log('  Total content length:', streamingContent.length);
                console.log('  Content preview:', streamingContent.substring(0, 100));
                console.log('  Download links:', downloadLinks.length);
                console.log('  Visualizations:', visualizations.length);
                console.log('  Status:', data.status);
                console.log('───────────────────────────────────────────────────────────');

                if (!isArenaResponse && assistantMessageId) {
                  // Finalize regular message with download links and visualizations
                  const finalMessage: RegularMessage = {
                    id: assistantMessageId,
                    type: 'regular',
                    sender: 'assistant',
                    content: streamingContent || '',
                    timestamp: new Date(),
                    sessionId: session.sessionId,
                    isStreaming: false,
                    downloadLinks: downloadLinks.length > 0 ? downloadLinks : undefined,
                    visualizations: visualizations.length > 0 ? visualizations : undefined,
                  };
                  // Update the message with download links and visualizations
                  useChatStore.getState().updateMessage(assistantMessageId, finalMessage);

                  if (visualizations.length > 0) {
                    console.log('Added visualizations to final message:', visualizations);
                  }
                }
                setLoading(false);
              }
              
            } catch (error) {
              console.error('Error parsing SSE data:', error, 'Line:', line);
            }
          }
        }
      }
      
      setLoading(false);
      
    } catch (error) {
      if (error instanceof Error && error.name === 'AbortError') {
        console.log('🛑 Request aborted by user');
        toast('Generation stopped', { icon: '🛑' });
      } else {
        console.error('Error sending message:', error);
        toast.error('Failed to send message');
      }
      setLoading(false);
    }
  }, [addMessage, setLoading, session.sessionId, startArenaBattle, updateStreamingContent]);

  return { sendMessage, stopGeneration };
};

export default useMessageStreaming;
