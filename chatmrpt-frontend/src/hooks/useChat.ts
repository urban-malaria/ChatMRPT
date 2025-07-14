import { useState, useCallback } from 'react';
import { useApp } from '../store/AppContext';
import chatService from '../services/chatService';
import { ChatMessage } from '../utils/types';
import { v4 as uuidv4 } from 'uuid';

export const useChat = () => {
  const { state, dispatch } = useApp();
  const [isStreaming, setIsStreaming] = useState(false);

  const addMessage = useCallback((message: ChatMessage) => {
    dispatch({ type: 'ADD_MESSAGE', payload: message });
  }, [dispatch]);

  const updateMessage = useCallback((id: string, content: string) => {
    dispatch({ type: 'UPDATE_MESSAGE', payload: { id, content } });
  }, [dispatch]);

  const sendMessage = useCallback(async (content: string) => {
    // Add user message
    const userMessage: ChatMessage = {
      id: uuidv4(),
      role: 'user',
      content,
      timestamp: new Date(),
    };
    addMessage(userMessage);

    // Create assistant message placeholder
    const assistantMessageId = uuidv4();
    const assistantMessage: ChatMessage = {
      id: assistantMessageId,
      role: 'assistant',
      content: '',
      timestamp: new Date(),
      streaming: true,
    };
    addMessage(assistantMessage);

    setIsStreaming(true);
    dispatch({ type: 'SET_ERROR', payload: null });

    try {
      await chatService.sendMessageStreaming(
        content,
        state.session.currentLanguage,
        // On chunk callback
        (chunk) => {
          if (chunk.content) {
            updateMessage(assistantMessageId, chunk.content);
          }
        },
        // On complete callback
        (result) => {
          setIsStreaming(false);
          
          // Update the final message
          const finalContent = result.content || 'Sorry, I encountered an error processing your request.';
          updateMessage(assistantMessageId, finalContent);
          
          // Handle any additional actions
          if (result.action) {
            handleAction(result.action, result);
          }
          
          if (result.error) {
            dispatch({ type: 'SET_ERROR', payload: result.error });
          }
        }
      );
    } catch (error) {
      setIsStreaming(false);
      const errorMessage = error instanceof Error ? error.message : 'An unknown error occurred';
      updateMessage(assistantMessageId, `Error: ${errorMessage}`);
      dispatch({ type: 'SET_ERROR', payload: errorMessage });
    }
  }, [state.session.currentLanguage, addMessage, updateMessage, dispatch]);

  const handleAction = useCallback((action: string, data: any) => {
    switch (action) {
      case 'data_loaded':
        dispatch({ 
          type: 'SET_SESSION', 
          payload: { 
            csvLoaded: data.csv_loaded,
            shapefileLoaded: data.shapefile_loaded,
            dataLoaded: data.csv_loaded && data.shapefile_loaded,
          } 
        });
        break;
      
      case 'analysis_complete':
        dispatch({ 
          type: 'SET_SESSION', 
          payload: { analysisComplete: true } 
        });
        break;
      
      case 'show_visualization':
        dispatch({
          type: 'SET_VISUALIZATION_DATA',
          payload: {
            mapUrl: data.map_url,
            mapType: data.map_type || 'iframe',
            chartData: data.chart_data,
            wardRankings: data.ward_rankings,
            tableData: data.table_data,
            analysisResults: data.analysis_results,
          }
        });
        break;
      
      // Add more action handlers as needed
    }
  }, [dispatch]);

  const clearMessages = useCallback(() => {
    dispatch({ type: 'CLEAR_MESSAGES' });
  }, [dispatch]);

  return {
    messages: state.messages,
    sendMessage,
    clearMessages,
    isStreaming,
  };
};