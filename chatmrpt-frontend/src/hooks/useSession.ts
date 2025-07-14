import { useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useApp } from '../store/AppContext';
import chatService from '../services/chatService';
import { v4 as uuidv4 } from 'uuid';

export const useSession = () => {
  const { state, dispatch } = useApp();

  // Initialize session on mount
  useEffect(() => {
    if (!state.session.sessionId) {
      const newSessionId = uuidv4();
      dispatch({ 
        type: 'SET_SESSION', 
        payload: { sessionId: newSessionId } 
      });
    }
  }, [state.session.sessionId, dispatch]);

  // Query session info from backend
  const { data: sessionInfo, refetch: refetchSession } = useQuery({
    queryKey: ['session', state.session.sessionId],
    queryFn: () => chatService.getSessionInfo(),
    enabled: !!state.session.sessionId,
    refetchInterval: 30000, // Refetch every 30 seconds
  });

  // Update local state when session info changes
  useEffect(() => {
    if (sessionInfo) {
      dispatch({
        type: 'SET_SESSION',
        payload: {
          csvLoaded: sessionInfo.csv_loaded || false,
          shapefileLoaded: sessionInfo.shapefile_loaded || false,
          analysisComplete: sessionInfo.analysis_complete || false,
          dataLoaded: sessionInfo.data_loaded || false,
          csvFilename: sessionInfo.csv_filename,
          shapefileFilename: sessionInfo.shapefile_filename,
          csvRows: sessionInfo.csv_rows,
          csvColumns: sessionInfo.csv_columns,
          shapefileFeatures: sessionInfo.shapefile_features,
          availableVariables: sessionInfo.available_variables,
        },
      });
    }
  }, [sessionInfo, dispatch]);

  const clearSession = async () => {
    try {
      await chatService.clearSession();
      dispatch({ type: 'CLEAR_MESSAGES' });
      dispatch({ 
        type: 'SET_SESSION', 
        payload: {
          csvLoaded: false,
          shapefileLoaded: false,
          analysisComplete: false,
          dataLoaded: false,
          csvFilename: undefined,
          shapefileFilename: undefined,
          csvRows: undefined,
          csvColumns: undefined,
          shapefileFeatures: undefined,
          availableVariables: undefined,
        } 
      });
      refetchSession();
    } catch (error) {
      console.error('Error clearing session:', error);
      dispatch({ 
        type: 'SET_ERROR', 
        payload: 'Failed to clear session' 
      });
    }
  };

  return {
    session: state.session,
    sessionInfo,
    refetchSession,
    clearSession,
  };
};