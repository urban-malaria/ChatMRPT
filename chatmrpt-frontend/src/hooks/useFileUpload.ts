import { useState, useCallback } from 'react';
import { useApp } from '../store/AppContext';
import { useSession } from './useSession';
import uploadService from '../services/uploadService';

export const useFileUpload = () => {
  const { dispatch } = useApp();
  const { refetchSession } = useSession();
  const [isUploading, setIsUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);

  const uploadFiles = useCallback(async (csvFile: File | null, shapeFile: File | null) => {
    setIsUploading(true);
    setUploadProgress(0);
    dispatch({ type: 'SET_ERROR', payload: null });

    try {
      // Simulate progress for demo
      const progressInterval = setInterval(() => {
        setUploadProgress(prev => {
          if (prev >= 90) {
            clearInterval(progressInterval);
            return 90;
          }
          return prev + 10;
        });
      }, 200);

      const response = await uploadService.uploadFiles(csvFile, shapeFile);
      
      clearInterval(progressInterval);
      setUploadProgress(100);

      if (response.status === 'success') {
        dispatch({ 
          type: 'SET_SESSION', 
          payload: {
            csvLoaded: response.csv_loaded || false,
            shapefileLoaded: response.shapefile_loaded || false,
            dataLoaded: (response.csv_loaded && response.shapefile_loaded) || false,
            csvFilename: response.csv_info?.filename,
            shapefileFilename: response.shapefile_info?.filename,
            csvRows: response.csv_info?.rows,
            csvColumns: response.csv_info?.columns,
            shapefileFeatures: response.shapefile_info?.features,
          }
        });

        // Show success message in chat
        const message = response.message || 'Files uploaded successfully!';
        dispatch({
          type: 'ADD_MESSAGE',
          payload: {
            id: Date.now().toString(),
            role: 'system',
            content: message,
            timestamp: new Date(),
          }
        });

        // Refresh session data
        await refetchSession();

        return { success: true };
      } else {
        throw new Error(response.message || 'Upload failed');
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Upload failed';
      dispatch({ type: 'SET_ERROR', payload: errorMessage });
      
      dispatch({
        type: 'ADD_MESSAGE',
        payload: {
          id: Date.now().toString(),
          role: 'system',
          content: `Error: ${errorMessage}`,
          timestamp: new Date(),
        }
      });

      return { success: false, error: errorMessage };
    } finally {
      setIsUploading(false);
      setUploadProgress(0);
    }
  }, [dispatch, refetchSession]);

  const uploadSampleData = useCallback(async () => {
    setIsUploading(true);
    dispatch({ type: 'SET_ERROR', payload: null });

    try {
      const response = await uploadService.loadSampleData();

      if (response.status === 'success') {
        dispatch({ 
          type: 'SET_SESSION', 
          payload: {
            csvLoaded: true,
            shapefileLoaded: true,
            dataLoaded: true,
          }
        });

        dispatch({
          type: 'ADD_MESSAGE',
          payload: {
            id: Date.now().toString(),
            role: 'system',
            content: 'Sample data loaded successfully! You can now run analysis or ask questions about the data.',
            timestamp: new Date(),
          }
        });

        await refetchSession();
        return { success: true };
      } else {
        throw new Error(response.message || 'Failed to load sample data');
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Failed to load sample data';
      dispatch({ type: 'SET_ERROR', payload: errorMessage });
      return { success: false, error: errorMessage };
    } finally {
      setIsUploading(false);
    }
  }, [dispatch, refetchSession]);

  return {
    uploadFiles,
    uploadSampleData,
    isUploading,
    uploadProgress,
  };
};