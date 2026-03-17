import axios, { type AxiosInstance } from 'axios';
import storage from '@/utils/storage';

// API base URL - uses proxy in development, direct URL in production
const API_BASE = import.meta.env.PROD 
  ? window.location.origin
  : '';

// Create axios instance with default config
const axiosInstance: AxiosInstance = axios.create({
  baseURL: API_BASE,
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
  withCredentials: true, // For session cookies
});

// Request interceptor for adding session ID
axiosInstance.interceptors.request.use(
  (config) => {
    const sessionId = storage.getSessionId();
    if (sessionId) {
      config.headers['X-Session-ID'] = sessionId;
    }
    return config;
  },
  (error) => Promise.reject(error)
);

// Response interceptor for error handling
axiosInstance.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      // Handle unauthorized
      window.location.href = '/login';
    }
    return Promise.reject(error);
  }
);

// Type definitions
export interface MessageRequest {
  message: string;
  session_id: string;
  mode?: 'normal' | 'arena' | 'data_analysis';
}

export interface AnalysisParams {
  analysis_type: string;
  variables?: string[];
  session_id: string;
}

export interface VisualizationRequest {
  type: string;
  session_id: string;
  data?: any;
}

export interface ExplainVisualizationRequest {
  session_id: string;
  viz_type?: string;
  viz_url?: string;
  viz_path?: string;
  title?: string;
  image_base64?: string;
  cache_key?: string;
  force?: boolean;
  metadata?: Record<string, unknown>;
}

export interface ExplainVisualizationResponse {
  status: 'success' | 'error';
  explanation?: string;
  message?: string;
  viz_type?: string;
  title?: string;
  cache_key?: string;
}

// API endpoints
export const api = {
  // Chat endpoints
  chat: {
    sendMessage: (data: MessageRequest) =>
      axiosInstance.post('/send_message', data),

    sendMessageStreaming: (data: MessageRequest) =>
      axiosInstance.post('/api/v1/data-analysis/chat', data),
  },
  
  // Arena mode endpoints
  arena: {
    startBattle: (message: string, sessionId: string) =>
      axiosInstance.post('/api/arena/start_battle', { message, session_id: sessionId }),
    
    getResponses: (battleId: string, sessionId: string) =>
      axiosInstance.post('/api/arena/get_responses', { battle_id: battleId, session_id: sessionId }),
    
    getResponsesStreaming: (battleId: string, sessionId: string) =>
      axiosInstance.post('/api/arena/get_responses_streaming', { battle_id: battleId, session_id: sessionId }),
    
    vote: (battleId: string, vote: string, sessionId: string) =>
      axiosInstance.post('/api/arena/vote', { battle_id: battleId, vote, session_id: sessionId }),
    
    getNextResponses: (battleId: string) =>
      axiosInstance.post('/api/arena/get_next_responses', 
        { battle_id: battleId },
        { timeout: 60000 } // 60 second timeout for model responses
      ),
    
    getLeaderboard: () =>
      axiosInstance.get('/api/arena/leaderboard'),
    
    getStatistics: () =>
      axiosInstance.get('/api/arena/statistics'),
  },
  
  // Data analysis endpoints
  analysis: {
    uploadFiles: (formData: FormData) =>
      axiosInstance.post('/api/data-analysis/upload', formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      }),
    
    runAnalysis: (params: AnalysisParams) =>
      axiosInstance.post('/run_analysis', params),
    
    getStatus: (sessionId: string) =>
      axiosInstance.get(`/api/data-analysis/status?session_id=${sessionId}`),
    
    activateMode: (sessionId: string) =>
      axiosInstance.post('/api/data-analysis/activate-mode', { session_id: sessionId }),
    
    clearMode: (sessionId: string) =>
      axiosInstance.post('/api/data-analysis/clear-mode', { session_id: sessionId }),
  },
  
  // Visualization endpoints
  visualization: {
    getVisualization: (data: VisualizationRequest) =>
      axiosInstance.post('/get_visualization', data),
    
    navigateMap: (direction: string, sessionId: string) =>
      axiosInstance.post('/navigate_composite_map', { direction, session_id: sessionId }),
    
    explainVisualization: (payload: ExplainVisualizationRequest) =>
      axiosInstance.post<ExplainVisualizationResponse>('/explain_visualization', payload),
    
    serveFile: (sessionId: string, filename: string) =>
      axiosInstance.get(`/serve_viz_file/${sessionId}/${filename}`),
  },
  
  // Upload endpoints
  upload: {
    uploadBothFiles: (formData: FormData) =>
      axiosInstance.post('/upload_both_files', formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      }),
    
    loadSampleData: (dataType: string, sessionId: string) =>
      axiosInstance.post('/load_sample_data', { data_type: dataType, session_id: sessionId }),
    
    downloadProcessedCSV: (sessionId: string) =>
      axiosInstance.get(`/api/download/processed-csv?session_id=${sessionId}`, {
        responseType: 'blob',
      }),
    
    downloadProcessedShapefile: (sessionId: string) =>
      axiosInstance.get(`/api/download/processed-shapefile?session_id=${sessionId}`, {
        responseType: 'blob',
      }),
  },
  
  // Report endpoints
  reports: {
    generateReport: (config: any) =>
      axiosInstance.post('/generate_report', config),
    
    previewReport: (config: any) =>
      axiosInstance.post('/preview_report', config),
    
    downloadReport: (filename: string) =>
      axiosInstance.get(`/download_report/${filename}`, {
        responseType: 'blob',
      }),
    
    getVariables: (sessionId: string) =>
      axiosInstance.get(`/api/variables?session_id=${sessionId}`),
    
    getWards: (sessionId: string) =>
      axiosInstance.get(`/api/wards?session_id=${sessionId}`),
  },
  
  // Conversation history endpoints
  conversations: {
    list: () =>
      axiosInstance.get('/api/conversations'),

    resume: (sessionId: string) =>
      axiosInstance.post(`/api/conversations/${sessionId}/resume`),
  },

  // Session endpoints
  session: {
    getInfo: () =>
      axiosInstance.get('/session_info'),
    
    clearSession: () =>
      axiosInstance.post('/clear_session'),
    
    reloadData: (sessionId: string) =>
      axiosInstance.post('/reload_session_data', { session_id: sessionId }),
    
    getStatus: () =>
      axiosInstance.get('/api/session/status'),
  },
  
  // System endpoints
  system: {
    getHealth: () =>
      axiosInstance.get('/ping'),
    
    getAppStatus: () =>
      axiosInstance.get('/app_status'),
  },
};

export default api;
