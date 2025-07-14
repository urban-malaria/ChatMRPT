// Session types
export interface SessionState {
  sessionId: string;
  csvLoaded: boolean;
  shapefileLoaded: boolean;
  analysisComplete: boolean;
  dataLoaded: boolean;
  csvFilename?: string;
  shapefileFilename?: string;
  csvRows?: number;
  csvColumns?: number;
  shapefileFeatures?: number;
  availableVariables?: string[];
  currentLanguage: string;
}

// Analysis types
export interface AnalysisMethod {
  id: string;
  name: string;
  description: string;
  variables?: string[];
}

// Visualization types
export interface Visualization {
  id: string;
  type: 'map' | 'chart' | 'table';
  title: string;
  url?: string;
  data?: any;
  config?: any;
}

// Report types
export type ReportFormat = 'pdf' | 'html' | 'markdown';

export interface ReportConfig {
  format: ReportFormat;
  sections?: string[];
  includeCharts?: boolean;
  includeMaps?: boolean;
}

// Theme types
export type Theme = 'light' | 'dark';

// Error types
export interface ApiError {
  status: number;
  message: string;
  details?: any;
}