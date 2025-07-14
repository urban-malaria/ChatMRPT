import api from './api';

export interface UploadResponse {
  status: string;
  message: string;
  csv_loaded?: boolean;
  shapefile_loaded?: boolean;
  csv_info?: {
    filename: string;
    rows: number;
    columns: number;
  };
  shapefile_info?: {
    filename: string;
    features: number;
  };
  errors?: string[];
}

export interface TPRState {
  state_name: string;
  row_count: number;
}

class UploadService {
  /**
   * Upload CSV and/or Shapefile
   */
  async uploadFiles(csvFile: File | null, shapeFile: File | null): Promise<UploadResponse> {
    const formData = new FormData();
    
    if (csvFile) {
      formData.append('csv_file', csvFile);
    }
    if (shapeFile) {
      formData.append('shapefile', shapeFile);
    }

    try {
      const response = await api.post('/upload_both_files', formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      });
      return response.data;
    } catch (error) {
      console.error('Error uploading files:', error);
      throw error;
    }
  }

  /**
   * Load sample data
   */
  async loadSampleData(): Promise<UploadResponse> {
    try {
      const response = await api.post('/load_sample_data', {});
      return response.data;
    } catch (error) {
      console.error('Error loading sample data:', error);
      throw error;
    }
  }

  /**
   * Detect states in TPR file
   */
  async detectTPRStates(file: File): Promise<{ states: TPRState[] }> {
    const formData = new FormData();
    formData.append('file', file);

    try {
      const response = await api.get('/api/tpr/detect-states', {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      });
      return response.data;
    } catch (error) {
      console.error('Error detecting TPR states:', error);
      throw error;
    }
  }

  /**
   * Process TPR file
   */
  async processTPR(file: File, stateName: string): Promise<any> {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('state_name', stateName);

    try {
      const response = await api.post('/api/tpr/process', formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      });
      return response.data;
    } catch (error) {
      console.error('Error processing TPR:', error);
      throw error;
    }
  }

  /**
   * Download processed CSV
   */
  async downloadProcessedCSV(): Promise<Blob> {
    try {
      const response = await api.get('/api/download/processed-csv', {
        responseType: 'blob',
      });
      return response.data;
    } catch (error) {
      console.error('Error downloading CSV:', error);
      throw error;
    }
  }

  /**
   * Download processed shapefile
   */
  async downloadProcessedShapefile(): Promise<Blob> {
    try {
      const response = await api.get('/api/download/processed-shapefile', {
        responseType: 'blob',
      });
      return response.data;
    } catch (error) {
      console.error('Error downloading shapefile:', error);
      throw error;
    }
  }
}

export default new UploadService();