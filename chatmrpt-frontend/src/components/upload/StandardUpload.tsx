import React, { useState, useCallback } from 'react';
import { useDropzone } from 'react-dropzone';
import { 
  DocumentTextIcon, 
  MapIcon, 
  CloudArrowUpIcon,
  CheckCircleIcon,
  XCircleIcon,
  ArrowUpTrayIcon,
  BeakerIcon
} from '@heroicons/react/24/outline';
import { useFileUpload } from '../../hooks/useFileUpload';
import { useApp } from '../../store/AppContext';

interface StandardUploadProps {
  onClose: () => void;
}

const StandardUpload: React.FC<StandardUploadProps> = ({ onClose }) => {
  const [csvFile, setCsvFile] = useState<File | null>(null);
  const [shapeFile, setShapeFile] = useState<File | null>(null);
  const { uploadFiles, uploadSampleData, isUploading, uploadProgress } = useFileUpload();
  const { dispatch } = useApp();

  const onDropCsv = useCallback((acceptedFiles: File[]) => {
    if (acceptedFiles.length > 0) {
      setCsvFile(acceptedFiles[0]);
    }
  }, []);

  const onDropShape = useCallback((acceptedFiles: File[]) => {
    if (acceptedFiles.length > 0) {
      setShapeFile(acceptedFiles[0]);
    }
  }, []);

  const { getRootProps: getCsvRootProps, getInputProps: getCsvInputProps, isDragActive: isCsvDragActive } = useDropzone({
    onDrop: onDropCsv,
    accept: {
      'text/csv': ['.csv'],
      'application/vnd.ms-excel': ['.xls'],
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx']
    },
    multiple: false,
    disabled: isUploading
  });

  const { getRootProps: getShapeRootProps, getInputProps: getShapeInputProps, isDragActive: isShapeDragActive } = useDropzone({
    onDrop: onDropShape,
    accept: {
      'application/zip': ['.zip']
    },
    multiple: false,
    disabled: isUploading
  });

  const handleUpload = async () => {
    if (!csvFile && !shapeFile) {
      dispatch({ type: 'SET_ERROR', payload: 'Please select at least one file to upload' });
      return;
    }

    const result = await uploadFiles(csvFile, shapeFile);
    if (result.success) {
      onClose();
    }
  };

  const handleSampleData = async () => {
    const result = await uploadSampleData();
    if (result.success) {
      onClose();
    }
  };

  return (
    <div className="space-y-6">
      <p className="text-sm text-gray-600 dark:text-gray-400">
        Please provide both your CSV/Excel data and the corresponding Shapefile (ZIP).
      </p>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* CSV Upload */}
        <div>
          <h4 className="text-sm font-medium text-gray-900 dark:text-white mb-2 flex items-center">
            <DocumentTextIcon className="h-5 w-5 mr-2 text-green-600" />
            CSV / Excel Data
          </h4>
          <div
            {...getCsvRootProps()}
            data-testid="csv-dropzone"
            className={`border-2 border-dashed rounded-lg p-6 text-center cursor-pointer transition-colors
              ${isCsvDragActive ? 'border-gray-400 bg-gray-50 dark:bg-gray-800' : 'border-gray-300 dark:border-gray-600'}
              ${csvFile ? 'bg-green-50 dark:bg-green-900/20 border-green-400' : ''}
              ${isUploading ? 'opacity-50 cursor-not-allowed' : 'hover:border-gray-400 dark:hover:border-gray-500'}
            `}
          >
            <input {...getCsvInputProps()} />
            {csvFile ? (
              <div className="space-y-2">
                <CheckCircleIcon className="h-8 w-8 mx-auto text-green-600" />
                <p className="text-sm font-medium text-gray-900 dark:text-white">{csvFile.name}</p>
                <p className="text-xs text-gray-500">
                  {(csvFile.size / 1024 / 1024).toFixed(2)} MB
                </p>
              </div>
            ) : (
              <div className="space-y-2">
                <CloudArrowUpIcon className="h-8 w-8 mx-auto text-gray-400" />
                <p className="text-sm text-gray-600 dark:text-gray-400">
                  Drop CSV/Excel file here or click to browse
                </p>
                <p className="text-xs text-gray-500">
                  Ward-level data (environmental factors, demographics)
                </p>
              </div>
            )}
          </div>
        </div>

        {/* Shapefile Upload */}
        <div>
          <h4 className="text-sm font-medium text-gray-900 dark:text-white mb-2 flex items-center">
            <MapIcon className="h-5 w-5 mr-2 text-blue-600" />
            Shapefile (ZIP)
          </h4>
          <div
            {...getShapeRootProps()}
            data-testid="shapefile-dropzone"
            className={`border-2 border-dashed rounded-lg p-6 text-center cursor-pointer transition-colors
              ${isShapeDragActive ? 'border-gray-400 bg-gray-50 dark:bg-gray-800' : 'border-gray-300 dark:border-gray-600'}
              ${shapeFile ? 'bg-green-50 dark:bg-green-900/20 border-green-400' : ''}
              ${isUploading ? 'opacity-50 cursor-not-allowed' : 'hover:border-gray-400 dark:hover:border-gray-500'}
            `}
          >
            <input {...getShapeInputProps()} />
            {shapeFile ? (
              <div className="space-y-2">
                <CheckCircleIcon className="h-8 w-8 mx-auto text-green-600" />
                <p className="text-sm font-medium text-gray-900 dark:text-white">{shapeFile.name}</p>
                <p className="text-xs text-gray-500">
                  {(shapeFile.size / 1024 / 1024).toFixed(2)} MB
                </p>
              </div>
            ) : (
              <div className="space-y-2">
                <CloudArrowUpIcon className="h-8 w-8 mx-auto text-gray-400" />
                <p className="text-sm text-gray-600 dark:text-gray-400">
                  Drop shapefile ZIP here or click to browse
                </p>
                <p className="text-xs text-gray-500">
                  Geographical boundaries of wards
                </p>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Upload Progress */}
      {isUploading && uploadProgress > 0 && (
        <div className="space-y-2">
          <div className="flex justify-between text-sm">
            <span className="text-gray-600 dark:text-gray-400">Uploading...</span>
            <span className="text-gray-900 dark:text-white">{uploadProgress}%</span>
          </div>
          <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2">
            <div 
              className="bg-blue-600 h-2 rounded-full transition-all duration-300"
              style={{ width: `${uploadProgress}%` }}
            />
          </div>
        </div>
      )}

      {/* Action Buttons */}
      <div className="flex flex-col space-y-3">
        <button
          onClick={handleUpload}
          disabled={(!csvFile && !shapeFile) || isUploading}
          className="w-full flex items-center justify-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          <ArrowUpTrayIcon className="h-5 w-5 mr-2" />
          {isUploading ? 'Uploading...' : 'Upload Files'}
        </button>

        <div className="relative">
          <div className="absolute inset-0 flex items-center">
            <div className="w-full border-t border-gray-300 dark:border-gray-600" />
          </div>
          <div className="relative flex justify-center text-sm">
            <span className="px-2 bg-white dark:bg-gray-800 text-gray-500">Or</span>
          </div>
        </div>

        <button
          onClick={handleSampleData}
          disabled={isUploading}
          className="w-full flex items-center justify-center px-4 py-2 border border-gray-300 dark:border-gray-600 text-sm font-medium rounded-md text-gray-700 dark:text-gray-300 bg-white dark:bg-gray-700 hover:bg-gray-50 dark:hover:bg-gray-600 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          <BeakerIcon className="h-5 w-5 mr-2" />
          Load Sample Data
        </button>
      </div>
    </div>
  );
};

export default StandardUpload;