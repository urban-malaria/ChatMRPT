import React from 'react';
import { ExclamationTriangleIcon } from '@heroicons/react/24/outline';

interface TPRUploadProps {
  onClose: () => void;
}

const TPRUpload: React.FC<TPRUploadProps> = ({ onClose }) => {
  return (
    <div className="space-y-6">
      <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
        <div className="flex">
          <ExclamationTriangleIcon className="h-5 w-5 text-blue-600 mr-2 flex-shrink-0 mt-0.5" />
          <div>
            <h4 className="text-sm font-medium text-blue-900 dark:text-blue-100">
              Test Positivity Rate (TPR) Upload
            </h4>
            <p className="text-sm text-blue-700 dark:text-blue-300 mt-1">
              Upload your TPR Excel file. The system will automatically extract environmental data and create analysis-ready datasets.
            </p>
          </div>
        </div>
      </div>

      <div className="text-center py-12">
        <p className="text-gray-500 dark:text-gray-400">
          TPR upload functionality coming soon...
        </p>
      </div>
    </div>
  );
};

export default TPRUpload;