import React from 'react';
import { CloudArrowDownIcon } from '@heroicons/react/24/outline';

interface DownloadDataProps {
  onClose: () => void;
}

const DownloadData: React.FC<DownloadDataProps> = ({ onClose }) => {
  return (
    <div className="space-y-6">
      <div className="text-center py-8">
        <CloudArrowDownIcon className="h-12 w-12 text-gray-400 mx-auto mb-4" />
        <h3 className="text-lg font-medium text-gray-900 dark:text-white mb-2">
          Download Processed Data
        </h3>
        <p className="text-sm text-gray-600 dark:text-gray-400 max-w-md mx-auto">
          After uploading and processing your data, you can download the enhanced datasets here.
        </p>
      </div>

      <div className="text-center">
        <p className="text-gray-500 dark:text-gray-400">
          No processed data available yet. Upload data first to enable downloads.
        </p>
      </div>
    </div>
  );
};

export default DownloadData;