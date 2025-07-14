import React, { useState } from 'react';
import Modal from '../common/Modal';
import { Tab } from '@headlessui/react';
import StandardUpload from './StandardUpload';
import TPRUpload from './TPRUpload';
import DownloadData from './DownloadData';
import { 
  DocumentIcon, 
  ChartBarIcon, 
  CloudArrowDownIcon 
} from '@heroicons/react/24/outline';

interface FileUploadModalProps {
  isOpen: boolean;
  onClose: () => void;
}

const FileUploadModal: React.FC<FileUploadModalProps> = ({ isOpen, onClose }) => {
  const [selectedIndex, setSelectedIndex] = useState(0);

  const tabs = [
    {
      name: 'Standard Upload',
      icon: DocumentIcon,
      component: StandardUpload,
    },
    {
      name: 'TPR Data Upload',
      icon: ChartBarIcon,
      component: TPRUpload,
    },
    {
      name: 'Download Data',
      icon: CloudArrowDownIcon,
      component: DownloadData,
    },
  ];

  const handleClose = () => {
    setSelectedIndex(0); // Reset to first tab
    onClose();
  };

  return (
    <Modal
      isOpen={isOpen}
      onClose={handleClose}
      title="Upload Your Data"
      size="xl"
    >
      <Tab.Group selectedIndex={selectedIndex} onChange={setSelectedIndex}>
        <Tab.List className="flex space-x-1 rounded-xl bg-gray-100 dark:bg-gray-700 p-1">
          {tabs.map((tab, index) => (
            <Tab
              key={tab.name}
              className={({ selected }) =>
                `w-full rounded-lg py-2.5 text-sm font-medium leading-5 
                ${
                  selected
                    ? 'bg-white dark:bg-gray-800 text-blue-700 dark:text-blue-400 shadow'
                    : 'text-gray-700 dark:text-gray-400 hover:bg-white/[0.12] hover:text-gray-900 dark:hover:text-white'
                }`
              }
            >
              <div className="flex items-center justify-center space-x-2">
                <tab.icon className="h-5 w-5" />
                <span>{tab.name}</span>
              </div>
            </Tab>
          ))}
        </Tab.List>
        
        <Tab.Panels className="mt-4">
          {tabs.map((tab, idx) => (
            <Tab.Panel
              key={idx}
              className="rounded-xl bg-white dark:bg-gray-800 p-3"
            >
              <tab.component onClose={handleClose} />
            </Tab.Panel>
          ))}
        </Tab.Panels>
      </Tab.Group>
    </Modal>
  );
};

export default FileUploadModal;