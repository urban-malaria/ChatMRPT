import React, { useState, useEffect } from 'react';
import Modal from '../common/Modal';
import { Tab } from '@headlessui/react';
import UniversalMapViewer from './UniversalMapViewer';
import ChartVisualization from './ChartVisualization';
import DataTableView from './DataTableView';
import { 
  MapIcon, 
  ChartBarIcon,
  TableCellsIcon
} from '@heroicons/react/24/outline';

interface VisualizationModalProps {
  isOpen: boolean;
  onClose: () => void;
  visualizationData?: any;
  mapUrl?: string;
  mapType?: 'leaflet' | 'folium' | 'plotly' | 'kepler' | 'iframe';
}

const VisualizationModal: React.FC<VisualizationModalProps> = ({ 
  isOpen, 
  onClose,
  visualizationData,
  mapUrl,
  mapType = 'iframe'
}) => {
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [availableTabs, setAvailableTabs] = useState<any[]>([]);

  useEffect(() => {
    // Dynamically set available tabs based on data
    const tabs = [];
    
    // Always add map view if we have map data or URL
    if (mapUrl || visualizationData?.mapData) {
      tabs.push({
        name: 'Map View',
        icon: MapIcon,
        component: () => (
          <UniversalMapViewer 
            mapData={visualizationData?.mapData}
            mapUrl={mapUrl}
            mapType={mapType}
          />
        ),
      });
    }

    // Add charts if we have chart data
    if (visualizationData?.chartData || visualizationData?.wardRankings) {
      tabs.push({
        name: 'Charts',
        icon: ChartBarIcon,
        component: () => <ChartVisualization data={visualizationData} />,
      });
    }

    // Add data table if we have tabular data
    if (visualizationData?.tableData || visualizationData?.wardRankings) {
      tabs.push({
        name: 'Data Table',
        icon: TableCellsIcon,
        component: () => <DataTableView data={visualizationData} />,
      });
    }

    // Default tabs if no specific data
    if (tabs.length === 0) {
      tabs.push(
        {
          name: 'Map View',
          icon: MapIcon,
          component: () => <UniversalMapViewer />,
        },
        {
          name: 'Charts',
          icon: ChartBarIcon,
          component: () => <ChartVisualization />,
        },
        {
          name: 'Data Table',
          icon: TableCellsIcon,
          component: () => <DataTableView />,
        }
      );
    }

    setAvailableTabs(tabs);
  }, [visualizationData, mapUrl, mapType]);

  return (
    <Modal
      isOpen={isOpen}
      onClose={onClose}
      title="Malaria Risk Visualization"
      size="xl"
    >
      <Tab.Group selectedIndex={selectedIndex} onChange={setSelectedIndex}>
        <Tab.List className="flex space-x-1 rounded-xl bg-gray-100 dark:bg-gray-700 p-1">
          {availableTabs.map((tab, index) => (
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
          {availableTabs.map((tab, idx) => (
            <Tab.Panel
              key={idx}
              className="rounded-xl bg-white dark:bg-gray-800 p-3"
            >
              <tab.component data={visualizationData} />
            </Tab.Panel>
          ))}
        </Tab.Panels>
      </Tab.Group>
    </Modal>
  );
};

export default VisualizationModal;