import React, { useEffect, useState } from 'react';
import MapVisualization from './MapVisualization';
import { DocumentMagnifyingGlassIcon } from '@heroicons/react/24/outline';

interface UniversalMapViewerProps {
  mapData?: any;
  mapUrl?: string;
  mapType?: 'leaflet' | 'folium' | 'plotly' | 'kepler' | 'iframe';
}

const UniversalMapViewer: React.FC<UniversalMapViewerProps> = ({ 
  mapData,
  mapUrl,
  mapType = 'iframe'
}) => {
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (mapUrl || mapData) {
      setIsLoading(false);
    } else {
      setError('No map data or URL provided');
      setIsLoading(false);
    }
  }, [mapUrl, mapData]);

  // For Leaflet maps with GeoJSON data
  if (mapType === 'leaflet' && mapData) {
    return <MapVisualization data={mapData} />;
  }

  // For all other map types served as HTML files from backend
  if (mapUrl) {
    return (
      <div className="h-[600px] w-full relative">
        {isLoading && (
          <div className="absolute inset-0 flex items-center justify-center bg-gray-50 dark:bg-gray-800">
            <div className="text-center">
              <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
              <p className="mt-2 text-sm text-gray-600 dark:text-gray-400">Loading map...</p>
            </div>
          </div>
        )}
        
        <iframe
          src={mapUrl}
          className="w-full h-full rounded-lg border-0"
          title="Malaria Risk Map"
          onLoad={() => setIsLoading(false)}
          sandbox="allow-scripts allow-same-origin"
        />

        {/* Map type indicator */}
        <div className="absolute top-2 right-2 bg-white dark:bg-gray-800 px-2 py-1 rounded text-xs font-medium text-gray-600 dark:text-gray-400 shadow">
          {mapType.charAt(0).toUpperCase() + mapType.slice(1)} Map
        </div>
      </div>
    );
  }

  // Error state
  if (error) {
    return (
      <div className="h-[600px] w-full flex items-center justify-center bg-gray-50 dark:bg-gray-800 rounded-lg">
        <div className="text-center">
          <DocumentMagnifyingGlassIcon className="h-12 w-12 text-gray-400 mx-auto mb-4" />
          <p className="text-gray-600 dark:text-gray-400">{error}</p>
        </div>
      </div>
    );
  }

  return null;
};

export default UniversalMapViewer;