import React, { useEffect, useState, useRef } from 'react';
import { MapContainer, TileLayer, GeoJSON, LayersControl } from 'react-leaflet';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';

// Fix for default markers in React-Leaflet
import icon from 'leaflet/dist/images/marker-icon.png';
import iconShadow from 'leaflet/dist/images/marker-shadow.png';
let DefaultIcon = L.icon({
  iconUrl: icon,
  shadowUrl: iconShadow,
  iconSize: [25, 41],
  iconAnchor: [12, 41]
});
L.Marker.prototype.options.icon = DefaultIcon;

interface MapVisualizationProps {
  data?: any;
}

const MapVisualization: React.FC<MapVisualizationProps> = ({ data }) => {
  const [geoJsonData, setGeoJsonData] = useState<any>(null);
  const [bounds, setBounds] = useState<L.LatLngBounds | null>(null);

  useEffect(() => {
    // Mock data for demonstration - replace with actual data from backend
    if (data?.geojson) {
      setGeoJsonData(data.geojson);
      // Calculate bounds from GeoJSON
      const geoJsonLayer = L.geoJSON(data.geojson);
      setBounds(geoJsonLayer.getBounds());
    } else {
      // Default to Nigeria bounds if no data
      setBounds(L.latLngBounds([4.27, 2.67], [13.89, 14.65]));
    }
  }, [data]);

  const getFeatureStyle = (feature: any) => {
    // Style based on risk level
    const riskLevel = feature?.properties?.risk_level || 'low';
    const colors: Record<string, string> = {
      high: '#dc2626',
      medium: '#f59e0b',
      low: '#10b981',
    };

    return {
      fillColor: colors[riskLevel] || '#6b7280',
      weight: 2,
      opacity: 1,
      color: 'white',
      dashArray: '3',
      fillOpacity: 0.7
    };
  };

  const onEachFeature = (feature: any, layer: L.Layer) => {
    if (feature.properties) {
      const props = feature.properties;
      const popupContent = `
        <div class="p-2">
          <h3 class="font-bold text-lg">${props.WardName || 'Unknown Ward'}</h3>
          <p class="text-sm">Risk Level: <span class="font-semibold">${props.risk_level || 'N/A'}</span></p>
          <p class="text-sm">Population: ${props.population || 'N/A'}</p>
          <p class="text-sm">TPR: ${props.tpr || 'N/A'}%</p>
          ${props.vulnerability_score ? `<p class="text-sm">Vulnerability Score: ${props.vulnerability_score.toFixed(2)}</p>` : ''}
        </div>
      `;
      (layer as L.Path).bindPopup(popupContent);
    }
  };

  return (
    <div className="h-[600px] w-full relative">
      <MapContainer
        bounds={bounds || undefined}
        style={{ height: '100%', width: '100%' }}
        className="rounded-lg"
      >
        <LayersControl position="topright">
          <LayersControl.BaseLayer checked name="OpenStreetMap">
            <TileLayer
              attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />
          </LayersControl.BaseLayer>
          <LayersControl.BaseLayer name="Satellite">
            <TileLayer
              attribution='&copy; <a href="https://www.esri.com/">Esri</a>'
              url="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
            />
          </LayersControl.BaseLayer>
        </LayersControl>

        {geoJsonData && (
          <GeoJSON
            data={geoJsonData}
            style={getFeatureStyle}
            onEachFeature={onEachFeature}
          />
        )}
      </MapContainer>

      {/* Legend */}
      <div className="absolute bottom-4 left-4 bg-white dark:bg-gray-800 p-3 rounded-lg shadow-lg z-[1000]">
        <h4 className="text-sm font-semibold mb-2">Risk Level</h4>
        <div className="space-y-1">
          <div className="flex items-center space-x-2">
            <div className="w-4 h-4 bg-red-600 rounded"></div>
            <span className="text-xs">High Risk</span>
          </div>
          <div className="flex items-center space-x-2">
            <div className="w-4 h-4 bg-amber-500 rounded"></div>
            <span className="text-xs">Medium Risk</span>
          </div>
          <div className="flex items-center space-x-2">
            <div className="w-4 h-4 bg-green-500 rounded"></div>
            <span className="text-xs">Low Risk</span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default MapVisualization;