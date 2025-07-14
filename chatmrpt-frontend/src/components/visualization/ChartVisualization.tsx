import React, { useState } from 'react';
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  RadarChart,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
  Radar,
} from 'recharts';

interface ChartVisualizationProps {
  data?: any;
}

const ChartVisualization: React.FC<ChartVisualizationProps> = ({ data }) => {
  const [selectedChart, setSelectedChart] = useState('bar');

  // Mock data for demonstration - replace with actual data from backend
  const barChartData = data?.wardRankings || [
    { name: 'Ward A', tpr: 45, population: 15000, risk_score: 85 },
    { name: 'Ward B', tpr: 38, population: 22000, risk_score: 72 },
    { name: 'Ward C', tpr: 32, population: 18000, risk_score: 65 },
    { name: 'Ward D', tpr: 28, population: 12000, risk_score: 58 },
    { name: 'Ward E', tpr: 22, population: 25000, risk_score: 45 },
  ];

  const pieChartData = data?.riskDistribution || [
    { name: 'High Risk', value: 15, color: '#dc2626' },
    { name: 'Medium Risk', value: 35, color: '#f59e0b' },
    { name: 'Low Risk', value: 50, color: '#10b981' },
  ];

  const radarData = data?.factorAnalysis || [
    { factor: 'TPR', value: 75 },
    { factor: 'Population Density', value: 60 },
    { factor: 'Rainfall', value: 85 },
    { factor: 'NDVI', value: 70 },
    { factor: 'Distance to Clinic', value: 45 },
    { factor: 'Housing Quality', value: 55 },
  ];

  const lineChartData = data?.trendData || [
    { month: 'Jan', cases: 120, tpr: 15 },
    { month: 'Feb', cases: 140, tpr: 18 },
    { month: 'Mar', cases: 180, tpr: 22 },
    { month: 'Apr', cases: 220, tpr: 28 },
    { month: 'May', cases: 280, tpr: 35 },
    { month: 'Jun', cases: 320, tpr: 42 },
  ];

  const chartTypes = [
    { id: 'bar', name: 'Ward Rankings' },
    { id: 'pie', name: 'Risk Distribution' },
    { id: 'radar', name: 'Factor Analysis' },
    { id: 'line', name: 'Trend Analysis' },
  ];

  return (
    <div className="space-y-4">
      {/* Chart Type Selector */}
      <div className="flex space-x-2 border-b dark:border-gray-700">
        {chartTypes.map((type) => (
          <button
            key={type.id}
            onClick={() => setSelectedChart(type.id)}
            className={`px-4 py-2 text-sm font-medium transition-colors ${
              selectedChart === type.id
                ? 'text-blue-600 border-b-2 border-blue-600'
                : 'text-gray-600 hover:text-gray-900 dark:text-gray-400 dark:hover:text-white'
            }`}
          >
            {type.name}
          </button>
        ))}
      </div>

      {/* Chart Display */}
      <div className="h-[500px] w-full">
        {selectedChart === 'bar' && (
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={barChartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="name" />
              <YAxis yAxisId="left" orientation="left" stroke="#8884d8" />
              <YAxis yAxisId="right" orientation="right" stroke="#82ca9d" />
              <Tooltip />
              <Legend />
              <Bar yAxisId="left" dataKey="tpr" fill="#8884d8" name="TPR (%)" />
              <Bar yAxisId="left" dataKey="risk_score" fill="#82ca9d" name="Risk Score" />
            </BarChart>
          </ResponsiveContainer>
        )}

        {selectedChart === 'pie' && (
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie
                data={pieChartData}
                cx="50%"
                cy="50%"
                labelLine={false}
                label={({ name, percent }) => `${name}: ${percent ? (percent * 100).toFixed(0) : 0}%`}
                outerRadius={150}
                fill="#8884d8"
                dataKey="value"
              >
                {pieChartData.map((entry: any, index: number) => (
                  <Cell key={`cell-${index}`} fill={entry.color} />
                ))}
              </Pie>
              <Tooltip />
            </PieChart>
          </ResponsiveContainer>
        )}

        {selectedChart === 'radar' && (
          <ResponsiveContainer width="100%" height="100%">
            <RadarChart data={radarData}>
              <PolarGrid />
              <PolarAngleAxis dataKey="factor" />
              <PolarRadiusAxis angle={90} domain={[0, 100]} />
              <Radar
                name="Risk Factors"
                dataKey="value"
                stroke="#8884d8"
                fill="#8884d8"
                fillOpacity={0.6}
              />
              <Tooltip />
            </RadarChart>
          </ResponsiveContainer>
        )}

        {selectedChart === 'line' && (
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={lineChartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="month" />
              <YAxis yAxisId="left" />
              <YAxis yAxisId="right" orientation="right" />
              <Tooltip />
              <Legend />
              <Line
                yAxisId="left"
                type="monotone"
                dataKey="cases"
                stroke="#8884d8"
                name="Cases"
              />
              <Line
                yAxisId="right"
                type="monotone"
                dataKey="tpr"
                stroke="#82ca9d"
                name="TPR (%)"
              />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Chart Description */}
      <div className="text-sm text-gray-600 dark:text-gray-400 mt-4">
        {selectedChart === 'bar' && (
          <p>Showing ward rankings by Test Positivity Rate (TPR) and calculated risk scores.</p>
        )}
        {selectedChart === 'pie' && (
          <p>Distribution of wards across different risk categories based on comprehensive analysis.</p>
        )}
        {selectedChart === 'radar' && (
          <p>Multi-factor analysis showing the contribution of various environmental and demographic factors.</p>
        )}
        {selectedChart === 'line' && (
          <p>Trend analysis showing the progression of malaria cases and TPR over time.</p>
        )}
      </div>
    </div>
  );
};

export default ChartVisualization;