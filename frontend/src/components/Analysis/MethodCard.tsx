import React, { useState } from 'react';

interface MethodCardProps {
  methodName: 'PCA' | 'Composite';
  score?: number;
  ranking?: number;
  totalRanked?: number;
  description: string;
  indicators?: string[];
  isActive?: boolean;
  onClick?: () => void;
}

const MethodCard: React.FC<MethodCardProps> = ({
  methodName,
  score,
  ranking,
  totalRanked,
  description,
  indicators = [],
  isActive = false,
  onClick,
}) => {
  const [isExpanded, setIsExpanded] = useState(false);
  
  const getMethodColor = () => {
    return methodName === 'PCA' ? 'blue' : 'green';
  };
  
  const color = getMethodColor();
  
  return (
    <div
      className={`
        relative overflow-hidden rounded-lg border-2 transition-all duration-300 cursor-pointer
        ${isActive
          ? `border-${color}-500 bg-${color}-50 dark:bg-${color}-900/20 shadow-lg`
          : 'border-gray-200 dark:border-dark-border bg-white dark:bg-dark-bg-tertiary hover:border-gray-300 dark:hover:border-dark-text-secondary hover:shadow-md'
        }
      `}
      onClick={onClick}
    >
      {/* Header */}
      <div className="p-4">
        <div className="flex items-center justify-between mb-2">
          <h3 className={`text-lg font-semibold ${isActive ? `text-${color}-700 dark:text-${color}-400` : 'text-gray-900 dark:text-dark-text'}`}>
            {methodName} Analysis
          </h3>
          {score !== undefined && (
            <span className={`px-3 py-1 text-sm font-medium rounded-full ${
              isActive ? `bg-${color}-100 dark:bg-${color}-900/30 text-${color}-700 dark:text-${color}-400` : 'bg-gray-100 dark:bg-dark-border text-gray-700 dark:text-dark-text'
            }`}>
              Score: {score.toFixed(2)}
            </span>
          )}
        </div>
        
        {/* Ranking */}
        {ranking !== undefined && totalRanked !== undefined && (
          <div className="flex items-center mb-3">
            <svg className="w-4 h-4 mr-2 text-gray-500 dark:text-dark-text-secondary" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
            </svg>
            <span className="text-sm text-gray-600 dark:text-dark-text-secondary">
              Rank: <span className="font-semibold dark:text-dark-text">{ranking}</span> of {totalRanked}
            </span>
          </div>
        )}

        {/* Description */}
        <p className="text-sm text-gray-600 dark:text-dark-text-secondary mb-3">
          {description}
        </p>
        
        {/* Expand/Collapse Button */}
        {indicators.length > 0 && (
          <button
            onClick={(e) => {
              e.stopPropagation();
              setIsExpanded(!isExpanded);
            }}
            className={`flex items-center text-sm font-medium ${
              isActive ? `text-${color}-600 hover:text-${color}-700` : 'text-gray-600 hover:text-gray-700'
            }`}
          >
            <span className="mr-1">{isExpanded ? 'Hide' : 'Show'} Indicators</span>
            <svg
              className={`w-4 h-4 transform transition-transform ${isExpanded ? 'rotate-180' : ''}`}
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
            </svg>
          </button>
        )}
      </div>
      
      {/* Expanded Indicators */}
      {isExpanded && indicators.length > 0 && (
        <div className={`px-4 pb-4 pt-0`}>
          <div className={`rounded-lg p-3 ${isActive ? `bg-${color}-100 dark:bg-${color}-900/30` : 'bg-gray-50 dark:bg-dark-border'}`}>
            <h4 className="text-xs font-semibold text-gray-500 dark:text-dark-text-secondary uppercase tracking-wider mb-2">
              Key Indicators
            </h4>
            <ul className="space-y-1">
              {indicators.map((indicator, index) => (
                <li key={index} className="flex items-start">
                  <svg className={`w-3 h-3 mr-2 mt-0.5 flex-shrink-0 ${
                    isActive ? `text-${color}-500` : 'text-gray-400 dark:text-dark-text-secondary'
                  }`} fill="currentColor" viewBox="0 0 20 20">
                    <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                  </svg>
                  <span className="text-xs text-gray-600 dark:text-dark-text-secondary">{indicator}</span>
                </li>
              ))}
            </ul>
          </div>
        </div>
      )}
      
      {/* Active Indicator */}
      {isActive && (
        <div className={`absolute top-0 left-0 w-full h-1 bg-${color}-500`} />
      )}
    </div>
  );
};

export default MethodCard;