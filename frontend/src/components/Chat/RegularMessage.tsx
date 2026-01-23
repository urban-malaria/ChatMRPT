import React, { useMemo } from 'react';
import ReactMarkdown from 'react-markdown';
import VisualizationContainer from '../Visualization/VisualizationContainer';
import DualMethodDisplay from '../Analysis/DualMethodDisplay';
import type { RegularMessage as RegularMessageType } from '@/types';

interface RegularMessageProps {
  message: RegularMessageType;
}

const RegularMessage: React.FC<RegularMessageProps> = ({ message }) => {
  const isUser = message.sender === 'user';
  
  // Detect and extract a top-level warning line (styled in red)
  let topWarning = '';
  let restContent = message.content;
  if (!isUser && typeof message.content === 'string') {
    const firstNewline = message.content.indexOf('\n');
    const firstLine = (firstNewline >= 0 ? message.content.slice(0, firstNewline) : message.content).trim();
    if (firstLine.startsWith('⚠️')) {
      topWarning = firstLine;
      restContent = firstNewline >= 0 ? message.content.slice(firstNewline + 1) : '';
    }
  }
  
  // Check if message contains visualization URLs or has visualizations array
  const hasVisualization = !isUser && (
    message.content.includes('/serve_viz_file/') ||
    message.content.includes('/static/visualizations/') ||
    message.content.includes('.html') ||
    (message.visualizations && message.visualizations.length > 0)
  );
  
  // Check if message contains analysis results
  const analysisData = useMemo(() => {
    if (isUser || !message.metadata?.analysisResults) return null;
    
    // Extract analysis data from metadata
    const { pca, composite, ward, state } = message.metadata.analysisResults;
    
    return {
      data: {
        pca: pca ? {
          score: pca.score,
          ranking: pca.ranking,
          totalRanked: pca.totalRanked,
          indicators: pca.indicators || []
        } : undefined,
        composite: composite ? {
          score: composite.score,
          ranking: composite.ranking,
          totalRanked: composite.totalRanked,
          indicators: composite.indicators || []
        } : undefined,
      },
      wardName: ward,
      stateName: state,
    };
  }, [isUser, message.metadata]);
  
  return (
    <div className={`flex ${isUser ? 'justify-end' : 'justify-start'}`}>
      <div
        className={`max-w-3xl rounded-lg px-4 py-3 ${
          isUser
            ? 'bg-blue-600 text-white'
            : 'bg-gray-100 dark:bg-dark-bg-tertiary text-gray-800 dark:text-dark-text'
        }`}
      >
        {/* Message Header */}
        <div className="flex items-center mb-1">
          <span className="text-xs font-medium opacity-75">
            {isUser ? 'You' : 'Assistant'}
          </span>
          {message.metadata?.model && (
            <span className="ml-2 text-xs opacity-50">
              ({message.metadata.model})
            </span>
          )}
          {message.isStreaming && (
            <span className="ml-2">
              <svg className="animate-spin h-3 w-3" viewBox="0 0 24 24">
                <circle
                  className="opacity-25"
                  cx="12"
                  cy="12"
                  r="10"
                  stroke="currentColor"
                  strokeWidth="4"
                  fill="none"
                />
                <path
                  className="opacity-75"
                  fill="currentColor"
                  d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                />
              </svg>
            </span>
          )}
        </div>
        
        {/* Message Content */}
        <div className={`prose prose-sm max-w-none ${isUser ? 'prose-invert' : ''}`}>
          {!isUser ? (
            <>
              {topWarning && (
                <div className="text-red-600 dark:text-red-400 font-bold mb-2">{topWarning}</div>
              )}
              {/* Check if content starts with our styled HTML */}
              {restContent.startsWith('<span style="color: red') ? (
                <>
                  {/* Extract and render the IMPORTANT message with HTML styling */}
                  {(() => {
                    const htmlEndIndex = restContent.indexOf('</span>\n');
                    if (htmlEndIndex > -1) {
                      const htmlContent = restContent.substring(0, htmlEndIndex + 8);
                      const remainingContent = restContent.substring(htmlEndIndex + 8);
                      return (
                        <>
                          <div dangerouslySetInnerHTML={{ __html: htmlContent }} className="mb-2" />
                          <ReactMarkdown
                            components={{
                              code: ({ className, children, ...props }) => {
                                const match = /language-(\w+)/.exec(className || '');
                                return match ? (
                                  <pre className="bg-gray-800 text-white p-2 rounded overflow-x-auto">
                                    <code className={className} {...props}>
                                      {children}
                                    </code>
                                  </pre>
                                ) : (
                                  <code className="bg-gray-200 dark:bg-dark-border px-1 rounded" {...props}>
                                    {children}
                                  </code>
                                );
                              },
                              a: ({ ...props }) => (
                                <a {...props} target="_blank" rel="noopener noreferrer" className="text-blue-600 dark:text-blue-400 hover:underline" />
                              ),
                            }}
                          >
                            {remainingContent}
                          </ReactMarkdown>
                        </>
                      );
                    }
                    // Fallback if parsing fails
                    return <div dangerouslySetInnerHTML={{ __html: restContent }} />;
                  })()}
                </>
              ) : (
                <ReactMarkdown
                  components={{
                    code: ({ className, children, ...props }) => {
                      const match = /language-(\w+)/.exec(className || '');
                      return match ? (
                        <pre className="bg-gray-800 text-white p-2 rounded overflow-x-auto">
                          <code className={className} {...props}>
                            {children}
                          </code>
                        </pre>
                      ) : (
                        <code className="bg-gray-200 dark:bg-dark-border px-1 rounded" {...props}>
                          {children}
                        </code>
                      );
                    },
                    a: ({ ...props }) => (
                      <a {...props} target="_blank" rel="noopener noreferrer" className="text-blue-600 dark:text-blue-400 hover:underline" />
                    ),
                  }}
                >
                  {restContent}
                </ReactMarkdown>
              )}
            </>
          ) : (
            <p className="whitespace-pre-wrap">{message.content}</p>
          )}
        </div>
        
        {/* Message Footer */}
        {message.metadata && (message.metadata.latency || message.metadata.tokens) && (
          <div className="mt-2 text-xs opacity-50">
            {message.metadata.latency && (
              <span>Latency: {message.metadata.latency}ms</span>
            )}
            {message.metadata.tokens && (
              <span className="ml-3">Tokens: {message.metadata.tokens}</span>
            )}
          </div>
        )}
        
        {/* Analysis Results Display */}
        {analysisData && (
          <div className="mt-4">
            <DualMethodDisplay
              data={analysisData.data}
              wardName={analysisData.wardName}
              stateName={analysisData.stateName}
            />
          </div>
        )}
        
        {/* Visualization Container */}
        {hasVisualization && (
          <div className="mt-4">
            {/* Handle visualizations array if present */}
            {message.visualizations && message.visualizations.length > 0 ? (
              message.visualizations.map((viz: any, index: number) => (
                <VisualizationContainer
                  key={index}
                  content={typeof viz === 'string' ? viz : (viz.path || viz.html_path || viz.url)}
                  onExplain={(vizUrl) => {
                    // Could trigger a new message asking for explanation
                    console.log('Explain visualization:', vizUrl);
                  }}
                />
              ))
            ) : (
              /* Fallback to original logic for content-based URLs */
              <VisualizationContainer
                content={message.content}
                onExplain={(vizUrl) => {
                  // Could trigger a new message asking for explanation
                  console.log('Explain visualization:', vizUrl);
                }}
              />
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default RegularMessage;
