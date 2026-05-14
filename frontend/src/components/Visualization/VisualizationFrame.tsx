import React, { useState, useRef, useEffect } from 'react';

interface VisualizationFrameProps {
  url: string;
  title?: string;
  className?: string;
  onLoad?: () => void;
  onError?: (error: Error) => void;
}

const VisualizationFrame: React.FC<VisualizationFrameProps> = ({
  url,
  title,
  className = '',
  onLoad,
  onError,
}) => {
  const [isLoading, setIsLoading] = useState(true);
  const [hasError, setHasError] = useState(false);
  const [currentUrl, setCurrentUrl] = useState(url);
  const iframeRef = useRef<HTMLIFrameElement>(null);
  const isSettlementMap = currentUrl.includes('settlement');
  const defaultHeight = isSettlementMap ? '820px' : '600px';
  
  const handleLoad = () => {
    setIsLoading(false);
    setHasError(false);
    onLoad?.();
    
    // Auto-adjust iframe height if possible
    try {
      if (iframeRef.current?.contentWindow) {
        const body = iframeRef.current.contentWindow.document.body;
        const height = body.scrollHeight;
        if (height > 0) {
          iframeRef.current.style.height = `${Math.min(height + 20, isSettlementMap ? 920 : 800)}px`;
        }
      }
    } catch (e) {
      // Cross-origin restriction, use default height
      console.log('Cannot access iframe content (cross-origin)');
    }
  };
  
  const handleError = () => {
    setIsLoading(false);
    setHasError(true);
    onError?.(new Error(`Failed to load visualization: ${url}`));
  };
  
  useEffect(() => {
    setCurrentUrl(url);
    setIsLoading(true);
    setHasError(false);
  }, [url]);
  
  // Listen for ITN map update messages from iframe
  useEffect(() => {
    const handleMessage = (event: MessageEvent) => {
      // Only handle ITN map updates
      if (event.data?.type === 'updateITNMap' && event.data?.mapPath) {
        console.log('Received ITN map update:', event.data.mapPath);
        // Update the iframe URL
        setCurrentUrl(event.data.mapPath);
        setIsLoading(true);
        setHasError(false);
      }
    };
    
    window.addEventListener('message', handleMessage);
    
    return () => {
      window.removeEventListener('message', handleMessage);
    };
  }, []);
  
  return (
    <div className={`visualization-frame relative ${className}`}>
      {isLoading && (
        <div className="absolute inset-0 flex items-center justify-center bg-gray-50 rounded-lg">
          <div className="flex flex-col items-center">
            <svg className="animate-spin h-8 w-8 text-blue-600 mb-2" viewBox="0 0 24 24">
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
            <p className="text-sm text-gray-600">Loading visualization...</p>
          </div>
        </div>
      )}
      
      {hasError && (
        <div className="flex items-center justify-center h-96 bg-gray-50 rounded-lg border-2 border-dashed border-gray-300">
          <div className="text-center">
            <svg className="mx-auto h-12 w-12 text-gray-400 mb-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            <p className="text-sm text-gray-600">Failed to load visualization</p>
            <p className="text-xs text-gray-500 mt-1">{title || 'Unknown visualization'}</p>
          </div>
        </div>
      )}
      
      <iframe
        ref={iframeRef}
        src={currentUrl}
        title={title || 'Visualization'}
        className={`w-full border-0 rounded-lg ${hasError ? 'hidden' : ''}`}
        style={{ minHeight: isSettlementMap ? '700px' : '400px', height: defaultHeight }}
        sandbox="allow-scripts allow-same-origin allow-popups"
        loading="lazy"
        onLoad={handleLoad}
        onError={handleError}
      />
    </div>
  );
};

export default VisualizationFrame;
