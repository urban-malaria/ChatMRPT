/**
 * Visualization Manager Module
 * Handles visualization display, navigation, and interactions
 */

import DOMHelpers from '../../utils/dom-helpers.js';

export class VisualizationManager {
    constructor(messageHandler) {
        this.messageHandler = messageHandler;
        this.setupEventListeners();
    }

    setupEventListeners() {
        // Listen for visualization responses
        document.addEventListener('messageResponse', (event) => {
            const { response } = event.detail;
            this.handleVisualizationResponse(response);
        });
    }

    handleVisualizationResponse(response) {
        // Process actions first (including visualizations)
        if (response.action) {
            this.handleAction(response);
        }

        // Process auto-generated visualizations (for method switching)
        if (response.visualizations && Array.isArray(response.visualizations)) {
            console.log('🎯 Processing visualizations:', response.visualizations);
            response.visualizations.forEach((viz, index) => {
                console.log(`🎯 Processing visualization ${index + 1}:`, viz);
                setTimeout(() => {
                    const vizPath = viz.url || viz.path || viz.html;
                    console.log(`🎯 Using visualization path: ${vizPath}`);
                    this.addVisualization(
                        vizPath,
                        viz.title,
                        viz.type,
                        viz.metadata || {}
                    );
                }, index * 500); // Stagger visualization loading
            });
        }
    }

    handleAction(response) {
        const action = response.action;
        const actionType = typeof action === 'string' ? action : action?.type;
        
        switch (actionType) {
            case 'show_visualization':
            case 'visualization_shown':
                if (response.visualization || response.response) {
                    const vizPath = response.visualization || this.extractVisualizationPath(response.response);
                    if (vizPath) {
                        const title = this.getVisualizationTitle(response.viz_type, response.variable);
                        
                        const vizData = this.extractVisualizationData(response);
                        
                        this.addVisualization(vizPath, title, response.viz_type || 'composite_map', vizData);
                    }
                }
                break;
            case 'visualization_ready':
                this.handleVisualizationReady(response);
                break;
        }
    }

    addVisualization(vizPath, title, vizType, vizData = {}) {
        if (!vizPath) {
            console.error('❌ No visualization path provided!');
            this.messageHandler.addSystemMessage('Error: No visualization file path provided.');
            return;
        }
        
        // Remove existing visualization of the same type to prevent duplicates
        this.removeExistingVisualization(vizType);
        
        const messageDiv = DOMHelpers.createElement('div', {
            className: 'message assistant-message visualization-message new-message'
        });

        const contentDiv = DOMHelpers.createElement('div', {
            className: 'message-content'
        });

        const pageInfo = this.extractPageInfo(title, vizData);
        
        const container = DOMHelpers.createElement('div', {
            className: 'visualization-container'
        });
        
        // Add essential data attributes for pagination functionality
        container.dataset.vizType = vizType;
        container.dataset.currentPage = pageInfo.currentPage.toString();
        container.dataset.totalPages = pageInfo.totalPages.toString();
        
        // Create header with title and controls
        const header = this.createVisualizationHeader(title, pageInfo, vizType, vizData, vizPath);
        const content = this.createVisualizationContent(vizPath);

        container.appendChild(header);
        container.appendChild(content);
        contentDiv.appendChild(container);
        messageDiv.appendChild(contentDiv);

        // Add to chat
        this.messageHandler.appendMessage(messageDiv);
        this.messageHandler.scrollToBottom();
    }

    removeExistingVisualization(vizType) {
        // Find and remove any existing visualization of the same type
        const chatContainer = this.messageHandler.chatContainer;
        if (!chatContainer) return;
        
        const existingViz = chatContainer.querySelector(`[data-viz-type="${vizType}"]`);
        if (existingViz) {
            // Remove the entire message containing this visualization
            const messageElement = existingViz.closest('.message');
            if (messageElement) {
                messageElement.remove();
                console.log(`🗑️ Removed existing ${vizType} visualization to prevent duplicates`);
            }
        }
    }

    createVisualizationHeader(title, pageInfo, vizType, vizData, vizPath) {
        const header = DOMHelpers.createElement('div', {
            className: 'visualization-header'
        });

        const titleElement = DOMHelpers.createElement('h4', {
            className: 'visualization-title'
        }, `📊 ${title}`);

        const controls = DOMHelpers.createElement('div', {
            className: 'visualization-controls'
        });

        // Add pagination controls if multi-page visualization
        if (pageInfo.totalPages > 1) {
            const paginationControls = this.createPaginationControls(pageInfo, vizType, vizData);
            controls.appendChild(paginationControls);
        }

        // Add expand and download buttons
        const expandBtn = this.createControlButton('fas fa-expand', 'View Fullscreen');
        const downloadBtn = this.createControlButton('fas fa-download', 'Download Visualization');

        // Add event listeners for the buttons
        expandBtn.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            this.openFullscreen(vizPath, title);
        });

        downloadBtn.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            this.downloadVisualization(vizPath, title);
        });

        controls.appendChild(expandBtn);
        controls.appendChild(downloadBtn);
        
        header.appendChild(titleElement);
        header.appendChild(controls);

        return header;
    }

    createVisualizationContent(vizPath) {
        const content = DOMHelpers.createElement('div', {
            className: 'visualization-content'
        });

        const iframe = DOMHelpers.createElement('iframe', {
            src: vizPath,
            className: 'visualization-iframe',
            frameborder: '0',
            style: 'width: 100%; height: 600px; border: none; border-radius: 8px; min-height: 400px;'
        });

        const loadingDiv = DOMHelpers.createElement('div', {
            className: 'visualization-loading',
            style: 'display: flex; align-items: center; justify-content: center; height: 400px; color: var(--text-secondary);'
        }, '<i class="fas fa-spinner fa-spin"></i> <span style="margin-left: 8px;">Loading visualization...</span>');

        content.appendChild(loadingDiv);
        content.appendChild(iframe);

        // Enhanced iframe load handling with responsive sizing
        iframe.addEventListener('load', () => {
            loadingDiv.style.display = 'none';
            iframe.style.display = 'block';
            
            // Auto-adjust iframe height based on content
            this.adjustIframeHeight(iframe);
            
            // Enable interactive features in iframe
            this.enableIframeInteractivity(iframe);
        });

        iframe.addEventListener('error', () => {
            loadingDiv.innerHTML = '<i class="fas fa-exclamation-triangle"></i> <span style="margin-left: 8px;">Error loading visualization</span>';
            loadingDiv.style.color = 'var(--error-color, #dc2626)';
        });

        // Add resize observer for responsive behavior
        if (window.ResizeObserver) {
            const resizeObserver = new ResizeObserver(() => {
                this.adjustIframeHeight(iframe);
            });
            resizeObserver.observe(content);
        }

        return content;
    }

    createControlButton(iconClass, title) {
        const button = DOMHelpers.createElement('button', {
            className: 'viz-btn',
            title: title
        }, `<i class="${iconClass}"></i>`);

        // Enhanced button interactions
        button.addEventListener('mouseenter', () => {
            button.style.transform = 'translateY(-1px)';
            button.style.transition = 'all 0.2s ease';
        });

        button.addEventListener('mouseleave', () => {
            button.style.transform = 'translateY(0)';
        });

        return button;
    }

    createPaginationControls(pageInfo, vizType, vizData) {
        const paginationDiv = DOMHelpers.createElement('div', {
            className: 'pagination-controls'
        });
        
        const prevBtn = DOMHelpers.createElement('button', {
            className: `viz-btn pagination-btn ${pageInfo.currentPage <= 1 ? 'disabled' : ''}`,
            title: 'Previous Page',
            disabled: pageInfo.currentPage <= 1
        }, '<i class="fas fa-chevron-left"></i>');
        
        const pageIndicator = DOMHelpers.createElement('span', {
            className: 'page-indicator'
        }, `Page ${pageInfo.currentPage} of ${pageInfo.totalPages}`);
        
        const nextBtn = DOMHelpers.createElement('button', {
            className: `viz-btn pagination-btn ${pageInfo.currentPage >= pageInfo.totalPages ? 'disabled' : ''}`,
            title: 'Next Page',
            disabled: pageInfo.currentPage >= pageInfo.totalPages
        }, '<i class="fas fa-chevron-right"></i>');
        
        // Add event listeners
        prevBtn.addEventListener('click', () => {
            this.navigateVisualization(vizType, 'prev', pageInfo, vizData);
        });
        
        nextBtn.addEventListener('click', () => {
            this.navigateVisualization(vizType, 'next', pageInfo, vizData);
        });
        
        paginationDiv.appendChild(prevBtn);
        paginationDiv.appendChild(pageIndicator);
        paginationDiv.appendChild(nextBtn);
        
        return paginationDiv;
    }

    getVisualizationTitle(vizType, variable = null) {
        const titles = {
            'variable_map': variable ? `${variable} Distribution Map` : 'Variable Distribution Map',
            'normalized_map': variable ? `Normalized ${variable} Map` : 'Normalized Variable Map',
            'composite_map': 'Risk Score Distribution by Model',
            'vulnerability_map': 'Ward Vulnerability Classification Map',
            'vulnerability_plot': 'Ward Vulnerability Rankings Distribution',
            'boxplot': 'Risk Score Distribution Plot',
            'vulnerability_boxplot': 'Vulnerability Score Distribution',
            'decision_tree': 'Malaria Risk Analysis Workflow',
            'urban_extent_map': 'Urban Extent Analysis'
        };
        
        return titles[vizType] || `${vizType.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase())} Visualization`;
    }

    extractPageInfo(title, vizData) {
        let currentPage = 1;
        let totalPages = 1;
        
        // Check multiple sources for pagination data
        if (vizData?.current_page) currentPage = vizData.current_page;
        if (vizData?.total_pages) totalPages = vizData.total_pages;
        if (vizData?.metadata?.current_page) currentPage = vizData.metadata.current_page;
        if (vizData?.metadata?.total_pages) totalPages = vizData.metadata.total_pages;
        
        // Fallback: extract from title
        if (totalPages === 1 && title) {
            const patterns = [
                /Page\s+(\d+)\s+of\s+(\d+)/i,
                /(\d+)\s+of\s+(\d+)/i,
                /\((\d+)\/(\d+)\)/i,
                /\[(\d+)\/(\d+)\]/i
            ];
            
            for (const pattern of patterns) {
                const pageMatch = title.match(pattern);
                if (pageMatch) {
                    currentPage = parseInt(pageMatch[1]);
                    totalPages = parseInt(pageMatch[2]);
                    break;
                }
            }
        }
        
        return { currentPage, totalPages };
    }

    async navigateVisualization(vizType, direction, pageInfo, vizData) {
        try {
            let endpoint = '/navigate_visualization';
            let requestBody = {
                viz_type: vizType,
                direction: direction,
                current_page: pageInfo.currentPage,
                total_pages: pageInfo.totalPages,
                metadata: vizData
            };
            
            // Use specific endpoints for known visualization types
            if (vizType === 'composite_map') {
                endpoint = '/navigate_composite_map';
                requestBody = { direction: direction, current_page: pageInfo.currentPage };
            } else if (vizType === 'boxplot' || vizType === 'vulnerability_boxplot') {
                endpoint = '/navigate_boxplot';
                requestBody = { direction: direction, current_page: pageInfo.currentPage };
            }
            
            const response = await fetch(endpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(requestBody)
            });
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            
            const result = await response.json();
            
            if (result.status === 'success') {
                const vizPath = result.visualization || result.image_path || result.file_path;
                
                if (vizPath) {
                    const title = this.getVisualizationTitle(vizType, result.variable);
                    this.addVisualization(vizPath, title, vizType, result.metadata || result);
                }
            }
        } catch (error) {
            console.error('Navigation error:', error);
            this.messageHandler.addSystemMessage(`Navigation error: ${error.message}`);
        }
    }

    /**
     * Extract visualization path from HTML response
     */
    extractVisualizationPath(htmlResponse) {
        if (!htmlResponse) return null;
        
        // Look for iframe src in the HTML
        const iframeMatch = htmlResponse.match(/src="([^"]+)"/);
        if (iframeMatch) {
            return iframeMatch[1];
        }
        
        return null;
    }

    /**
     * Extract visualization data from response
     */
    extractVisualizationData(response) {
        const vizData = {};
        
        // Copy pagination info from various possible fields
        if (response.current_page) vizData.current_page = response.current_page;
        if (response.total_pages) vizData.total_pages = response.total_pages;
        if (response.data_summary) vizData.data_summary = response.data_summary;
        if (response.visual_elements) vizData.visual_elements = response.visual_elements;
        if (response.metadata) vizData.metadata = response.metadata;
        
        // Also try to extract from HTML title
        if (response.response) {
            const titleMatch = response.response.match(/<h4[^>]*class="visualization-title"[^>]*>([^<]+)<\/h4>/);
            if (titleMatch) {
                const title = titleMatch[1];
                const pageInfo = this.extractPageInfo(title, vizData);
                vizData.current_page = pageInfo.currentPage;
                vizData.total_pages = pageInfo.totalPages;
            }
        }
        
        return vizData;
    }

    /**
     * Adjust iframe height for optimal display
     */
    adjustIframeHeight(iframe) {
        try {
            const container = iframe.closest('.visualization-container');
            if (!container) return;

            // Calculate optimal height based on viewport and container
            const viewportHeight = window.innerHeight;
            const containerRect = container.getBoundingClientRect();
            const availableHeight = viewportHeight - containerRect.top - 100; // Leave some margin
            
            // Set minimum and maximum heights
            const minHeight = 400;
            const maxHeight = Math.min(800, availableHeight);
            const optimalHeight = Math.max(minHeight, Math.min(600, maxHeight));
            
            iframe.style.height = `${optimalHeight}px`;
            
            // Try to communicate with iframe content for better sizing
            this.optimizeIframeContent(iframe);
            
        } catch (error) {
            console.warn('Could not adjust iframe height:', error);
        }
    }

    /**
     * Enable enhanced interactivity in iframe
     */
    enableIframeInteractivity(iframe) {
        try {
            // Add interaction event listeners
            iframe.addEventListener('mouseenter', () => {
                iframe.style.transform = 'scale(1.01)';
                iframe.style.transition = 'transform 0.2s ease';
                iframe.style.boxShadow = '0 4px 20px rgba(0,0,0,0.15)';
            });

            iframe.addEventListener('mouseleave', () => {
                iframe.style.transform = 'scale(1)';
                iframe.style.boxShadow = 'none';
            });

            // Enable scroll wheel interaction
            iframe.style.pointerEvents = 'auto';
            
        } catch (error) {
            console.warn('Could not enable iframe interactivity:', error);
        }
    }

    /**
     * Optimize iframe content for better display
     */
    optimizeIframeContent(iframe) {
        try {
            // Try to access iframe content (same-origin only)
            const iframeDoc = iframe.contentDocument || iframe.contentWindow.document;
            if (iframeDoc) {
                // Add responsive meta tag if not present
                let viewport = iframeDoc.querySelector('meta[name="viewport"]');
                if (!viewport) {
                    viewport = iframeDoc.createElement('meta');
                    viewport.name = 'viewport';
                    viewport.content = 'width=device-width, initial-scale=1.0, user-scalable=yes';
                    iframeDoc.head.appendChild(viewport);
                }

                // Optimize Plotly charts if present
                if (iframeDoc.querySelector('.plotly-graph-div')) {
                    this.optimizePlotlyInIframe(iframeDoc);
                }
            }
        } catch (error) {
            // Cross-origin restrictions - this is expected for external content
            console.debug('Cannot access iframe content (cross-origin):', error.message);
        }
    }

    /**
     * Optimize Plotly charts within iframe
     */
    optimizePlotlyInIframe(iframeDoc) {
        try {
            const plotlyDivs = iframeDoc.querySelectorAll('.plotly-graph-div');
            plotlyDivs.forEach(div => {
                // Ensure responsive behavior
                div.style.width = '100%';
                div.style.height = 'auto';
                div.style.minHeight = '400px';
                
                // Try to trigger Plotly resize if available
                if (iframeDoc.defaultView && iframeDoc.defaultView.Plotly) {
                    iframeDoc.defaultView.Plotly.Plots.resize(div);
                }
            });
        } catch (error) {
            console.warn('Could not optimize Plotly charts:', error);
        }
    }

    /**
     * Open visualization in fullscreen mode
     */
    openFullscreen(vizPath, title) {
        try {
            console.log('🔍 Opening fullscreen visualization:', vizPath);
            
            // Create fullscreen modal
            const modal = this.createFullscreenModal(vizPath, title);
            document.body.appendChild(modal);
            
            // Show modal with animation
            setTimeout(() => {
                modal.style.opacity = '1';
                modal.classList.add('show');
            }, 10);
            
            // Focus trap and ESC key handler
            this.setupModalKeyHandling(modal);
            
        } catch (error) {
            console.error('Error opening fullscreen:', error);
            this.showToast('Error opening fullscreen view', 'error');
        }
    }

    /**
     * Create fullscreen modal for visualization
     */
    createFullscreenModal(vizPath, title) {
        const modal = DOMHelpers.createElement('div', {
            className: 'visualization-modal',
            style: `
                position: fixed;
                top: 0;
                left: 0;
                width: 100vw;
                height: 100vh;
                background: rgba(0, 0, 0, 0.95);
                z-index: 10000;
                display: flex;
                flex-direction: column;
                opacity: 0;
                transition: opacity 0.3s ease;
            `
        });

        // Modal header
        const header = DOMHelpers.createElement('div', {
            className: 'modal-header',
            style: `
                padding: 1rem 2rem;
                background: rgba(255, 255, 255, 0.1);
                backdrop-filter: blur(10px);
                display: flex;
                justify-content: space-between;
                align-items: center;
                color: white;
            `
        });

        const titleElement = DOMHelpers.createElement('h3', {
            style: 'margin: 0; font-size: 1.2rem;'
        }, title || 'Visualization');

        const closeButton = DOMHelpers.createElement('button', {
            className: 'modal-close-btn',
            style: `
                background: none;
                border: none;
                color: white;
                font-size: 1.5rem;
                cursor: pointer;
                padding: 0.5rem;
                border-radius: 50%;
                transition: all 0.2s ease;
            `,
            title: 'Close (ESC)'
        }, '<i class="fas fa-times"></i>');

        closeButton.addEventListener('click', () => this.closeFullscreen(modal));
        closeButton.addEventListener('mouseenter', () => {
            closeButton.style.background = 'rgba(255, 255, 255, 0.2)';
        });
        closeButton.addEventListener('mouseleave', () => {
            closeButton.style.background = 'none';
        });

        header.appendChild(titleElement);
        header.appendChild(closeButton);

        // Modal content
        const content = DOMHelpers.createElement('div', {
            className: 'modal-content',
            style: `
                flex: 1;
                padding: 1rem;
                display: flex;
                align-items: center;
                justify-content: center;
            `
        });

        const iframe = DOMHelpers.createElement('iframe', {
            src: vizPath,
            style: `
                width: 100%;
                height: 100%;
                border: none;
                border-radius: 8px;
                background: white;
            `,
            frameborder: '0'
        });

        content.appendChild(iframe);
        modal.appendChild(header);
        modal.appendChild(content);

        // Close on background click
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                this.closeFullscreen(modal);
            }
        });

        return modal;
    }

    /**
     * Close fullscreen modal
     */
    closeFullscreen(modal) {
        modal.style.opacity = '0';
        setTimeout(() => {
            if (modal.parentNode) {
                modal.parentNode.removeChild(modal);
            }
        }, 300);
    }

    /**
     * Setup keyboard handling for modal
     */
    setupModalKeyHandling(modal) {
        const keyHandler = (e) => {
            if (e.key === 'Escape') {
                this.closeFullscreen(modal);
                document.removeEventListener('keydown', keyHandler);
            }
        };
        document.addEventListener('keydown', keyHandler);
    }

    /**
     * Download visualization
     */
    downloadVisualization(vizPath, title) {
        try {
            console.log('📥 Downloading visualization:', vizPath);
            
            // Create download link
            const link = document.createElement('a');
            link.href = vizPath;
            link.download = this.generateFilename(title, vizPath);
            link.style.display = 'none';
            
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            
            this.showToast('Download started', 'success');
            
        } catch (error) {
            console.error('Error downloading visualization:', error);
            this.showToast('Error downloading file', 'error');
        }
    }

    /**
     * Generate appropriate filename for download
     */
    generateFilename(title, vizPath) {
        // Clean title for filename
        const cleanTitle = (title || 'visualization')
            .replace(/[^a-zA-Z0-9\s-]/g, '')
            .replace(/\s+/g, '_')
            .toLowerCase();
        
        // Get file extension from path
        const extension = vizPath.split('.').pop() || 'html';
        
        return `${cleanTitle}.${extension}`;
    }

    /**
     * Show toast notification
     */
    showToast(message, type = 'info') {
        const toast = DOMHelpers.createElement('div', {
            className: `toast toast-${type}`,
            style: `
                position: fixed;
                top: 20px;
                right: 20px;
                padding: 1rem 1.5rem;
                background: ${type === 'success' ? '#10b981' : type === 'error' ? '#ef4444' : '#3b82f6'};
                color: white;
                border-radius: 8px;
                z-index: 10001;
                opacity: 0;
                transform: translateX(100%);
                transition: all 0.3s ease;
            `
        }, message);

        document.body.appendChild(toast);

        // Animate in
        setTimeout(() => {
            toast.style.opacity = '1';
            toast.style.transform = 'translateX(0)';
        }, 10);

        // Auto remove
        setTimeout(() => {
            toast.style.opacity = '0';
            toast.style.transform = 'translateX(100%)';
            setTimeout(() => {
                if (toast.parentNode) {
                    toast.parentNode.removeChild(toast);
                }
            }, 300);
        }, 3000);
    }
} 