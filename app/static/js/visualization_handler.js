/**
 * Visualization Handler for ChatMRPT
 * Handles all visualization display with iframe support
 */

class VisualizationHandler {
    constructor() {
        this.visualizations = new Map();
        this.initializeHandlers();
        this.setupSpinnerStyles();
    }

    setupSpinnerStyles() {
        // Add CSS animation for spinner if not already present
        if (!document.querySelector('#viz-spinner-style')) {
            const style = document.createElement('style');
            style.id = 'viz-spinner-style';
            style.textContent = `
                @keyframes spin {
                    to { transform: rotate(360deg); }
                }
            `;
            document.head.appendChild(style);
        }
    }

    initializeHandlers() {
        // Listen for visualization messages
        document.addEventListener('DOMContentLoaded', () => {
            this.setupMessageObserver();
        });
    }

    setupMessageObserver() {
        // Watch for new messages in the chat - check multiple possible selectors
        const selectors = [
            '#root',  // React root container
            '.chat-container',
            '.messages',
            '.message-list',
            '[class*="message"]',
            '#messages-container',
            '.messages-container',
            '[data-messages]'
        ];

        let chatContainer = null;
        for (const selector of selectors) {
            chatContainer = document.querySelector(selector);
            if (chatContainer) {
                console.log('Found chat container with selector:', selector);
                break;
            }
        }

        if (!chatContainer) {
            // If no container found, try to process the entire document
            console.log('Using document body as fallback container');
            chatContainer = document.body;
        }

        // Create observer for new messages
        const observer = new MutationObserver((mutations) => {
            mutations.forEach((mutation) => {
                mutation.addedNodes.forEach((node) => {
                    if (node.nodeType === 1) { // Element node
                        this.processVisualizationContent(node);
                    }
                });
            });
        });

        observer.observe(chatContainer, {
            childList: true,
            subtree: true
        });

        // Process existing content
        this.processAllVisualizationContent();
    }

    processAllVisualizationContent() {
        // Find all existing messages that might contain visualizations
        const messageSelectors = [
            '.message',
            '[data-message]',
            '.assistant-message',
            '[class*="message"]',
            'div[class*="assistant"]',
            'div[class*="response"]',
            'div[class*="chat"]'
        ];

        let messages = [];
        messageSelectors.forEach(selector => {
            const found = document.querySelectorAll(selector);
            found.forEach(msg => {
                if (!messages.includes(msg)) {
                    messages.push(msg);
                }
            });
        });

        console.log(`Processing ${messages.length} potential message elements`);
        messages.forEach(message => {
            this.processVisualizationContent(message);
        });
    }

    processVisualizationContent(element) {
        // Check if element contains visualization URLs
        const content = element.textContent || element.innerText || '';

        // Pattern to match serve_viz_file URLs
        const vizUrlPattern = /\/serve_viz_file\/[a-zA-Z0-9\-_\/]+\.[a-zA-Z]+/g;
        const matches = content.match(vizUrlPattern);

        if (matches && matches.length > 0) {
            // Check if we've already processed this element
            if (element.dataset.vizProcessed === 'true') {
                return;
            }

            console.log('🎨 Found visualization URLs:', matches);
            console.log('🎨 Creating visualization frames with Explain buttons...');
            this.createVisualizationFrames(element, matches);
            element.dataset.vizProcessed = 'true';
            console.log('🎨 Visualization processed successfully');
        }

        // Also check for specific visualization indicators
        if (content.includes('visualization_generated') ||
            content.includes('map has been created') ||
            content.includes('chart has been generated') ||
            content.includes('plot has been created')) {

            // Extract URLs from the content
            const urls = this.extractVisualizationUrls(content);
            if (urls.length > 0) {
                this.createVisualizationFrames(element, urls);
                element.dataset.vizProcessed = 'true';
            }
        }
    }

    extractVisualizationUrls(content) {
        const urls = [];

        // Pattern for serve_viz_file URLs
        const pattern = /\/serve_viz_file\/[a-zA-Z0-9\-_\/]+\.[a-zA-Z]+/g;
        const matches = content.match(pattern);

        if (matches) {
            matches.forEach(url => {
                // Ensure URL is complete
                if (!url.startsWith('http')) {
                    // Get base URL
                    const baseUrl = window.location.origin;
                    urls.push(baseUrl + url);
                } else {
                    urls.push(url);
                }
            });
        }

        return urls;
    }

    // Function to explain visualization (py-sidebot style)
    async explainVisualization(vizUrl, container) {
        console.log('🔵 DEBUG: explainVisualization called');
        console.log('🔵 DEBUG: vizUrl:', vizUrl);
        console.log('🔵 DEBUG: Container element:', container);

        // Find or create explanation area
        let explanationDiv = container.querySelector('.viz-explanation');
        if (!explanationDiv) {
            console.log('🔵 DEBUG: Creating new explanation div');
            explanationDiv = document.createElement('div');
            explanationDiv.className = 'viz-explanation';
            explanationDiv.style.cssText = `
                padding: 16px;
                background: #f9fafb;
                border-top: 1px solid #e5e7eb;
                display: none;
            `;
            container.appendChild(explanationDiv);
        } else {
            console.log('🔵 DEBUG: Using existing explanation div');
        }

        // Show loading state
        explanationDiv.innerHTML = `
            <div style="display: flex; align-items: center; gap: 8px;">
                <div class="spinner" style="
                    width: 16px;
                    height: 16px;
                    border: 2px solid #e5e7eb;
                    border-top-color: #6366f1;
                    border-radius: 50%;
                    animation: spin 1s linear infinite;
                "></div>
                <span style="color: #6b7280;">Analyzing visualization...</span>
            </div>
        `;
        explanationDiv.style.display = 'block';

        try {
            // Extract visualization type and path
            const pathMatch = vizUrl.match(/\/serve_viz_file\/(.*)/);
            const vizPath = pathMatch ? pathMatch[1] : '';
            console.log('🔵 DEBUG: Extracted vizPath:', vizPath);

            // Determine visualization type from filename
            let vizType = 'visualization';
            if (vizPath.includes('vulnerability_map')) vizType = 'vulnerability_map';
            else if (vizPath.includes('box_plot')) vizType = 'box_plot';
            else if (vizPath.includes('pca_map')) vizType = 'pca_map';
            else if (vizPath.includes('composite_score')) vizType = 'composite_score_maps';
            else if (vizPath.includes('variable_distribution')) vizType = 'variable_distribution';
            else if (vizPath.includes('settlement')) vizType = 'settlement_map';
            else if (vizPath.includes('itn')) vizType = 'itn_map';
            else if (vizPath.includes('tpr')) vizType = 'tpr_map';
            else if (vizPath.includes('evi')) vizType = 'evi_map';

            console.log('🔵 DEBUG: Determined vizType:', vizType);

            // Build request payload
            const requestPayload = {
                viz_url: vizUrl,
                viz_type: vizType,
                viz_path: vizPath,
                visualization_path: vizPath  // Add alternative field name
            };

            console.log('🔵 DEBUG: Request payload:', JSON.stringify(requestPayload, null, 2));
            console.log('🔵 DEBUG: Calling /explain_visualization endpoint...');

            // Call backend to get explanation
            const response = await fetch('/explain_visualization', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(requestPayload)
            });

            console.log('🔵 DEBUG: Response status:', response.status);
            console.log('🔵 DEBUG: Response headers:', response.headers);

            const data = await response.json();
            console.log('🔵 DEBUG: Response data:', JSON.stringify(data, null, 2));

            if (data.status === 'success' && data.explanation) {
                console.log('🎉 DEBUG: SUCCESS - Got AI explanation');
                console.log('🎉 DEBUG: Explanation length:', data.explanation.length);
                console.log('🎉 DEBUG: First 200 chars:', data.explanation.substring(0, 200));

                // Check if it's a fallback message
                if (data.explanation.includes('This visualization shows malaria risk analysis')) {
                    console.log('⚠️ DEBUG: WARNING - Got FALLBACK message, not real AI explanation!');
                    console.log('⚠️ DEBUG: Full explanation:', data.explanation);
                } else {
                    console.log('✅ DEBUG: Got REAL AI-powered explanation!');
                }

                // Show explanation with nice formatting
                explanationDiv.innerHTML = `
                    <div style="display: flex; align-items: start; gap: 12px;">
                        <div style="font-size: 24px;">💡</div>
                        <div style="flex: 1;">
                            <h4 style="margin: 0 0 8px 0; color: #111827; font-weight: 600;">
                                AI Explanation
                            </h4>
                            <div style="color: #374151; line-height: 1.6;">
                                ${this.formatExplanation(data.explanation)}
                            </div>
                        </div>
                    </div>
                `;
            } else {
                console.log('❌ DEBUG: FAILED - No explanation in response');
                console.log('❌ DEBUG: Response status:', data.status);
                console.log('❌ DEBUG: Error message:', data.message);

                // Show error or fallback
                explanationDiv.innerHTML = `
                    <div style="color: #ef4444;">
                        ⚠️ Unable to generate explanation. ${data.message || 'Please try again.'}
                    </div>
                `;
            }
        } catch (error) {
            console.error('❌ DEBUG: Exception in explainVisualization:', error);
            console.error('❌ DEBUG: Error stack:', error.stack);

            explanationDiv.innerHTML = `
                <div style="color: #ef4444;">
                    ⚠️ Error explaining visualization: ${error.message}
                </div>
            `;
        }
    }

    // Helper function to format explanation text
    formatExplanation(text) {
        // Convert markdown-like formatting to HTML
        return text
            .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
            .replace(/\*(.*?)\*/g, '<em>$1</em>')
            .replace(/\n\n/g, '</p><p>')
            .replace(/\n/g, '<br>')
            .replace(/^/, '<p>')
            .replace(/$/, '</p>');
    }

    createVisualizationFrames(element, urls) {
        // Create container for visualizations
        const vizContainer = document.createElement('div');
        vizContainer.className = 'visualization-container mt-4';
        vizContainer.style.cssText = `
            margin-top: 16px;
            border-radius: 8px;
            overflow: hidden;
            border: 1px solid #e5e7eb;
        `;

        urls.forEach((url, index) => {
            // Ensure complete URL
            const fullUrl = url.startsWith('http') ? url : window.location.origin + url;

            // Create frame wrapper
            const frameWrapper = document.createElement('div');
            frameWrapper.className = 'viz-frame-wrapper';
            frameWrapper.style.cssText = `
                margin-bottom: ${index < urls.length - 1 ? '16px' : '0'};
            `;

            // Create header
            const header = this.createVisualizationHeader(fullUrl, index + 1, urls.length);
            frameWrapper.appendChild(header);

            // Create iframe
            const iframe = this.createVisualizationIframe(fullUrl);
            frameWrapper.appendChild(iframe);

            vizContainer.appendChild(frameWrapper);
        });

        // Insert visualization container after the message
        if (element.querySelector('.viz-frame-wrapper')) {
            // Already has visualization, update it
            const existing = element.querySelector('.visualization-container');
            if (existing) {
                existing.replaceWith(vizContainer);
            }
        } else {
            // Add new visualization
            element.appendChild(vizContainer);
        }
    }

    createVisualizationHeader(url, current, total) {
        const header = document.createElement('div');
        header.className = 'viz-header';
        header.style.cssText = `
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 12px 16px;
            background: #f9fafb;
            border-bottom: 1px solid #e5e7eb;
        `;

        // Title
        const title = document.createElement('div');
        title.style.cssText = `
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 14px;
            font-weight: 500;
            color: #374151;
        `;

        // Icon
        const icon = document.createElement('span');
        icon.innerHTML = `
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <rect x="3" y="3" width="18" height="18" rx="2" ry="2"></rect>
                <line x1="9" y1="9" x2="15" y2="9"></line>
                <line x1="9" y1="15" x2="15" y2="15"></line>
            </svg>
        `;

        // Extract filename from URL
        const filename = url.split('/').pop().replace(/\.[^/.]+$/, '').replace(/_/g, ' ');
        const titleText = document.createElement('span');
        titleText.textContent = filename || 'Visualization';

        title.appendChild(icon);
        title.appendChild(titleText);

        // Page indicator if multiple
        if (total > 1) {
            const pageIndicator = document.createElement('span');
            pageIndicator.style.cssText = `
                margin-left: 8px;
                padding: 2px 8px;
                background: #ddd6fe;
                color: #6b21a8;
                border-radius: 4px;
                font-size: 12px;
            `;
            pageIndicator.textContent = `${current}/${total}`;
            title.appendChild(pageIndicator);
        }

        // Actions
        const actions = document.createElement('div');
        actions.style.cssText = `
            display: flex;
            gap: 8px;
        `;

        // Add Explain button (py-sidebot style)
        const explainBtn = document.createElement('button');
        explainBtn.innerHTML = '✨ Explain';
        explainBtn.style.cssText = `
            padding: 4px 12px;
            background: #6366f1;
            color: white;
            border: 1px solid #6366f1;
            border-radius: 6px;
            cursor: pointer;
            font-size: 12px;
            font-weight: 500;
            transition: background 0.2s;
        `;
        explainBtn.onmouseover = () => explainBtn.style.background = '#5558e3';
        explainBtn.onmouseout = () => explainBtn.style.background = '#6366f1';
        explainBtn.onclick = () => {
            console.log('🟢 DEBUG: Explain button clicked!');
            console.log('🟢 DEBUG: URL to explain:', url);
            console.log('🟢 DEBUG: Header element:', header);

            // Find the parent frame wrapper which will contain the explanation area
            const frameWrapper = header.parentElement;
            console.log('🟢 DEBUG: Frame wrapper element:', frameWrapper);

            // Call explain visualization
            this.explainVisualization(url, frameWrapper);
        };

        // Fullscreen button
        const fullscreenBtn = this.createActionButton('Fullscreen', () => {
            const iframe = header.nextElementSibling;
            if (iframe.requestFullscreen) {
                iframe.requestFullscreen();
            }
        });

        // Open in new tab button
        const newTabBtn = this.createActionButton('Open in New Tab', () => {
            window.open(url, '_blank');
        });

        actions.appendChild(explainBtn);
        actions.appendChild(fullscreenBtn);
        actions.appendChild(newTabBtn);

        header.appendChild(title);
        header.appendChild(actions);

        return header;
    }

    createActionButton(label, onClick) {
        const button = document.createElement('button');
        button.textContent = label;
        button.onclick = onClick;
        button.style.cssText = `
            padding: 4px 12px;
            background: white;
            border: 1px solid #d1d5db;
            border-radius: 6px;
            font-size: 12px;
            color: #4b5563;
            cursor: pointer;
            transition: all 0.2s;
        `;

        button.onmouseover = () => {
            button.style.background = '#f3f4f6';
            button.style.borderColor = '#9ca3af';
        };

        button.onmouseout = () => {
            button.style.background = 'white';
            button.style.borderColor = '#d1d5db';
        };

        return button;
    }

    createVisualizationIframe(url) {
        const iframe = document.createElement('iframe');
        iframe.src = url;
        iframe.className = 'visualization-iframe';
        iframe.style.cssText = `
            width: 100%;
            height: 600px;
            border: none;
            display: block;
        `;

        // Set sandbox attributes for security but allow necessary features
        iframe.setAttribute('sandbox', 'allow-scripts allow-same-origin allow-popups allow-forms allow-downloads');
        iframe.setAttribute('loading', 'lazy');

        // Add loading state
        const loadingDiv = document.createElement('div');
        loadingDiv.className = 'viz-loading';
        loadingDiv.style.cssText = `
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            text-align: center;
            color: #6b7280;
        `;
        loadingDiv.innerHTML = `
            <div style="margin-bottom: 8px;">
                <svg class="animate-spin" style="width: 24px; height: 24px; margin: 0 auto;" viewBox="0 0 24 24">
                    <circle style="opacity: 0.25;" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" fill="none"></circle>
                    <path style="opacity: 0.75;" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                </svg>
            </div>
            <div style="font-size: 14px;">Loading visualization...</div>
        `;

        const container = document.createElement('div');
        container.style.position = 'relative';
        container.style.minHeight = '600px';
        container.appendChild(loadingDiv);
        container.appendChild(iframe);

        // Handle iframe load
        iframe.onload = () => {
            loadingDiv.style.display = 'none';

            // Try to adjust height based on content
            try {
                const iframeDoc = iframe.contentDocument || iframe.contentWindow.document;
                const height = iframeDoc.body.scrollHeight;
                if (height > 0 && height < 2000) {
                    iframe.style.height = `${Math.min(height + 20, 800)}px`;
                }
            } catch (e) {
                // Cross-origin, can't access content
                console.log('Cannot access iframe content (cross-origin)');
            }
        };

        iframe.onerror = () => {
            loadingDiv.innerHTML = `
                <div style="color: #ef4444;">
                    <svg style="width: 24px; height: 24px; margin: 0 auto;" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path>
                    </svg>
                    <div style="margin-top: 8px;">Failed to load visualization</div>
                </div>
            `;
        };

        return container;
    }

    // Method to manually trigger visualization processing
    processMessage(messageElement) {
        this.processVisualizationContent(messageElement);
    }
}

// Initialize the visualization handler
const vizHandler = new VisualizationHandler();

// Export for use in other modules
window.VisualizationHandler = VisualizationHandler;
window.vizHandler = vizHandler;

// Also process on various events
document.addEventListener('message-received', (e) => {
    if (e.detail && e.detail.element) {
        vizHandler.processMessage(e.detail.element);
    }
});

document.addEventListener('chat-message-added', (e) => {
    if (e.detail && e.detail.element) {
        vizHandler.processMessage(e.detail.element);
    }
});

// Process when new content is added via AJAX
if (window.jQuery) {
    $(document).ajaxComplete(function() {
        setTimeout(() => {
            vizHandler.processAllVisualizationContent();
        }, 500);
    });
}

// Periodic check for new visualizations (fallback for React)
setInterval(() => {
    // Look for iframes that might be visualizations
    const iframes = document.querySelectorAll('iframe');
    iframes.forEach(iframe => {
        // Skip if already enhanced
        if (iframe.dataset.vizEnhanced === 'true') return;

        const src = iframe.src || '';
        // Only process visualization iframes
        if (!src.includes('/serve_viz_file/')) return;

        const parent = iframe.parentElement;
        if (!parent) return;

        // Check if already has our header
        const prevSibling = iframe.previousElementSibling;
        if (prevSibling && prevSibling.classList.contains('viz-header')) {
            iframe.dataset.vizEnhanced = 'true';
            return;
        }

        console.log('🎨 Enhancing visualization:', src);

        // Create a wrapper div
        const wrapper = document.createElement('div');
        wrapper.className = 'viz-enhanced-wrapper';
        wrapper.style.cssText = `
            border: 1px solid #e5e7eb;
            border-radius: 8px;
            overflow: hidden;
            margin: 16px 0;
        `;

        // Insert wrapper before iframe
        parent.insertBefore(wrapper, iframe);

        // Create header
        const header = vizHandler.createVisualizationHeader(src, 1, 1);
        header.style.marginBottom = '0';

        // Add header to wrapper
        wrapper.appendChild(header);

        // Move iframe into wrapper (after header)
        wrapper.appendChild(iframe);

        // Reset iframe styling
        iframe.style.cssText = `
            width: 100%;
            height: 600px;
            border: none;
            display: block;
        `;

        // Mark as enhanced
        iframe.dataset.vizEnhanced = 'true';
        console.log('✅ Enhanced iframe with Explain button');
    });

}, 2000); // Check every 2 seconds

console.log('🚀 Visualization Handler v2 initialized with periodic scanning');
console.log('🚀 vizHandler available globally:', window.vizHandler);
console.log('🚀 Ready to process visualizations. Look for "✨ Explain" buttons.');
