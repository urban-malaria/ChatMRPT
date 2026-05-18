/**
 * Direct Visualization Injector for ChatMRPT
 * Specifically targets and replaces visualization text with iframes
 */

(function() {
    console.log('Visualization Injector starting...');

    // Function to explain visualization (py-sidebot style)
    async function explainVisualization(vizUrl, container) {
        console.log('Explaining visualization:', vizUrl);

        // Find or create explanation area
        let explanationDiv = container.querySelector('.viz-explanation');
        if (!explanationDiv) {
            explanationDiv = document.createElement('div');
            explanationDiv.className = 'viz-explanation';
            explanationDiv.style.cssText = `
                padding: 16px;
                background: #f9fafb;
                border-top: 1px solid #e5e7eb;
                display: none;
            `;
            container.appendChild(explanationDiv);
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

        // Add CSS animation for spinner
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

        try {
            // Extract visualization type and path
            const pathMatch = vizUrl.match(/\/serve_viz_file\/(.*)/);
            const vizPath = pathMatch ? pathMatch[1] : '';

            // Determine visualization type from filename
            let vizType = 'visualization';
            if (vizPath.includes('vulnerability_map')) vizType = 'vulnerability_map';
            else if (vizPath.includes('box_plot')) vizType = 'box_plot';
            else if (vizPath.includes('pca_map')) vizType = 'pca_map';
            else if (vizPath.includes('composite_score')) vizType = 'composite_score_maps';
            else if (vizPath.includes('variable_distribution')) vizType = 'variable_distribution';
            else if (vizPath.includes('settlement')) vizType = 'settlement_map';
            else if (vizPath.includes('itn')) vizType = 'itn_map';

            // Call backend to get explanation
            const response = await fetch('/explain_visualization', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    viz_url: vizUrl,
                    viz_type: vizType,
                    viz_path: vizPath
                })
            });

            const data = await response.json();

            if (data.status === 'success' && data.explanation) {
                // Show explanation with nice formatting
                explanationDiv.innerHTML = `
                    <div style="display: flex; align-items: start; gap: 12px;">
                        <div style="font-size: 24px;">💡</div>
                        <div style="flex: 1;">
                            <h4 style="margin: 0 0 8px 0; color: #111827; font-weight: 600;">
                                AI Explanation
                            </h4>
                            <div style="color: #374151; line-height: 1.6;">
                                ${formatExplanation(data.explanation)}
                            </div>
                        </div>
                    </div>
                `;
            } else {
                // Show error or fallback
                explanationDiv.innerHTML = `
                    <div style="color: #ef4444;">
                        ⚠️ Unable to generate explanation. ${data.message || 'Please try again.'}
                    </div>
                `;
            }
        } catch (error) {
            console.error('Error explaining visualization:', error);
            explanationDiv.innerHTML = `
                <div style="color: #ef4444;">
                    ⚠️ Error explaining visualization: ${error.message}
                </div>
            `;
        }
    }

    // Helper function to format explanation text
    function formatExplanation(text) {
        // Convert markdown-like formatting to HTML
        return text
            .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
            .replace(/\*(.*?)\*/g, '<em>$1</em>')
            .replace(/\n\n/g, '</p><p>')
            .replace(/\n/g, '<br>')
            .replace(/^/, '<p>')
            .replace(/$/, '</p>');
    }

    function injectVisualizations() {
        // Find all elements that might contain the visualization text
        const allElements = document.querySelectorAll('div, p, span');

        allElements.forEach(element => {
            // Skip if already processed
            if (element.dataset.vizInjected === 'true') return;

            const text = element.textContent || '';

            // Check for the specific pattern we see in the screenshot
            if (text.includes('Variable Distribution Generated') ||
                text.includes('visualization_generated') ||
                text.includes('/serve_viz_file/')) {

                console.log('Found visualization text to inject:', text.substring(0, 100));

                // Extract the URL from the text
                const urlMatch = text.match(/\/serve_viz_file\/[a-zA-Z0-9\-_\/]+\.[a-zA-Z]+/);

                if (urlMatch) {
                    const vizUrl = urlMatch[0];
                    const fullUrl = window.location.origin + vizUrl;

                    console.log('Injecting iframe for URL:', fullUrl);

                    // Create the iframe container
                    const container = document.createElement('div');
                    container.style.cssText = `
                        margin: 16px 0;
                        border: 1px solid #e5e7eb;
                        border-radius: 8px;
                        overflow: hidden;
                        background: white;
                    `;

                    // Create header
                    const header = document.createElement('div');
                    header.style.cssText = `
                        padding: 12px 16px;
                        background: #f9fafb;
                        border-bottom: 1px solid #e5e7eb;
                        display: flex;
                        justify-content: space-between;
                        align-items: center;
                    `;

                    const title = document.createElement('div');
                    title.style.cssText = `
                        font-weight: 500;
                        color: #374151;
                    `;
                    title.textContent = '📊 Visualization';

                    const actions = document.createElement('div');
                    actions.style.cssText = 'display: flex; gap: 8px;';

                    // Add Explain button (py-sidebot style)
                    const explainBtn = document.createElement('button');
                    explainBtn.innerHTML = '✨ Explain';
                    explainBtn.style.cssText = `
                        padding: 6px 12px;
                        background: #6366f1;
                        color: white;
                        border: 1px solid #6366f1;
                        border-radius: 6px;
                        cursor: pointer;
                        font-size: 14px;
                        font-weight: 500;
                        transition: background 0.2s;
                    `;
                    explainBtn.onmouseover = () => explainBtn.style.background = '#5558e3';
                    explainBtn.onmouseout = () => explainBtn.style.background = '#6366f1';
                    explainBtn.onclick = () => explainVisualization(fullUrl, container);

                    const openBtn = document.createElement('button');
                    openBtn.textContent = 'Open in New Tab';
                    openBtn.style.cssText = `
                        padding: 6px 12px;
                        background: white;
                        border: 1px solid #d1d5db;
                        border-radius: 6px;
                        cursor: pointer;
                        font-size: 14px;
                    `;
                    openBtn.onclick = () => window.open(fullUrl, '_blank');

                    actions.appendChild(explainBtn);
                    actions.appendChild(openBtn);
                    header.appendChild(title);
                    header.appendChild(actions);

                    // Create iframe
                    const iframe = document.createElement('iframe');
                    iframe.src = fullUrl;
                    iframe.style.cssText = `
                        width: 100%;
                        height: 600px;
                        border: none;
                        display: block;
                    `;
                    iframe.setAttribute('sandbox', 'allow-scripts allow-same-origin allow-popups allow-forms allow-downloads');

                    // Assemble container
                    container.appendChild(header);
                    container.appendChild(iframe);

                    // Replace the text element with the iframe
                    element.style.display = 'none';
                    element.parentNode.insertBefore(container, element.nextSibling);

                    // Mark as processed
                    element.dataset.vizInjected = 'true';

                    console.log('Iframe injected successfully');
                } else {
                    console.log('No URL found in visualization text');
                }
            }
        });
    }

    // Run immediately
    injectVisualizations();

    // Run periodically to catch new content
    setInterval(injectVisualizations, 1000);

    // Also run on DOM changes
    const observer = new MutationObserver(() => {
        injectVisualizations();
    });

    // Start observing when DOM is ready
    if (document.body) {
        observer.observe(document.body, {
            childList: true,
            subtree: true
        });
    } else {
        document.addEventListener('DOMContentLoaded', () => {
            observer.observe(document.body, {
                childList: true,
                subtree: true
            });
        });
    }

    console.log('Visualization Injector initialized');
})();
