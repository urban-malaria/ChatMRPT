/**
 * Chat Manager Module
 * Handles all chat functionality including message sending, receiving, and display
 */

import DOMHelpers from '../utils/dom-helpers.js';
import { SessionDataManager } from '../utils/storage.js';
import apiClient from '../utils/api-client.js';

export class ChatManager {
    constructor() {
        this.chatContainer = null;
        this.messageInput = null;
        this.sendButton = null;
        this.isWaitingForResponse = false;
        this.isInitialState = true; // Track if we're in initial centered state
        this.chatContainerElement = null; // Reference to main chat container
        
        this.init();
    }

    /**
     * Initialize chat manager
     */
    init() {
        console.log('🎯 ChatManager initializing...');
        
        this.chatContainer = DOMHelpers.getElementById('chat-messages');
        this.messageInput = DOMHelpers.getElementById('message-input');
        this.sendButton = DOMHelpers.getElementById('send-message');
        this.chatContainerElement = document.querySelector('.chat-container.full-screen');
        
        console.log('📦 Elements found:', {
            chatContainer: !!this.chatContainer,
            messageInput: !!this.messageInput,
            sendButton: !!this.sendButton,
            chatContainerElement: !!this.chatContainerElement
        });
        
        // Set initial state
        this.setInitialState();
        
        this.setupEventListeners();
        this.loadWelcomeMessages();
        
        // Setup automatic error clearing every 2 seconds
        this.setupAutoErrorClearing();
        
        console.log('✅ ChatManager initialized');
    }

    /**
     * Set the initial centered state
     */
    setInitialState() {
        if (this.chatContainerElement) {
            this.chatContainerElement.classList.add('initial-state');
        }
    }

    /**
     * Transition from initial state to normal chat
     */
    transitionToNormalChat() {
        if (!this.isInitialState || !this.chatContainerElement) return;
        
        this.isInitialState = false;
        
        // Add transition class for smooth animation
        this.chatContainerElement.classList.add('transitioning');
        
        setTimeout(() => {
            this.chatContainerElement.classList.remove('initial-state');
            
            setTimeout(() => {
                this.chatContainerElement.classList.remove('transitioning');
            }, 400); // Match transition duration
        }, 100);
    }

    /**
     * Setup event listeners for chat functionality
     */
    setupEventListeners() {
        if (this.sendButton) {
            this.sendButton.addEventListener('click', () => this.sendMessage());
        }

        if (this.messageInput) {
            this.messageInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                    this.sendMessage();
                }
            });

            // Auto-resize textarea
            this.messageInput.addEventListener('input', () => {
                this.autoResizeInput();
            });
        }
    }

    /**
     * Load welcome messages - ChatGPT Style (Simple & Clean)
     */
    loadWelcomeMessages() {
        console.log('🔄 Loading welcome message...');
        
        // Simple, clean welcome message like ChatGPT
        this.addAssistantMessage(`
            <div class="simple-welcome">
                <h2>How can I help with malaria risk analysis today?</h2>
                <p>I can analyze data, create risk maps, identify vulnerable areas, and generate comprehensive reports.</p>
            </div>
        `);
        
        console.log('✅ Welcome message loaded');
    }

    /**
     * Send message to backend
     */
    async sendMessage() {
        if (this.isWaitingForResponse) return;

        const message = this.messageInput.value.trim();
        if (!message) return;

        // Clear any existing error messages when user sends a new message
        this.clearErrorMessages();

        // Transition from initial state to normal chat on first message
        if (this.isInitialState) {
            this.transitionToNormalChat();
        }

        // Clear input and add user message
        this.messageInput.value = '';
        this.autoResizeInput();
        this.addUserMessage(message);

        this.isWaitingForResponse = true;
        this.showTypingIndicator();

        try {
            const sessionData = SessionDataManager.getSessionData();
            const response = await apiClient.sendMessage(message, sessionData.currentLanguage);

            this.hideTypingIndicator();
            this.handleBackendResponse(response);
        } catch (error) {
            this.hideTypingIndicator();
            this.addSystemMessage('Sorry, there was an error processing your request. Please try again.');
            console.error('Error sending message:', error);
        } finally {
            this.isWaitingForResponse = false;
        }
    }

    /**
     * Handle response from backend
     * @param {Object} response - Backend response
     */
    handleBackendResponse(response) {
        console.log('🔄 handleBackendResponse called with:', response);
        
        // Clear any previous error messages on successful response
        if (response.status === 'success') {
            this.clearErrorMessages();
        }
        
        // Process actions first (including visualizations)
        if (response.action) {
            console.log('📊 Processing action:', response.action);
            this.handleAction(response);
        }

        // Then process regular responses
        if (response.response) {
            console.log('💬 Processing response text:', response.response.substring(0, 100) + '...');
            this.addAssistantMessage(response.response);
        }

        // Finally process enhanced explanations
        if (response.enhanced_explanation) {
            console.log('📋 Processing enhanced explanation');
            this.addEnhancedExplanation(response.enhanced_explanation);
        }

        this.scrollToBottom();
    }

    /**
     * Handle special actions from backend
     * @param {Object} response - Backend response with action
     */
    handleAction(response) {
        const action = response.action;
        
        console.log('handleAction called with:', { action, response });
        
        // Handle legacy string actions and new object actions
        const actionType = typeof action === 'string' ? action : action?.type;
        
        console.log('Detected action type:', actionType);
        
        switch (actionType) {
            case 'run_analysis_prompt':
                this.showAnalysisPrompt(action.variables || response.variables);
                break;
            case 'show_visualization':
                // Legacy format - visualization data is directly in response
                console.log('Handling show_visualization action');
                if (response.visualization && response.viz_type) {
                    console.log('Visualization data found:', response.visualization, response.viz_type);
                    const title = this.getVisualizationTitle(response.viz_type, response.variable);
                    this.addVisualization(response.visualization, title, response.viz_type, response.metadata || {});
                } else {
                    console.error('Missing visualization data:', { visualization: response.visualization, viz_type: response.viz_type });
                }
                break;
            case 'visualization_ready':
                this.handleVisualizationResponse(response);
                break;
            case 'variable_explanation':
                this.addVariableExplanation(action.variable, action.explanation);
                break;
            case 'ward_explanation':
                this.addWardExplanation(action.ward, action.explanation);
                break;
            case 'error':
                console.log(`Unknown action type: error`, action);
                
                // Check for specific error messages related to data loading issues
                if (action.response && typeof action.response === 'string') {
                    const lowerResponse = action.response.toLowerCase();
                    if (lowerResponse.includes('no csv data loaded') || 
                        lowerResponse.includes('not properly loaded') || 
                        lowerResponse.includes('data handler not initialized')) {
                        
                        console.warn('⚠️ Data loading error detected. Attempting recovery...');
                        
                        // Show recovery message to user
                        const recoveryMessage = `<div class="alert alert-warning">
                            <p><strong>Data loading issue detected.</strong></p>
                            <p>It appears your data files were uploaded but not properly loaded for analysis.</p>
                            <p>
                                <button class="btn btn-primary btn-sm recovery-action" data-action="reload-data">
                                    Attempt Recovery
                                </button>
                            </p>
                        </div>`;
                        
                        this.appendMessage('system', recoveryMessage);
                        
                        // Add event listener for recovery button
                        document.querySelector('.recovery-action[data-action="reload-data"]').addEventListener('click', () => {
                            this.attemptDataRecovery();
                        });
                    }
                }
                break;
            default:
                console.log('Unknown action type:', actionType, response);
        }
    }

    /**
     * Get visualization title
     * @param {string} vizType - Visualization type
     * @param {string} variable - Variable name (optional)
     * @returns {string} Visualization title
     */
    getVisualizationTitle(vizType, variable = null) {
        console.log('🏷️ Getting title for visualization type:', vizType, 'with variable:', variable);
        
        const titles = {
            'variable_map': variable ? `${variable} Distribution Map` : 'Variable Distribution Map',
            'normalized_map': variable ? `Normalized ${variable} Map` : 'Normalized Variable Map', // Legacy support
            'composite_map': 'Composite Risk Score Map',
            'vulnerability_map': 'Vulnerability Classification Map',
            'vulnerability_plot': 'Vulnerability Ranking Plot',
            'boxplot': 'Risk Score Distribution Plot',
            'vulnerability_boxplot': 'Vulnerability Score Distribution',
            'decision_tree': 'Decision Tree Analysis',
            'urban_extent_map': 'Urban Extent Analysis'
        };
        
        const title = titles[vizType] || `${vizType.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase())} Visualization`;
        console.log('🏷️ Generated title:', title);
        
        return title;
    }

    /**
     * Add user message to chat
     * @param {string} message - User message
     */
    addUserMessage(message) {
        const messageElement = this.createMessageElement('user', message, 'user-message');
        this.appendMessage(messageElement);
        this.scrollToBottom();
    }

    /**
     * Add assistant message to chat
     * @param {string} message - Assistant message
     */
    addAssistantMessage(message) {
        const messageElement = this.createMessageElement('assistant', message, 'assistant-message');
        this.appendMessage(messageElement);
        this.scrollToBottom();
    }

    /**
     * Add system message to chat
     * @param {string} message - System message
     */
    addSystemMessage(message) {
        const messageElement = this.createMessageElement('system', message, 'system-message');
        this.appendMessage(messageElement);
        this.scrollToBottom();
    }

    /**
     * Create message element
     * @param {string} sender - Message sender type
     * @param {string} content - Message content
     * @param {string} className - CSS class name
     * @returns {HTMLElement} Message element
     */
    createMessageElement(sender, content, className) {
        const messageDiv = DOMHelpers.createElement('div', {
            className: `message ${className} new-message`
        });

        const contentDiv = DOMHelpers.createElement('div', {
            className: 'message-content'
        }, content);

        messageDiv.appendChild(contentDiv);
        return messageDiv;
    }

    /**
     * Append message to chat container
     * @param {HTMLElement} messageElement - Message element to append
     */
    appendMessage(messageElement) {
        console.log('📝 Appending message:', messageElement, 'to container:', this.chatContainer);
        
        if (this.chatContainer) {
            this.chatContainer.appendChild(messageElement);
            console.log('✅ Message appended, container children count:', this.chatContainer.children.length);
            
            // Remove animation class after animation completes
            setTimeout(() => {
                DOMHelpers.removeClass(messageElement, 'new-message');
            }, 500);
        } else {
            console.error('❌ No chat container found!');
        }
    }

    /**
     * Show typing indicator
     */
    showTypingIndicator() {
        this.hideTypingIndicator(); // Remove any existing indicator
        
        const typingDiv = DOMHelpers.createElement('div', {
            id: 'typing-indicator',
            className: 'typing-indicator'
        }, `
            <span></span>
            <span></span>
            <span></span>
        `);

        if (this.chatContainer) {
            this.chatContainer.appendChild(typingDiv);
            this.scrollToBottom();
        }
    }

    /**
     * Hide typing indicator
     */
    hideTypingIndicator() {
        const typingIndicator = DOMHelpers.getElementById('typing-indicator');
        if (typingIndicator) {
            typingIndicator.remove();
        }
    }

    /**
     * Auto-resize input textarea
     */
    autoResizeInput() {
        if (this.messageInput) {
            this.messageInput.style.height = 'auto';
            this.messageInput.style.height = Math.min(this.messageInput.scrollHeight, 200) + 'px';
        }
    }

    /**
     * Scroll chat to bottom
     */
    scrollToBottom() {
        if (this.chatContainer) {
            DOMHelpers.scrollToBottom(this.chatContainer);
        }
    }

    /**
     * Show analysis prompt with variables
     * @param {Array<string>} variables - Suggested variables
     */
    showAnalysisPrompt(variables) {
        const promptHTML = `
            <div class="analysis-ready-prompt">
                <p><strong>Your data is ready for analysis!</strong></p>
                <p>I found these key variables: ${variables.join(', ')}</p>
                <button class="btn btn-primary" onclick="window.chatManager.runAnalysis(['${variables.join("', '")}'])">
                    Run Analysis with These Variables
                </button>
                <p class="mt-2"><small>Or type your own variable selection like: "analyze using rainfall, temperature, population"</small></p>
            </div>
        `;
        this.addAssistantMessage(promptHTML);
    }

    /**
     * Handle visualization response
     * @param {Object} response - Backend response with visualization data
     */
    handleVisualizationResponse(response) {
        if (response.visualization) {
            this.addVisualization(
                response.visualization.path,
                response.visualization.title,
                response.visualization.type,
                response.visualization.data || {}
            );
        }
    }

    /**
     * Add enhanced explanation
     * @param {Object} explanation - Enhanced explanation data
     */
    addEnhancedExplanation(explanation) {
        const explanationHTML = `
            <div class="explanation-summary">
                <p><strong>${explanation.summary}</strong></p>
            </div>
            <div class="explanation-detail-toggle">
                <button class="toggle-explanation-detail" onclick="this.nextElementSibling.classList.toggle('show'); this.querySelector('i').style.transform = this.nextElementSibling.classList.contains('show') ? 'rotate(180deg)' : 'rotate(0deg)';">
                    More Details <i class="fas fa-chevron-down"></i>
                </button>
                <div class="explanation-detail-content">
                    ${explanation.details}
                </div>
            </div>
            ${explanation.interactive ? `<div class="explanation-interactive">${explanation.interactive}</div>` : ''}
        `;
        this.addAssistantMessage(explanationHTML);
    }

    /**
     * Add variable explanation
     * @param {string} variable - Variable name
     * @param {string} explanation - Variable explanation
     */
    addVariableExplanation(variable, explanation) {
        const explanationHTML = `
            <div class="variable-explanation">
                <h5>📊 ${variable}</h5>
                <p>${explanation}</p>
            </div>
        `;
        this.addAssistantMessage(explanationHTML);
    }

    /**
     * Add ward explanation
     * @param {string} ward - Ward name
     * @param {string} explanation - Ward explanation
     */
    addWardExplanation(ward, explanation) {
        const explanationHTML = `
            <div class="ward-explanation">
                <h5>📍 ${ward}</h5>
                <p>${explanation}</p>
            </div>
        `;
        this.addAssistantMessage(explanationHTML);
    }

    /**
     * Add visualization to chat - Modern Minimalist Design
     * @param {string} vizPath - Visualization file path
     * @param {string} title - Visualization title
     * @param {string} vizType - Visualization type
     * @param {Object} vizData - Additional visualization data
     */
    addVisualization(vizPath, title, vizType, vizData = {}) {
        console.log('🎨 addVisualization called with:', { vizPath, title, vizType, vizData });
        
        if (!vizPath) {
            console.error('❌ No visualization path provided!');
            this.addSystemMessage('Error: No visualization file path provided.');
            return;
        }
        
        // Create the visualization message element
        const messageDiv = DOMHelpers.createElement('div', {
            className: 'message assistant-message visualization-message new-message'
        });

        const contentDiv = DOMHelpers.createElement('div', {
            className: 'message-content'
        });

        // Extract page info to set proper data attributes
        const pageInfo = this.extractPageInfo(title, vizData);
        
        const container = DOMHelpers.createElement('div', {
            className: 'visualization-container'
        });
        
        // Add essential data attributes for pagination functionality
        container.dataset.vizType = vizType;
        container.dataset.currentPage = pageInfo.currentPage.toString();
        container.dataset.totalPages = pageInfo.totalPages.toString();
        
        // Add additional metadata as data attributes
        if (vizData.items_per_page) {
            container.dataset.vizItemsPerPage = vizData.items_per_page.toString();
        }
        if (vizData.variables_per_page) {
            container.dataset.vizVariablesPerPage = vizData.variables_per_page.toString();
        }
        if (vizData.metadata) {
            Object.keys(vizData.metadata).forEach(key => {
                container.dataset[`viz${key.charAt(0).toUpperCase() + key.slice(1)}`] = vizData.metadata[key];
            });
        }
        
        console.log('📊 Container data attributes set:', container.dataset);

        // Create header with title and controls
        const header = DOMHelpers.createElement('div', {
            className: 'visualization-header'
        });

        const titleElement = DOMHelpers.createElement('h4', {
            className: 'visualization-title'
        }, `📊 ${title}`);

        // Create controls container
        const controls = DOMHelpers.createElement('div', {
            className: 'visualization-controls'
        });

        // Add pagination controls if multi-page visualization
        if (pageInfo.totalPages > 1) {
            const paginationControls = this.createPaginationControls(pageInfo, vizType, vizData);
            controls.appendChild(paginationControls);
        }

        // Add expand button
        const expandBtn = DOMHelpers.createElement('button', {
            className: 'viz-btn',
            title: 'View Fullscreen'
        }, '<i class="fas fa-expand"></i>');

        // Add download button
        const downloadBtn = DOMHelpers.createElement('button', {
            className: 'viz-btn',
            title: 'Download Visualization'
        }, '<i class="fas fa-download"></i>');

        controls.appendChild(expandBtn);
        controls.appendChild(downloadBtn);
        
        header.appendChild(titleElement);
        header.appendChild(controls);

        // Create content container
        const content = DOMHelpers.createElement('div', {
            className: 'visualization-content'
        });

        // Create responsive iframe with proper sizing
        const iframe = DOMHelpers.createElement('iframe', {
            src: vizPath,
            className: 'visualization-iframe',
            frameborder: '0',
            style: 'width: 100%; height: 500px; border: none; border-radius: 8px;'
        });

        // Add loading state
        const loadingDiv = DOMHelpers.createElement('div', {
            className: 'visualization-loading',
            style: 'display: flex; align-items: center; justify-content: center; height: 400px; color: var(--text-secondary);'
        }, '<i class="fas fa-spinner fa-spin"></i> <span style="margin-left: 8px;">Loading visualization...</span>');

        content.appendChild(loadingDiv);
        content.appendChild(iframe);

        // Handle iframe load
        iframe.addEventListener('load', () => {
            console.log('✅ Visualization iframe loaded successfully:', vizPath);
            loadingDiv.style.display = 'none';
            iframe.style.display = 'block';
            
            // Add resize observer for responsive behavior
            if (window.ResizeObserver) {
                const resizeObserver = new ResizeObserver(entries => {
                    for (let entry of entries) {
                        const width = entry.contentRect.width;
                        iframe.style.height = Math.max(400, width * 0.6) + 'px';
                    }
                });
                resizeObserver.observe(container);
            }
        });

        // Handle error loading iframe
        iframe.addEventListener('error', () => {
            console.error('❌ Error loading visualization iframe:', vizPath);
            loadingDiv.innerHTML = '<i class="fas fa-exclamation-triangle"></i> <span style="margin-left: 8px;">Error loading visualization</span>';
            loadingDiv.style.color = 'var(--error-color, #dc2626)';
        });

        // Handle expand functionality
        expandBtn.addEventListener('click', () => {
            this.expandVisualization(vizPath, title);
        });

        // Handle download functionality
        downloadBtn.addEventListener('click', () => {
            this.downloadVisualization(vizPath, title);
        });

        // Assemble the complete visualization
        container.appendChild(header);
        container.appendChild(content);
        contentDiv.appendChild(container);
        messageDiv.appendChild(contentDiv);

        // Add to chat using the proper method
        console.log('📌 Appending visualization to chat:', title);
        this.appendMessage(messageDiv);
        this.scrollToBottom();
        
        console.log('✅ Visualization added successfully:', title);
    }

    /**
     * Expand visualization to fullscreen modal
     */
    expandVisualization(vizPath, title) {
        const modal = document.getElementById('visualizationModal');
        const modalTitle = document.getElementById('visualizationModalLabel');
        const modalBody = document.getElementById('visualizationModalBody');
        
        modalTitle.textContent = title;
        modalBody.innerHTML = `
            <iframe 
                src="${vizPath}" 
                style="width: 100%; height: 100%; border: none; border-radius: 8px;"
                frameborder="0">
            </iframe>
        `;
        
        // Show modal using Bootstrap
        const bsModal = new bootstrap.Modal(modal);
        bsModal.show();
    }

    /**
     * Download visualization
     */
    downloadVisualization(vizPath, title) {
        const link = document.createElement('a');
        link.href = vizPath;
        link.download = `${title.replace(/[^a-z0-9]/gi, '_').toLowerCase()}.html`;
        link.click();
    }

    /**
     * Show notification to user
     */
    showNotification(message, type = 'info') {
        const notification = DOMHelpers.createElement('div', {
            className: `notification notification-${type}`
        });

        const icon = type === 'success' ? 'fa-check-circle' : 
                     type === 'error' ? 'fa-exclamation-circle' : 
                     'fa-info-circle';

        notification.innerHTML = `
            <div class="notification-content">
                <i class="fas ${icon}"></i>
                <span>${message}</span>
            </div>
        `;

        document.body.appendChild(notification);

        // Auto remove after 3 seconds
        setTimeout(() => {
            notification.classList.add('notification-fade-out');
            setTimeout(() => notification.remove(), 300);
        }, 3000);
    }

    /**
     * Track visualization interactions for analytics
     */
    trackVisualizationInteraction(action, data = {}) {
        const event = {
            action,
            timestamp: new Date().toISOString(),
            ...data
        };
        
        // Store in session storage for analytics
        const interactions = JSON.parse(sessionStorage.getItem('viz_interactions') || '[]');
        interactions.push(event);
        sessionStorage.setItem('viz_interactions', JSON.stringify(interactions));
        
        console.log('Visualization interaction tracked:', event);
    }

    /**
     * Clear chat messages
     */
    clearChat() {
        if (this.chatContainer) {
            DOMHelpers.clearChildren(this.chatContainer);
        }
    }

    /**
     * Get chat history
     * @returns {Array} Array of message objects
     */
    getChatHistory() {
        if (!this.chatContainer) return [];

        const messages = [];
        const messageElements = this.chatContainer.querySelectorAll('.message');
        
        messageElements.forEach(element => {
            let type = 'unknown';
            if (element.classList.contains('user-message')) type = 'user';
            else if (element.classList.contains('assistant-message')) type = 'assistant';
            else if (element.classList.contains('system-message')) type = 'system';

            const content = element.querySelector('.message-content');
            if (content) {
                messages.push({
                    type: type,
                    content: content.innerHTML,
                    timestamp: Date.now()
                });
            }
        });

        return messages;
    }

    /**
     * Run analysis (public method)
     * @param {Array<string>} variables - Variables for analysis
     */
    async runAnalysis(variables = null) {
        try {
            this.showTypingIndicator();
            this.addSystemMessage('Starting analysis...');
            
            const response = await apiClient.runAnalysis(variables);
            this.hideTypingIndicator();
            
            if (response.status === 'success') {
                this.addSystemMessage('Analysis completed successfully!');
                SessionDataManager.updateSessionData({ analysisComplete: true });
                
                if (response.message) {
                    this.addAssistantMessage(response.message);
                }
            } else {
                this.addSystemMessage('Analysis failed. Please try again.');
            }
        } catch (error) {
            this.hideTypingIndicator();
            this.addSystemMessage('Error running analysis. Please try again.');
            console.error('Analysis error:', error);
        }
    }

    /**
     * Add report generation interface to chat
     */
    addReportGenerationInterface() {
        const container = DOMHelpers.createElement('div', {
            className: 'report-generation-container message'
        });

        const header = DOMHelpers.createElement('div', {
            className: 'report-generation-header'
        });

        const title = DOMHelpers.createElement('h3', {
            className: 'report-generation-title'
        }, 'Generate Analysis Report');

        const subtitle = DOMHelpers.createElement('p', {
            className: 'text-sm text-gray-600 mt-1'
        }, 'Create a comprehensive report of your malaria risk analysis');

        header.appendChild(title);
        header.appendChild(subtitle);

        // Report options section
        const optionsSection = DOMHelpers.createElement('div', {
            className: 'report-options-section'
        });

        const optionsTitle = DOMHelpers.createElement('h4', {
            className: 'text-lg font-semibold mb-3 text-gray-800'
        }, 'Report Options');

        const formatOptions = DOMHelpers.createElement('div', {
            className: 'format-options mb-4'
        });

        const formatLabel = DOMHelpers.createElement('label', {
            className: 'block text-sm font-medium text-gray-700 mb-2'
        }, 'Select Format:');

        const formatSelect = DOMHelpers.createElement('select', {
            id: 'report-format-select',
            className: 'w-full rounded-md border-gray-300 shadow-sm focus:border-teal-500 focus:ring-teal-500 py-2 px-3'
        });

        const formats = [
            { value: 'html', label: 'Interactive HTML Dashboard', icon: '🌐' },
            { value: 'pdf', label: 'PDF Document', icon: '📄' },
            { value: 'markdown', label: 'Markdown Report', icon: '📝' }
        ];

        formats.forEach(format => {
            const option = DOMHelpers.createElement('option', {
                value: format.value
            }, `${format.icon} ${format.label}`);
            formatSelect.appendChild(option);
        });

        formatOptions.appendChild(formatLabel);
        formatOptions.appendChild(formatSelect);

        // Report sections checkboxes
        const sectionsOptions = DOMHelpers.createElement('div', {
            className: 'sections-options mb-4'
        });

        const sectionsLabel = DOMHelpers.createElement('label', {
            className: 'block text-sm font-medium text-gray-700 mb-2'
        }, 'Include Sections:');

        const sections = [
            { id: 'executive_summary', label: 'Executive Summary', required: true },
            { id: 'data_overview', label: 'Data Overview & Methodology' },
            { id: 'vulnerability_rankings', label: 'Vulnerability Rankings', required: true },
            { id: 'spatial_analysis', label: 'Spatial Analysis & Patterns' },
            { id: 'variable_analysis', label: 'Variable Relationships' },
            { id: 'recommendations', label: 'Recommendations & Insights' },
            { id: 'technical_appendix', label: 'Technical Appendix' }
        ];

        const sectionsGrid = DOMHelpers.createElement('div', {
            className: 'grid grid-cols-1 md:grid-cols-2 gap-2'
        });

        sections.forEach(section => {
            const checkboxWrapper = DOMHelpers.createElement('div', {
                className: 'flex items-center'
            });

            const checkbox = DOMHelpers.createElement('input', {
                type: 'checkbox',
                id: `section-${section.id}`,
                className: 'rounded border-gray-300 text-teal-600 focus:ring-teal-500',
                checked: section.required,
                disabled: section.required
            });

            const checkboxLabel = DOMHelpers.createElement('label', {
                for: `section-${section.id}`,
                className: `ml-2 text-sm ${section.required ? 'font-semibold' : ''}`
            }, section.label + (section.required ? ' (Required)' : ''));

            checkboxWrapper.appendChild(checkbox);
            checkboxWrapper.appendChild(checkboxLabel);
            sectionsGrid.appendChild(checkboxWrapper);
        });

        sectionsOptions.appendChild(sectionsLabel);
        sectionsOptions.appendChild(sectionsGrid);

        // Quick options buttons
        const quickOptions = DOMHelpers.createElement('div', {
            className: 'quick-options mb-4'
        });

        const quickLabel = DOMHelpers.createElement('label', {
            className: 'block text-sm font-medium text-gray-700 mb-2'
        }, 'Quick Options:');

        const quickButtonsContainer = DOMHelpers.createElement('div', {
            className: 'flex flex-wrap gap-2'
        });

        const quickButtons = [
            { id: 'basic-report', label: '📊 Basic Report', sections: ['executive_summary', 'vulnerability_rankings'] },
            { id: 'detailed-report', label: '📈 Detailed Analysis', sections: ['executive_summary', 'data_overview', 'vulnerability_rankings', 'spatial_analysis', 'variable_analysis'] },
            { id: 'full-report', label: '📋 Complete Report', sections: sections.map(s => s.id) }
        ];

        quickButtons.forEach(button => {
            const btn = DOMHelpers.createElement('button', {
                type: 'button',
                className: 'report-option-btn px-3 py-2 text-sm',
                'data-sections': JSON.stringify(button.sections)
            }, button.label);

            btn.addEventListener('click', () => {
                // Update checkboxes based on selection
                sections.forEach(section => {
                    const checkbox = document.getElementById(`section-${section.id}`);
                    if (checkbox && !checkbox.disabled) {
                        checkbox.checked = button.sections.includes(section.id);
                    }
                });

                // Visual feedback
                quickButtonsContainer.querySelectorAll('.report-option-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
            });

            quickButtonsContainer.appendChild(btn);
        });

        quickOptions.appendChild(quickLabel);
        quickOptions.appendChild(quickButtonsContainer);

        // Assembly the options section
        optionsSection.appendChild(optionsTitle);
        optionsSection.appendChild(formatOptions);
        optionsSection.appendChild(sectionsOptions);
        optionsSection.appendChild(quickOptions);

        // Action buttons
        const actionsSection = DOMHelpers.createElement('div', {
            className: 'actions-section'
        });

        const generateBtn = DOMHelpers.createElement('button', {
            type: 'button',
            className: 'generate-report-btn w-full',
            id: 'generate-report-btn'
        });
        generateBtn.innerHTML = '<i class="fas fa-cogs"></i> Generate Report';

        const previewBtn = DOMHelpers.createElement('button', {
            type: 'button',
            className: 'report-option-btn w-full mt-2',
            id: 'preview-report-btn'
        });
        previewBtn.innerHTML = '<i class="fas fa-eye"></i> Preview Report';

        actionsSection.appendChild(generateBtn);
        actionsSection.appendChild(previewBtn);

        // Progress section (initially hidden)
        const progressSection = DOMHelpers.createElement('div', {
            className: 'report-progress',
            id: 'report-progress'
        });

        const progressBar = DOMHelpers.createElement('div', {
            className: 'progress-bar'
        });

        const progressFill = DOMHelpers.createElement('div', {
            className: 'progress-fill',
            id: 'progress-fill'
        });

        const progressText = DOMHelpers.createElement('div', {
            className: 'progress-text',
            id: 'progress-text'
        }, 'Initializing report generation...');

        progressBar.appendChild(progressFill);
        progressSection.appendChild(progressBar);
        progressSection.appendChild(progressText);

        // Add event listeners
        generateBtn.addEventListener('click', () => {
            this.generateReport();
        });

        previewBtn.addEventListener('click', () => {
            this.previewReport();
        });

        // Assemble the container
        container.appendChild(header);
        container.appendChild(optionsSection);
        container.appendChild(actionsSection);
        container.appendChild(progressSection);

        // Add to chat with animation
        container.classList.add('new-message');
        this.chatMessages.appendChild(container);
        this.scrollToBottom();

        return container;
    }

    /**
     * Generate report based on user selections
     */
    async generateReport() {
        const formatSelect = document.getElementById('report-format-select');
        const generateBtn = document.getElementById('generate-report-btn');
        const progressSection = document.getElementById('report-progress');
        const progressFill = document.getElementById('progress-fill');
        const progressText = document.getElementById('progress-text');

        if (!formatSelect) {
            this.showNotification('Report interface not found', 'error');
            return;
        }

        // Collect selected sections
        const selectedSections = [];
        document.querySelectorAll('input[id^="section-"]:checked').forEach(checkbox => {
            const sectionId = checkbox.id.replace('section-', '');
            selectedSections.push(sectionId);
        });

        // Prepare form data
        const formData = new FormData();
        formData.append('report_format', formatSelect.value);
        formData.append('detail_level', 'standard');
        selectedSections.forEach(section => {
            formData.append('custom_sections', section);
        });

        try {
            // Show progress
            generateBtn.disabled = true;
            generateBtn.classList.add('loading');
            progressSection.style.display = 'block';
            
            // Simulate progress updates
            this.updateProgress(progressFill, progressText, 0, 'Preparing report data...');
            
            setTimeout(() => {
                this.updateProgress(progressFill, progressText, 30, 'Generating visualizations...');
            }, 500);

            setTimeout(() => {
                this.updateProgress(progressFill, progressText, 60, 'Compiling analysis results...');
            }, 1500);

            setTimeout(() => {
                this.updateProgress(progressFill, progressText, 80, 'Formatting report...');
            }, 2500);

            // Make the actual request
            const response = await fetch('/generate_report', {
                method: 'POST',
                body: formData
            });

            const result = await response.json();

            if (result.status === 'success') {
                this.updateProgress(progressFill, progressText, 100, 'Report generated successfully!');
                
                setTimeout(() => {
                    // Add download link to chat
                    this.addReportDownloadMessage(result);
                    
                    // Reset interface
                    this.resetReportInterface(generateBtn, progressSection);
                }, 1000);
                
                this.showNotification('Report generated successfully!', 'success');
                this.trackVisualizationInteraction('report_generated', { 
                    format: formatSelect.value, 
                    sections: selectedSections.length 
                });
            } else {
                throw new Error(result.message || 'Report generation failed');
            }

        } catch (error) {
            console.error('Report generation error:', error);
            this.updateProgress(progressFill, progressText, 0, 'Error generating report');
            
            setTimeout(() => {
                this.resetReportInterface(generateBtn, progressSection);
            }, 2000);
            
            this.showNotification(`Report generation failed: ${error.message}`, 'error');
            this.trackVisualizationInteraction('report_generation_failed', { error: error.message });
        }
    }

    /**
     * Preview report content
     */
    async previewReport() {
        const formatSelect = document.getElementById('report-format-select');
        
        // Collect selected sections
        const selectedSections = [];
        document.querySelectorAll('input[id^="section-"]:checked').forEach(checkbox => {
            const sectionId = checkbox.id.replace('section-', '');
            selectedSections.push(sectionId);
        });

        try {
            const response = await fetch('/preview_report', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    format: formatSelect.value,
                    sections: selectedSections,
                    type: 'preview'
                })
            });

            const result = await response.json();

            if (result.status === 'success') {
                // Create preview modal or add preview to chat
                this.showReportPreview(result.preview_html, selectedSections.length);
                this.showNotification('Report preview generated', 'info');
            } else {
                throw new Error(result.message || 'Preview generation failed');
            }

        } catch (error) {
            console.error('Preview generation error:', error);
            this.showNotification(`Preview failed: ${error.message}`, 'error');
        }
    }

    /**
     * Update progress bar and text
     */
    updateProgress(progressFill, progressText, percentage, text) {
        if (progressFill) {
            progressFill.style.width = `${percentage}%`;
        }
        if (progressText) {
            progressText.textContent = text;
        }
    }

    /**
     * Reset report interface after completion
     */
    resetReportInterface(generateBtn, progressSection) {
        generateBtn.disabled = false;
        generateBtn.classList.remove('loading');
        progressSection.style.display = 'none';
    }

    /**
     * Add report download message to chat
     */
    addReportDownloadMessage(result) {
        const container = DOMHelpers.createElement('div', {
            className: 'message assistant-message'
        });

        const content = DOMHelpers.createElement('div', {
            className: 'message-content'
        });

        content.innerHTML = `
            <div class="report-success">
                <div class="flex items-center mb-3">
                    <i class="fas fa-check-circle text-green-500 text-xl mr-2"></i>
                    <h4 class="text-lg font-semibold">Report Generated Successfully!</h4>
                </div>
                <p class="mb-3">Your ${result.format.toUpperCase()} report has been generated and is ready for download.</p>
                <div class="download-actions">
                    <a href="${result.download_url}" 
                       class="inline-flex items-center px-4 py-2 bg-teal-600 text-white rounded-md hover:bg-teal-700 transition-colors mr-2"
                       download>
                        <i class="fas fa-download mr-2"></i>
                        Download Report
                    </a>
                    <button onclick="window.open('${result.download_url}', '_blank')" 
                            class="inline-flex items-center px-4 py-2 bg-gray-600 text-white rounded-md hover:bg-gray-700 transition-colors">
                        <i class="fas fa-external-link-alt mr-2"></i>
                        View Online
                    </button>
                </div>
            </div>
        `;

        container.appendChild(content);
        container.classList.add('new-message');
        this.chatMessages.appendChild(container);
        this.scrollToBottom();
    }

    /**
     * Show report preview in modal or chat
     */
    showReportPreview(previewHtml, sectionCount) {
        // Create preview message
        const container = DOMHelpers.createElement('div', {
            className: 'message assistant-message'
        });

        const content = DOMHelpers.createElement('div', {
            className: 'message-content'
        });

        content.innerHTML = `
            <div class="report-preview">
                <div class="flex items-center justify-between mb-3">
                    <h4 class="text-lg font-semibold">Report Preview</h4>
                    <span class="text-sm text-gray-500">${sectionCount} sections</span>
                </div>
                <div class="preview-content max-h-96 overflow-y-auto border rounded-md p-3 bg-gray-50">
                    ${previewHtml}
                </div>
                <p class="mt-3 text-sm text-gray-600">
                    <i class="fas fa-info-circle mr-1"></i>
                    This is a preview of your report. Generate the full report for complete formatting and downloadable version.
                </p>
            </div>
        `;

        container.appendChild(content);
        container.classList.add('new-message');
        this.chatMessages.appendChild(container);
        this.scrollToBottom();
    }

    /**
     * Extract page information from visualization title or metadata
     */
    extractPageInfo(title, vizData) {
        let currentPage = 1;
        let totalPages = 1;
        
        console.log('🔍 Extracting page info from:', { title, vizData });
        
        // Check multiple sources for pagination data - prioritize direct fields
        if (vizData && vizData.current_page) {
            currentPage = vizData.current_page;
            console.log('📊 Found current_page in vizData:', currentPage);
        }
        if (vizData && vizData.total_pages) {
            totalPages = vizData.total_pages;
            console.log('📊 Found total_pages in vizData:', totalPages);
        }
        
        // Check nested metadata
        if (vizData && vizData.metadata) {
            if (vizData.metadata.current_page) {
                currentPage = vizData.metadata.current_page;
                console.log('📊 Found current_page in nested metadata:', currentPage);
            }
            if (vizData.metadata.total_pages) {
                totalPages = vizData.metadata.total_pages;
                console.log('📊 Found total_pages in nested metadata:', totalPages);
            }
        }
        
        // Fallback: extract from title (e.g., "Page 1 of 5" or "1 of 5")  
        if (totalPages === 1 && title) {
            // More flexible regex patterns
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
                    console.log('📄 Extracted from title:', { currentPage, totalPages });
                    break;
                }
            }
        }
        
        // REMOVED: The hardcoded default that was causing the "1/5" issue!
        // No more hardcoded defaults - use actual data from backend
        
        console.log('✅ Final page info:', { currentPage, totalPages });
        return { currentPage, totalPages };
    }

    /**
     * Create pagination controls
     */
    createPaginationControls(pageInfo, vizType, vizData) {
        const paginationDiv = DOMHelpers.createElement('div', {
            className: 'pagination-controls'
        });
        
        // Previous button
        const prevBtn = DOMHelpers.createElement('button', {
            className: `viz-btn pagination-btn ${pageInfo.currentPage <= 1 ? 'disabled' : ''}`,
            title: 'Previous Page',
            disabled: pageInfo.currentPage <= 1
        }, '<i class="fas fa-chevron-left"></i>');
        
        // Page indicator
        const pageIndicator = DOMHelpers.createElement('span', {
            className: 'page-indicator'
        }, `Page ${pageInfo.currentPage} of ${pageInfo.totalPages}`);
        
        // Next button  
        const nextBtn = DOMHelpers.createElement('button', {
            className: `viz-btn pagination-btn ${pageInfo.currentPage >= pageInfo.totalPages ? 'disabled' : ''}`,
            title: 'Next Page',
            disabled: pageInfo.currentPage >= pageInfo.totalPages
        }, '<i class="fas fa-chevron-right"></i>');
        
        // Add event listeners for navigation
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

    /**
     * Navigate visualization pages
     */
    async navigateVisualization(vizType, direction, pageInfo, vizData) {
        try {
            console.log('🔄 Navigating visualization:', { vizType, direction, pageInfo, vizData });
            
            let endpoint = '/navigate_visualization'; // Default generic endpoint
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
                requestBody = {
                    direction: direction,
                    current_page: pageInfo.currentPage
                };
                console.log('📡 Using composite map specific endpoint');
            } else if (vizType === 'boxplot' || vizType === 'vulnerability_boxplot') {
                endpoint = '/navigate_boxplot';
                requestBody = {
                    direction: direction,
                    current_page: pageInfo.currentPage
                };
                console.log('📡 Using boxplot specific endpoint');
            } else {
                console.log('📡 Using generic navigation endpoint');
            }
            
            console.log('📤 Request to:', endpoint, 'with payload:', requestBody);
            
            const response = await fetch(endpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(requestBody)
            });
            
            console.log('📥 Response status:', response.status);
            
            if (!response.ok) {
                const errorText = await response.text();
                console.error('❌ HTTP Error:', response.status, errorText);
                throw new Error(`HTTP ${response.status}: ${errorText}`);
            }
            
            const result = await response.json();
            console.log('✅ Navigation response:', result);
            
            if (result.status === 'success') {
                console.log('🎯 Navigation successful, updating visualization');
                
                // For specific endpoints, the response structure might be different
                let vizPath = result.visualization || result.image_path || result.file_path;
                
                if (vizPath) {
                    // Replace the current visualization with the new one
                    const title = this.getVisualizationTitle(vizType, result.variable);
                    this.addVisualization(vizPath, title, vizType, result.metadata || result);
                    this.showNotification(`Navigated to page ${result.current_page || 'next'}`, 'success');
                } else {
                    console.error('❌ No visualization path in response:', result);
                    this.showNotification('No visualization received', 'warning');
                }
            } else {
                console.error('❌ Navigation failed:', result);
                this.showNotification(result.message || 'Failed to navigate visualization', 'error');
            }
        } catch (error) {
            console.error('💥 Error navigating visualization:', error);
            this.showNotification(`Navigation error: ${error.message}`, 'error');
        }
    }

    /**
     * Setup automatic error clearing
     */
    setupAutoErrorClearing() {
        // TEMPORARILY DISABLED - Clear errors every 2 seconds
        // setInterval(() => {
        //     const removed = this.clearErrorMessages();
        //     if (removed > 0) {
        //         console.log(`🔄 Auto-cleared ${removed} error messages`);
        //     }
        // }, 2000);
        
        console.log('⏰ Auto error clearing DISABLED for testing');
    }

    /**
     * Clear error messages - AGGRESSIVE VERSION (FIXED)
     */
    clearErrorMessages() {
        if (!this.chatContainer) return;
        
        console.log('🧹 AGGRESSIVE error clearing starting...');
        
        let removedCount = 0;
        
        // Method 1: Target specific error content patterns
        const allMessages = this.chatContainer.querySelectorAll('.message, .system-message, .assistant-message, .error-message, .alert, .notification');
        
        allMessages.forEach(message => {
            const textContent = message.textContent ? message.textContent.toLowerCase() : '';
            
            // Skip if this is clearly a success message
            const successIndicators = [
                'excellent!',
                'successfully',
                'success!',
                'completed successfully',
                'loaded successfully',
                'files are now loaded',
                'analysis completed',
                'generated successfully',
                'ready for analysis',
                'everything is ready',
                'all files',
                '✅',
                '🚀',
                '📊',
                '🗺️'
            ];
            
            const hasSuccessIndicator = successIndicators.some(indicator => textContent.includes(indicator));
            if (hasSuccessIndicator) {
                console.log('🛡️ PROTECTING success message from removal:', textContent.substring(0, 50) + '...');
                return; // Skip this message
            }
            
            // Only remove actual error patterns
            const errorPatterns = [
                'unexpected error occurred',
                'please refresh the page',
                'error occurred. please refresh',
                'an error occurred',
                'something went wrong',
                'please try again',
                'error processing',
                'failed to',
                'could not',
                'before generating a report',
                'run an analysis before',
                'analysis before generating',
                'run the analysis',
                'please run an analysis'
            ];
            
            const hasErrorPattern = errorPatterns.some(pattern => textContent.includes(pattern));
            
            if (hasErrorPattern) {
                console.log('🧹 REMOVING error message:', textContent.substring(0, 100) + '...');
                message.remove();
                removedCount++;
            }
        });
        
        // Method 2: Target by CSS classes (only clear actual error classes)
        const errorElements = this.chatContainer.querySelectorAll(`
            .error-message, .alert-danger, .notification-error,
            [class*="error"]:not([class*="success"]):not([class*="ready"]):not([class*="analysis"])
        `);
        
        errorElements.forEach(element => {
            const text = element.textContent ? element.textContent.toLowerCase() : '';
            // Double-check it's not a success message
            if (!text.includes('success') && !text.includes('excellent') && !text.includes('completed') && !text.includes('ready')) {
                console.log('🧹 REMOVING error element by class:', element.className);
                element.remove();
                removedCount++;
            }
        });
        
        // Method 3: Search for specific error text content directly (more selective)
        const walker = document.createTreeWalker(
            this.chatContainer,
            NodeFilter.SHOW_TEXT,
            null,
            false
        );
        
        const textNodesToRemove = [];
        let node;
        
        while (node = walker.nextNode()) {
            const text = node.textContent.toLowerCase();
            if ((text.includes('unexpected error occurred') || 
                text.includes('please refresh the page') ||
                text.includes('run an analysis before')) &&
                !text.includes('success') && !text.includes('excellent') && !text.includes('completed')) {
                textNodesToRemove.push(node.parentElement);
            }
        }
        
        textNodesToRemove.forEach(element => {
            if (element && element.remove) {
                console.log('🧹 REMOVING element by text content:', element.textContent.substring(0, 100));
                element.remove();
                removedCount++;
            }
        });
        
        console.log(`✅ AGGRESSIVE clearing completed - removed ${removedCount} error elements`);
        
        return removedCount;
    }

    /**
     * Manual error clearing for debugging - can be called from console
     */
    manualClearErrors() {
        console.log('🔧 MANUAL ERROR CLEARING INITIATED');
        
        // Clear everything that might be an error
        const selectors = [
            '.message', '.system-message', '.assistant-message', '.error-message',
            '.alert', '.alert-danger', '.alert-warning', '.notification',
            '[class*="error"]', '[class*="alert"]', '[class*="warning"]'
        ];
        
        let totalRemoved = 0;
        
        selectors.forEach(selector => {
            const elements = document.querySelectorAll(selector);
            elements.forEach(element => {
                const text = element.textContent ? element.textContent.toLowerCase() : '';
                if (text.includes('error') || text.includes('refresh') || text.includes('analysis before')) {
                    console.log('🔧 MANUAL REMOVE:', text.substring(0, 50));
                    element.remove();
                    totalRemoved++;
                }
            });
        });
        
        console.log(`🔧 MANUAL CLEARING DONE - removed ${totalRemoved} elements`);
        return totalRemoved;
    }

    /**
     * Manual fix for analysis state - for debugging
     */
    async fixAnalysisState() {
        try {
            console.log('🔧 Attempting to fix analysis state...');
            
            const response = await fetch('/debug/fix_analysis_state', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' }
            });
            
            const result = await response.json();
            console.log('🔧 Fix analysis state result:', result);
            
            if (result.status === 'success') {
                this.showNotification('Analysis state fixed! You can now generate reports.', 'success');
                this.addSystemMessage('✅ Analysis state has been fixed! You can now generate reports and visualizations.');
                return true;
            } else {
                this.showNotification(result.message || 'Failed to fix analysis state', 'error');
                this.addSystemMessage(`❌ ${result.message || 'Failed to fix analysis state'}`);
                return false;
            }
        } catch (error) {
            console.error('💥 Error fixing analysis state:', error);
            this.showNotification(`Fix failed: ${error.message}`, 'error');
            return false;
        }
    }

    /**
     * Debug function to check session state
     */
    async checkSessionState() {
        try {
            console.log('🔍 Checking session state...');
            
            const response = await fetch('/debug/session_state', {
                method: 'GET',
                headers: { 'Content-Type': 'application/json' }
            });
            
            const result = await response.json();
            console.log('🔍 Session state result:', result);
            
            if (result.status === 'success') {
                console.table(result.session_state);
                this.addSystemMessage(`📊 Session State Debug:\n${JSON.stringify(result.session_state, null, 2)}`);
                return result.session_state;
            } else {
                console.error('❌ Failed to get session state:', result.message);
                return null;
            }
        } catch (error) {
            console.error('💥 Error checking session state:', error);
            return null;
        }
    }

    /**
     * Test analysis completion message
     */
    async testAnalysisMessage() {
        try {
            console.log('🧪 Testing analysis completion message...');
            
            const response = await fetch('/send_message', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    message: 'run analysis'
                })
            });
            
            const result = await response.json();
            console.log('🧪 Analysis test result:', result);
            
            // Check if action is analysis_complete
            if (result.action === 'analysis_complete') {
                console.log('✅ Analysis completion detected!');
                this.addSystemMessage('✅ Analysis completion action detected! Session should be updated.');
            } else {
                console.log('ℹ️ Analysis action:', result.action);
                this.addSystemMessage(`ℹ️ Analysis action: ${result.action}`);
            }
            
            return result;
        } catch (error) {
            console.error('💥 Error testing analysis message:', error);
            return null;
        }
    }

    /**
     * Attempt to recover from data loading errors by forcing a reload of the session data
     */
    async attemptDataRecovery() {
        console.log('🔄 Attempting data recovery...');
        this.appendMessage('system', `<div class="alert alert-info">Attempting to reload your data... Please wait.</div>`);
        
        // Call a backend endpoint to force reload session data
        fetch('/reload_session_data', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            }
        })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'success') {
                this.appendMessage('system', `<div class="alert alert-success">
                    <p><strong>Recovery successful!</strong></p>
                    <p>${data.message}</p>
                    <p>You can now try running the analysis again.</p>
                    <button class="btn btn-primary btn-sm" onclick="document.getElementById('message-input').value='Run the analysis'; document.getElementById('send-message').click();">
                        Run Analysis Now
                    </button>
                </div>`);
            } else {
                this.appendMessage('system', `<div class="alert alert-danger">
                    <p><strong>Recovery failed.</strong></p>
                    <p>${data.message || 'Please try uploading your files again.'}</p>
                </div>`);
            }
        })
        .catch(error => {
            console.error('Error during recovery attempt:', error);
            this.appendMessage('system', `<div class="alert alert-danger">
                <p><strong>Recovery failed.</strong></p>
                <p>An error occurred while attempting recovery. Please try uploading your files again.</p>
            </div>`);
        });
    }
}

// Create and export singleton instance
const chatManager = new ChatManager();

// Make available globally for button handlers
window.chatManager = chatManager;

// Make manual error clearing available globally for debugging
window.clearErrors = () => {
    console.log('🔧 Global clearErrors() called');
    if (window.chatManager && window.chatManager.manualClearErrors) {
        return window.chatManager.manualClearErrors();
    } else {
        console.error('❌ ChatManager not available');
        return 0;
    }
};

// Make analysis state fix available globally for debugging
window.fixAnalysisState = () => {
    console.log('🔧 Global fixAnalysisState() called');
    if (window.chatManager && window.chatManager.fixAnalysisState) {
        return window.chatManager.fixAnalysisState();
    } else {
        console.error('❌ ChatManager not available');
        return false;
    }
};

// Make session state check available globally for debugging
window.checkSessionState = () => {
    console.log('🔍 Global checkSessionState() called');
    if (window.chatManager && window.chatManager.checkSessionState) {
        return window.chatManager.checkSessionState();
    } else {
        console.error('❌ ChatManager not available');
        return null;
    }
};

// Make analysis test available globally for debugging
window.testAnalysisMessage = () => {
    console.log('🧪 Global testAnalysisMessage() called');
    if (window.chatManager && window.chatManager.testAnalysisMessage) {
        return window.chatManager.testAnalysisMessage();
    } else {
        console.error('❌ ChatManager not available');
        return null;
    }
};

export default chatManager; 