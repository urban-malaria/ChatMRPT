/**
 * ChatManager - Refactored & Clean
 * Orchestrates chat functionality through focused modules
 * 
 * BEFORE: 1899 lines of spaghetti code
 * AFTER: ~100 lines of clean orchestration
 */

import DOMHelpers from '../utils/dom-helpers.js';
import { MessageHandler } from './core/message-handler.js';
import { VisualizationManager } from './visualization/visualization-manager.js';
import { MethodSelector } from './analysis/method-selector.js';
import variableDisplayManager from '../utils/variable-display.js';

export class ChatManager {
    constructor() {
        this.currentSessionId = this.generateSessionId();
        this.isInitialState = true;
        this.chatContainerElement = null;
        
        this.init();
    }

    /**
     * Initialize ChatManager - Clean & Focused
     */
    init() {
        console.log('🎯 ChatManager initializing (REFACTORED)...');
        
        // Get DOM elements
        const chatContainer = DOMHelpers.getElementById('chat-messages');
        const messageInput = DOMHelpers.getElementById('message-input');
        const sendButton = DOMHelpers.getElementById('send-message');
        this.chatContainerElement = document.querySelector('.chat-container.full-screen');
        
        if (!chatContainer || !messageInput || !sendButton) {
            console.error('❌ Required DOM elements not found!');
            return;
        }
        
        // Initialize focused modules
        this.messageHandler = new MessageHandler(chatContainer, messageInput, sendButton);
        this.visualizationManager = new VisualizationManager(this.messageHandler);
        this.methodSelector = new MethodSelector(this.messageHandler);
        
        // Set initial state
        this.setInitialState();
        
        // Setup response handling
        this.setupResponseHandling();
        
        // CRITICAL FIX: Setup resize handler for scroll container
        this.setupResizeHandler();
        
        // Load welcome message
        this.loadWelcomeMessage();
        
        // CRITICAL FIX: Initialize scroll container after everything is loaded
        setTimeout(() => {
            this.recalculateScrollContainer();
        }, 100);
        
        console.log('✅ ChatManager initialized (CLEAN & MODULAR)');
    }

    /**
     * Set initial centered state
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
        
        // CRITICAL FIX: Store scroll position before transition
        const messagesContainer = document.getElementById('chat-messages');
        const scrollTop = messagesContainer ? messagesContainer.scrollTop : 0;
        
        this.isInitialState = false;
        this.chatContainerElement.classList.add('transitioning');
        
        setTimeout(() => {
            this.chatContainerElement.classList.remove('initial-state');
            setTimeout(() => {
                this.chatContainerElement.classList.remove('transitioning');
                
                // CRITICAL FIX: Restore scroll position after transition
                if (messagesContainer) {
                    messagesContainer.scrollTop = scrollTop;
                    // Ensure messages are still scrollable after transition
                    this.messageHandler.scrollToBottom();
                }
            }, 400);
        }, 100);
    }

    /**
     * Setup response handling for non-visualization actions
     */
    setupResponseHandling() {
        // Handle automatic messages from upload workflow
        document.addEventListener('sendAutomaticMessage', (event) => {
            const { message } = event.detail;
            console.log('🎯 Automatic message triggered:', message);
            
            // Send the automatic message through the message handler
            if (this.messageHandler) {
                this.messageHandler.sendMessage(message);
            }
        });
        
        document.addEventListener('messageResponse', async (event) => {
            const { response } = event.detail;
            
            console.log('🔍 DEBUG: messageResponse event received:', response);
            console.log('🔍 DEBUG: response.response field:', response.response);
            console.log('🔍 DEBUG: response.message field:', response.message);
            
            // ✅ FIX: Skip processing if this was already handled via streaming
            if (response.streamed || response.streaming_handled) {
                console.log('🔍 DEBUG: Skipping messageResponse - already handled via streaming');
                return;
            }
            
            // Transition from initial state on first message
            if (this.isInitialState) {
                this.transitionToNormalChat();
            }
            
            // Handle clarification responses
            if (response.status === 'clarification_needed' && response.response) {
                this.messageHandler.addAssistantMessage(response.response);
                
                // If suggestions are provided, show them as clickable options
                if (response.suggestions && response.suggestions.length > 0) {
                    const suggestionsHTML = `
                        <div class="clarification-suggestions mt-2">
                            <p class="mb-2"><small>Quick options:</small></p>
                            <div class="suggestion-buttons">
                                ${response.suggestions.map(suggestion => 
                                    `<button class="btn btn-sm btn-outline-primary suggestion-btn me-2 mb-2" data-suggestion="${suggestion}">${suggestion}</button>`
                                ).join(' ')}
                            </div>
                        </div>
                    `;
                    this.messageHandler.addAssistantMessage(suggestionsHTML);
                    
                    // Add click handlers for suggestions
                    setTimeout(() => {
                        document.querySelectorAll('.suggestion-btn').forEach(btn => {
                            btn.addEventListener('click', () => {
                                const suggestion = btn.getAttribute('data-suggestion');
                                this.messageHandler.sendMessage(`Use ${suggestion}`);
                            });
                        });
                    }, 100);
                }
                return;
            }
            
            // Handle error responses
            if (response.status === 'error' && response.response) {
                this.messageHandler.addAssistantMessage(response.response);
                
                // If it's a handled error, it already has helpful guidance
                if (response.error_handled) {
                    return;
                }
                return;
            }
            
            // Handle regular responses with intelligent variable display
            if (response.response) {
                console.log('🔍 DEBUG: Processing response.response');
                const enhancedResponse = await this.enhanceResponseWithVariableNames(response.response, response.variables_used);
                this.messageHandler.addAssistantMessage(enhancedResponse);
            } else if (response.message) {
                console.log('🔍 DEBUG: Using response.message as fallback');
                const enhancedResponse = await this.enhanceResponseWithVariableNames(response.message, response.variables_used);
                this.messageHandler.addAssistantMessage(enhancedResponse);
            } else {
                console.log('🔍 DEBUG: No response text found');
            }
            
            // Handle special actions (non-visualization)
            if (response.action) {
                await this.handleSpecialActions(response);
            }
            
            // Handle enhanced explanations
            if (response.enhanced_explanation) {
                await this.addEnhancedExplanation(response.enhanced_explanation);
            }
        });
    }

    /**
     * Enhance response text by replacing variable codes with display names
     */
    async enhanceResponseWithVariableNames(responseText, variablesUsed = []) {
        try {
            if (!responseText || !variablesUsed || variablesUsed.length === 0) {
                return responseText;
            }
            
            // Use the variable display manager to replace codes with names
            return await variableDisplayManager.replaceVariableCodesInText(responseText, variablesUsed);
            
        } catch (error) {
            console.warn('Error enhancing response with variable names:', error);
            return responseText; // Return original on error
        }
    }

    /**
     * Handle special actions that aren't visualization-related
     */
    async handleSpecialActions(response) {
        const action = response.action;
        const actionType = typeof action === 'string' ? action : action?.type;
        
        switch (actionType) {
            case 'run_analysis_prompt':
                await this.showAnalysisPrompt(action.variables || response.variables);
                break;
            case 'variable_explanation':
                await this.addVariableExplanation(action.variable, action.explanation);
                break;
            case 'ward_explanation':
                this.addWardExplanation(action.ward, action.explanation);
                break;
            case 'error':
                this.handleErrorAction(action);
                break;
        }
    }

    /**
     * Show analysis prompt with variables using intelligent display
     * DEPRECATED: Now using conversational approach instead of buttons
     */
    async showAnalysisPrompt(variables) {
        // Deprecated - analysis should now be requested conversationally
        // Just show variables without analysis button
        try {
            const variableDisplays = await Promise.all(
                variables.map(async (varCode) => {
                    const displayName = await variableDisplayManager.getDisplayName(varCode);
                    return { code: varCode, display: displayName };
                })
            );
            
            const displayNames = variableDisplays.map(v => v.display);
            
            const promptHTML = `
                <div class="analysis-ready-prompt">
                    <p><strong>Your data is ready for analysis!</strong></p>
                    <p>I found these key variables: ${displayNames.join(', ')}</p>
                    <p class="mt-2"><small>You can now ask me to run analysis like: "Run composite analysis" or "Analyze using ${displayNames.slice(0, 3).join(', ')}"</small></p>
                </div>
            `;
            this.messageHandler.addAssistantMessage(promptHTML);
            
        } catch (error) {
            console.warn('Error showing analysis prompt:', error);
            // Simplified fallback without button
            const promptHTML = `
                <div class="analysis-ready-prompt">
                    <p><strong>Your data is ready for analysis!</strong></p>
                    <p>Variables found: ${variables.join(', ')}</p>
                    <p class="mt-2"><small>Ask me to run analysis like: "Run composite analysis"</small></p>
                </div>
            `;
            this.messageHandler.addAssistantMessage(promptHTML);
        }
    }

    /**
     * Load welcome message
     */
    loadWelcomeMessage() {
        const welcomeHTML = `
            <div class="simple-welcome">
                <h2>How can I help with malaria risk analysis today?</h2>
                <p>I can analyze data, create risk maps, identify vulnerable areas, and generate comprehensive reports.</p>
            </div>
        `;
        this.messageHandler.addAssistantMessage(welcomeHTML);
    }

    /**
     * Add enhanced explanation with intelligent variable display
     */
    async addEnhancedExplanation(explanation) {
        try {
            // Extract any variable codes mentioned in the explanation
            const variableCodes = this.extractVariableCodesFromText(explanation.summary + ' ' + explanation.details);
            
            // Enhance the explanation text
            const enhancedSummary = await variableDisplayManager.replaceVariableCodesInText(
                explanation.summary, 
                variableCodes
            );
            const enhancedDetails = await variableDisplayManager.replaceVariableCodesInText(
                explanation.details, 
                variableCodes
            );
            
            const explanationHTML = `
                <div class="explanation-summary">
                    <p><strong>${enhancedSummary}</strong></p>
                </div>
                <div class="explanation-detail-toggle">
                    <button class="toggle-explanation-detail" onclick="this.nextElementSibling.classList.toggle('show'); this.querySelector('i').style.transform = this.nextElementSibling.classList.contains('show') ? 'rotate(180deg)' : 'rotate(0deg)';">
                        More Details <i class="fas fa-chevron-down"></i>
                    </button>
                    <div class="explanation-detail-content">
                        ${enhancedDetails}
                    </div>
                </div>
                ${explanation.interactive ? `<div class="explanation-interactive">${explanation.interactive}</div>` : ''}
            `;
            this.messageHandler.addAssistantMessage(explanationHTML);
            
        } catch (error) {
            console.warn('Error adding enhanced explanation:', error);
            // Fallback to original
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
            this.messageHandler.addAssistantMessage(explanationHTML);
        }
    }

    /**
     * Add variable explanation with intelligent display
     */
    async addVariableExplanation(variable, explanation) {
        try {
            const displayName = await variableDisplayManager.getDisplayName(variable);
            const metadata = await variableDisplayManager.getMetadata(variable);
            
            const explanationHTML = `
                <div class="variable-explanation">
                    <h5>📊 ${displayName}</h5>
                    <p><strong>Description:</strong> ${metadata.description}</p>
                    <p><strong>Unit:</strong> ${metadata.unit}</p>
                    <p><strong>Category:</strong> ${metadata.category}</p>
                    <p>${explanation}</p>
                </div>
            `;
            this.messageHandler.addAssistantMessage(explanationHTML);
            
        } catch (error) {
            console.warn('Error adding variable explanation:', error);
            // Fallback
            const explanationHTML = `
                <div class="variable-explanation">
                    <h5>📊 ${variable}</h5>
                    <p>${explanation}</p>
                </div>
            `;
            this.messageHandler.addAssistantMessage(explanationHTML);
        }
    }

    /**
     * Extract potential variable codes from text (simple heuristic)
     */
    extractVariableCodesFromText(text) {
        // Look for patterns that might be variable codes
        const patterns = [
            /\b[a-z_]+\b/g,  // Simple underscore patterns
            /\b[A-Z_]+\b/g,  // Uppercase patterns
        ];
        
        const codes = new Set();
        patterns.forEach(pattern => {
            const matches = text.match(pattern) || [];
            matches.forEach(match => {
                if (match.length > 2 && (match.includes('_') || /^[A-Z]+$/.test(match))) {
                    codes.add(match);
                }
            });
        });
        
        return Array.from(codes);
    }

    /**
     * Add ward explanation
     */
    addWardExplanation(ward, explanation) {
        const explanationHTML = `
            <div class="ward-explanation">
                <h5>📍 ${ward}</h5>
                <p>${explanation}</p>
            </div>
        `;
        this.messageHandler.addAssistantMessage(explanationHTML);
    }

    /**
     * Handle error actions
     */
    handleErrorAction(action) {
        if (action.response && typeof action.response === 'string') {
            const lowerResponse = action.response.toLowerCase();
            if (lowerResponse.includes('no csv data loaded') || 
                lowerResponse.includes('not properly loaded') || 
                lowerResponse.includes('data handler not initialized')) {
                
                const recoveryMessage = `<div class="alert alert-warning">
                    <p><strong>Data loading issue detected.</strong></p>
                    <p>It appears your data files were uploaded but not properly loaded for analysis.</p>
                    <p>
                        <button class="btn btn-primary btn-sm recovery-action" data-action="reload-data">
                            Attempt Recovery
                        </button>
                    </p>
                </div>`;
                
                this.messageHandler.addSystemMessage(recoveryMessage);
            }
        }
    }

    /**
     * CRITICAL FIX: Setup resize handler for scroll container
     */
    setupResizeHandler() {
        let resizeTimeout;
        
        window.addEventListener('resize', () => {
            clearTimeout(resizeTimeout);
            resizeTimeout = setTimeout(() => {
                this.recalculateScrollContainer();
            }, 250);
        });
    }
    
    /**
     * CRITICAL FIX: Recalculate scroll container height
     */
    recalculateScrollContainer() {
        const messagesContainer = document.getElementById('chat-messages');
        if (!messagesContainer) return;
        
        const header = document.querySelector('.chat-header');
        const inputContainer = document.querySelector('.chat-input-container');
        
        if (header && inputContainer) {
            const headerHeight = header.offsetHeight;
            const inputHeight = inputContainer.offsetHeight;
            const windowHeight = window.innerHeight;
            
            const availableHeight = windowHeight - headerHeight - inputHeight - 20; // 20px padding
            messagesContainer.style.maxHeight = `${availableHeight}px`;
            
            console.log(`📊 Recalculated scroll container height: ${availableHeight}px`);
        }
    }
    
    /**
     * Generate unique session ID
     */
    generateSessionId() {
        return 'session_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
    }

    /**
     * Public API for running analysis (for backward compatibility)
     */
    async runAnalysis(variables = null) {
        const message = variables ? 
            `Run analysis using variables: ${variables.join(', ')}` : 
            'Run analysis';
        
        return await this.messageHandler.sendMessage(message);
    }

    /**
     * Public API for clearing chat
     */
    clearChat() {
        this.messageHandler.clearChat();
        // Also clear variable cache when chat is cleared
        variableDisplayManager.clearCache();
    }

    // === PUBLIC API METHODS FOR BACKWARD COMPATIBILITY ===

    /**
     * Public API for adding system messages
     */
    addSystemMessage(message) {
        return this.messageHandler.addSystemMessage(message);
    }

    /**
     * Public API for adding user messages
     */
    addUserMessage(message) {
        return this.messageHandler.addUserMessage(message);
    }

    /**
     * Public API for adding assistant messages
     */
    addAssistantMessage(message) {
        return this.messageHandler.addAssistantMessage(message);
    }

    /**
     * Public API for sending messages
     */
    async sendMessage(message = null) {
        return await this.messageHandler.sendMessage(message);
    }

    /**
     * Get chat history for saving
     */
    getChatHistory() {
        // Extract messages from chat container
        const messages = [];
        if (this.messageHandler.chatContainer) {
            const messageElements = this.messageHandler.chatContainer.querySelectorAll('.message');
            messageElements.forEach(el => {
                const role = el.classList.contains('user-message') ? 'user' : 
                           el.classList.contains('assistant-message') ? 'assistant' : 'system';
                const content = el.querySelector('.message-content')?.innerHTML || '';
                messages.push({ role, content });
            });
        }
        return messages;
    }

    /**
     * Load welcome messages
     */
    loadWelcomeMessages() {
        this.loadWelcomeMessage();
    }
}

// Create and export singleton instance
const chatManager = new ChatManager();

// Make available globally for button handlers (backward compatibility)
window.chatManager = chatManager;

export default chatManager; 