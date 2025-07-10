/**
 * Message Handler Module
 * Handles core message sending, receiving, and display
 */

import { SessionDataManager } from '../../utils/storage.js';
import apiClient from '../../utils/api-client.js';
import DOMHelpers from '../../utils/dom-helpers.js';

export class MessageHandler {
    constructor(chatContainer, messageInput, sendButton) {
        this.chatContainer = chatContainer;
        this.messageInput = messageInput;
        this.sendButton = sendButton;
        this.isWaitingForResponse = false;
        
        this.bindEvents();
    }

    bindEvents() {
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

            this.messageInput.addEventListener('input', () => {
                this.autoResizeInput();
            });
        }
    }

    async sendMessage(customMessage = null) {
        if (this.isWaitingForResponse) return;

        const message = customMessage || this.messageInput?.value?.trim();
        if (!message) return;

        // Clear input if using default input
        if (!customMessage && this.messageInput) {
            this.messageInput.value = '';
            this.autoResizeInput();
        }

        this.addUserMessage(message);
        this.isWaitingForResponse = true;
        this.showTypingIndicator();
        
        // FIXED: Add loading state to send button
        if (this.sendButton) {
            this.sendButton.classList.add('loading');
            this.sendButton.disabled = true;
        }

        try {
            const sessionData = SessionDataManager.getSessionData();
            
            // Check if streaming is enabled (can be toggled via settings)
            const useStreaming = true; // Always use streaming for better UX
            console.log('🔥 STREAMING DEBUG: useStreaming =', useStreaming);
            
            if (useStreaming) {
                console.log('🔥 STREAMING DEBUG: Using streaming endpoint!');
                // Use streaming for better UX
                let streamingMessageElement = null;
                let streamingContent = '';
                let fullResponse = {};
                
                apiClient.sendMessageStreaming(
                    message, 
                    sessionData.currentLanguage,
                    // onChunk callback
                    (chunk) => {
                        if (!streamingMessageElement) {
                            this.hideTypingIndicator();
                            streamingMessageElement = this.createMessageElement('assistant', '', 'assistant-message streaming');
                            this.appendMessage(streamingMessageElement);
                        }
                        
                        if (chunk.content) {
                            streamingContent += chunk.content;
                            const contentDiv = streamingMessageElement.querySelector('.message-content');
                            if (contentDiv) {
                                // FIXED: Only show raw content during streaming, parse markdown at end
                                // This prevents performance issues from re-parsing on every chunk
                                contentDiv.textContent = streamingContent;
                                // FIXED: Remove redundant scroll calls during streaming
                                // Scroll is handled by appendMessage when element is created
                            }
                        }
                        
                        // Update fullResponse with latest data
                        if (chunk.status) fullResponse.status = chunk.status;
                        if (chunk.visualizations) fullResponse.visualizations = chunk.visualizations;
                        if (chunk.tools_used) fullResponse.tools_used = chunk.tools_used;
                    },
                    // onComplete callback
                    (finalData) => {
                        if (streamingMessageElement) {
                            streamingMessageElement.classList.remove('streaming');
                            
                            // FIXED: Now parse markdown only once at completion
                            const contentDiv = streamingMessageElement.querySelector('.message-content');
                            if (contentDiv && streamingContent) {
                                const parsedContent = this.parseMarkdownContent(streamingContent);
                                contentDiv.innerHTML = parsedContent;
                            }
                        }
                        
                        // Prepare final response object
                        fullResponse.response = streamingContent;
                        fullResponse.message = streamingContent;
                        fullResponse.streaming_handled = true;  // ✅ Mark as handled via streaming
                        
                        // Emit event for other modules to handle
                        document.dispatchEvent(new CustomEvent('messageResponse', { 
                            detail: { response: fullResponse, originalMessage: message }
                        }));
                        
                        this.isWaitingForResponse = false;
                        
                        // FIXED: Remove loading state from send button
                        if (this.sendButton) {
                            this.sendButton.classList.remove('loading');
                            this.sendButton.disabled = false;
                        }
                    }
                );
                
                return; // Exit early for streaming
            } else {
                // Fallback to regular non-streaming
                const response = await apiClient.sendMessage(message, sessionData.currentLanguage);

                this.hideTypingIndicator();
                
                // Debug logging
                console.log('🔍 DEBUG: Raw response from backend:', response);
                console.log('🔍 DEBUG: Response has .response field:', !!response.response);
                console.log('🔍 DEBUG: Response has .message field:', !!response.message);
                
                // Emit event for other modules to handle
                document.dispatchEvent(new CustomEvent('messageResponse', { 
                    detail: { response, originalMessage: message }
                }));

                return response;
            }
        } catch (error) {
            this.hideTypingIndicator();
            this.isWaitingForResponse = false;
            
            // FIXED: Remove loading state on error
            if (this.sendButton) {
                this.sendButton.classList.remove('loading');
                this.sendButton.disabled = false;
            }
            
            // FIXED: Better error messages based on error type
            let errorMessage = 'Sorry, there was an error processing your request.';
            
            if (error.name === 'TypeError' && error.message.includes('fetch')) {
                errorMessage = 'Connection failed. Please check your internet connection and try again.';
            } else if (error.message.includes('timeout')) {
                errorMessage = 'Request timed out. The server might be busy. Please try again.';
            } else if (error.message.includes('429')) {
                errorMessage = 'Too many requests. Please wait a moment and try again.';
            } else if (error.message.includes('5')) {
                errorMessage = 'Server error. Our team has been notified. Please try again in a few minutes.';
            }
            
            this.addSystemMessage(`
                <div class="error-message">
                    <i class="fas fa-exclamation-triangle"></i>
                    <span>${errorMessage}</span>
                    <button class="btn btn-sm btn-outline-primary retry-btn" onclick="document.getElementById('message-input').focus()">
                        Try Again
                    </button>
                </div>
            `);
            
            console.error('Error sending message:', error);
            // Don't re-throw error to prevent global error handler spam
            return { status: 'error', message: error.message };
        }
    }

    addUserMessage(message) {
        const messageElement = this.createMessageElement('user', message, 'user-message');
        this.appendMessage(messageElement);
        // FIXED: Remove redundant scroll call - appendMessage handles it
    }

    addAssistantMessage(message) {
        const messageElement = this.createMessageElement('assistant', message, 'assistant-message');
        this.appendMessage(messageElement);
        // FIXED: Remove redundant scroll call - appendMessage handles it
    }

    addSystemMessage(message) {
        const messageElement = this.createMessageElement('system', message, 'system-message');
        this.appendMessage(messageElement);
        // FIXED: Remove redundant scroll call - appendMessage handles it
    }

    createMessageElement(sender, content, className) {
        const messageDiv = DOMHelpers.createElement('div', {
            className: `message ${className} new-message`
        });

        const contentDiv = document.createElement('div');
        contentDiv.className = 'message-content';
        
        // Enhanced markdown parsing for all content
        const parsedContent = this.parseMarkdownContent(content);
        console.log('🔍 DEBUG: Setting innerHTML to:', parsedContent.substring(0, 100));
        contentDiv.innerHTML = parsedContent;

        messageDiv.appendChild(contentDiv);
        return messageDiv;
    }
    
    /**
     * Parse markdown-like content and convert to HTML
     * Handles the specific formatting from our analysis tools
     */
    parseMarkdownContent(content) {
        if (!content) return '';
        
        let text = typeof content === 'string' ? content : String(content);
        
        console.log('🔍 MARKDOWN DEBUG: Original length:', text.length);
        console.log('🔍 MARKDOWN DEBUG: Contains headers:', /^##/gm.test(text));
        console.log('🔍 MARKDOWN DEBUG: Contains bold:', /\*\*/.test(text));
        console.log('🔍 MARKDOWN DEBUG: Contains bullets:', /^[•-]/gm.test(text));
        
        // Step 1: Convert headers (more specific matching)
        text = text.replace(/^### (.*?)$/gm, '<h3>$1</h3>');
        text = text.replace(/^## (.*?)$/gm, '<h2>$1</h2>');
        text = text.replace(/^# (.*?)$/gm, '<h1>$1</h1>');
        
        // Step 2: Convert bold text (non-greedy)
        text = text.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');
        
        // Step 3: Convert links
        text = text.replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank" rel="noopener noreferrer">$1</a>');
        
        // Step 4: Handle bullet points more carefully
        text = text.replace(/^[•\-] (.+)$/gm, '<li>$1</li>');
        
        // Step 5: Wrap consecutive list items in ul tags
        text = text.replace(/(<li>.*?<\/li>(?:\s*<li>.*?<\/li>)*)/gs, '<ul>$1</ul>');
        
        // Step 6: Handle line breaks and paragraphs
        // Split by double newlines for paragraphs
        const paragraphs = text.split(/\n\s*\n/);
        text = paragraphs.map(paragraph => {
            // Don't wrap headers, lists, or already wrapped content
            if (paragraph.includes('<h') || paragraph.includes('<ul') || paragraph.startsWith('<')) {
                return paragraph.replace(/\n/g, ' ');
            } else {
                return '<p>' + paragraph.replace(/\n/g, '<br>') + '</p>';
            }
        }).join('\n\n');
        
        console.log('🔍 MARKDOWN DEBUG: Final length:', text.length);
        console.log('🔍 MARKDOWN DEBUG: Final preview:', text.substring(0, 300));
        
        return text;
    }

    appendMessage(messageElement) {
        if (this.chatContainer) {
            // FIXED: Simplified scroll logic - single source of truth
            const wasScrolledToBottom = this.isScrolledToBottom();
            
            this.chatContainer.appendChild(messageElement);
            
            // FIXED: Direct scroll control without redundant checks
            if (wasScrolledToBottom) {
                // User was at bottom, maintain that position
                DOMHelpers.scrollToBottom(this.chatContainer);
            }
            // If user was not at bottom, let them stay where they were (don't interfere)
            
            setTimeout(() => {
                DOMHelpers.removeClass(messageElement, 'new-message');
            }, 500);
        }
    }

    showTypingIndicator() {
        this.hideTypingIndicator();
        
        // FIXED: Enhanced loading indicator with better visual feedback
        const typingDiv = DOMHelpers.createElement('div', {
            id: 'typing-indicator',
            className: 'typing-indicator enhanced'
        }, `
            <div class="typing-avatar">
                <i class="fas fa-robot"></i>
            </div>
            <div class="typing-content">
                <div class="typing-dots">
                    <span></span><span></span><span></span>
                </div>
                <div class="typing-text">ChatMRPT is thinking...</div>
            </div>
        `);

        if (this.chatContainer) {
            this.chatContainer.appendChild(typingDiv);
            // FIXED: Don't force scroll when showing typing indicator
            // Let appendMessage handle scroll logic consistently
        }
    }

    hideTypingIndicator() {
        const typingIndicator = DOMHelpers.getElementById('typing-indicator');
        if (typingIndicator) {
            typingIndicator.remove();
        }
    }

    autoResizeInput() {
        if (this.messageInput) {
            this.messageInput.style.height = 'auto';
            this.messageInput.style.height = Math.min(this.messageInput.scrollHeight, 200) + 'px';
        }
    }

    scrollToBottom() {
        if (this.chatContainer) {
            // FIXED: Simplified - just scroll to bottom when called
            // All logic moved to appendMessage for centralized control
            DOMHelpers.scrollToBottom(this.chatContainer);
        }
    }
    
    /**
     * CRITICAL FIX: Check if user is scrolled to bottom
     * @returns {boolean} True if scrolled to bottom
     */
    isScrolledToBottom() {
        if (!this.chatContainer) return true;
        
        const threshold = 50; // FIXED: Reduced from 100px - less aggressive auto-scroll
        const scrollTop = this.chatContainer.scrollTop;
        const scrollHeight = this.chatContainer.scrollHeight;
        const clientHeight = this.chatContainer.clientHeight;
        
        return scrollHeight - scrollTop - clientHeight < threshold;
    }

    clearChat() {
        if (this.chatContainer) {
            // CRITICAL FIX: Store reference to prevent accidental clearing
            const messages = this.chatContainer.querySelectorAll('.message');
            console.log(`🧹 Clearing ${messages.length} messages from chat`);
            DOMHelpers.clearChildren(this.chatContainer);
        }
    }

    scrollToTop() {
        if (this.chatContainer) {
            this.chatContainer.scrollTop = 0;
            console.log('🔝 Scrolled to top of chat');
        }
    }
    
    /**
     * CRITICAL FIX: Get all messages count for debugging
     * @returns {number} Number of messages in chat
     */
    getMessageCount() {
        if (!this.chatContainer) return 0;
        return this.chatContainer.querySelectorAll('.message').length;
    }
    
    /**
     * CRITICAL FIX: Debug scroll information
     */
    debugScrollInfo() {
        if (!this.chatContainer) return;
        
        console.log('📊 SCROLL DEBUG INFO:');
        console.log('- Container height:', this.chatContainer.clientHeight);
        console.log('- Scroll height:', this.chatContainer.scrollHeight);
        console.log('- Scroll top:', this.chatContainer.scrollTop);
        console.log('- Is scrolled to bottom:', this.isScrolledToBottom());
        console.log('- Messages count:', this.getMessageCount());
        console.log('- Container overflow-y:', getComputedStyle(this.chatContainer).overflowY);
    }
} 