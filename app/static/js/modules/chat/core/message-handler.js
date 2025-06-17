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

        try {
            const sessionData = SessionDataManager.getSessionData();
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
        } catch (error) {
            this.hideTypingIndicator();
            this.addSystemMessage('Sorry, there was an error processing your request. Please try again.');
            console.error('Error sending message:', error);
            throw error;
        } finally {
            this.isWaitingForResponse = false;
        }
    }

    addUserMessage(message) {
        const messageElement = this.createMessageElement('user', message, 'user-message');
        this.appendMessage(messageElement);
        this.scrollToBottom();
    }

    addAssistantMessage(message) {
        const messageElement = this.createMessageElement('assistant', message, 'assistant-message');
        this.appendMessage(messageElement);
        this.scrollToBottom();
    }

    addSystemMessage(message) {
        const messageElement = this.createMessageElement('system', message, 'system-message');
        this.appendMessage(messageElement);
        this.scrollToBottom();
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
            this.chatContainer.appendChild(messageElement);
            
            setTimeout(() => {
                DOMHelpers.removeClass(messageElement, 'new-message');
            }, 500);
        }
    }

    showTypingIndicator() {
        this.hideTypingIndicator();
        
        const typingDiv = DOMHelpers.createElement('div', {
            id: 'typing-indicator',
            className: 'typing-indicator'
        }, `<span></span><span></span><span></span>`);

        if (this.chatContainer) {
            this.chatContainer.appendChild(typingDiv);
            this.scrollToBottom();
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
            DOMHelpers.scrollToBottom(this.chatContainer);
        }
    }

    clearChat() {
        if (this.chatContainer) {
            DOMHelpers.clearChildren(this.chatContainer);
        }
    }
} 