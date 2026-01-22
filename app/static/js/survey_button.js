/**
 * Survey Button Integration for ChatMRPT
 * Adds a survey button to the existing React interface
 */

(function() {
    'use strict';

    class SurveyButton {
        constructor() {
            console.log('🔵 SurveyButton: Constructor called');
            this.pendingSurveys = 0;
            this.chatmrptSessionId = this.getSessionId();
            this.checkInterval = null;

            this.init();
        }

        init() {
            console.log('🔵 SurveyButton: Init called, readyState:', document.readyState);
            // Wait for page to fully load
            if (document.readyState === 'loading') {
                console.log('🔵 SurveyButton: Waiting for DOMContentLoaded');
                document.addEventListener('DOMContentLoaded', () => this.setup());
            } else {
                console.log('🔵 SurveyButton: Page already loaded, calling setup');
                this.setup();
            }
        }

        setup() {
            console.log('🔵 SurveyButton: Setup called');
            // Create and inject survey button
            this.createSurveyButton();

            // Start checking for pending surveys
            this.startPolling();

            // Listen for ChatMRPT events
            this.setupEventListeners();
        }

        getSessionId() {
            // Try to get session ID from various sources
            return sessionStorage.getItem('chatmrpt_session_id') ||
                   localStorage.getItem('chatmrpt_session_id') ||
                   this.generateSessionId();
        }

        generateSessionId() {
            const id = 'session_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
            sessionStorage.setItem('chatmrpt_session_id', id);
            return id;
        }

        createSurveyButton() {
            console.log('🔵 SurveyButton: CreateSurveyButton called');
            // Wait a bit for React to render, then try multiple times to find the nav bar
            let attempts = 0;
            const maxAttempts = 10;

            const tryCreateButton = () => {
                attempts++;
                console.log(`🔵 SurveyButton: Attempt ${attempts}/${maxAttempts} to find nav bar`);

                // Try to find the ChatMRPT navigation bar - look for the area with Clear and Export buttons
                let navBar = document.querySelector('header') ||
                            document.querySelector('[class*="navbar"]') ||
                            document.querySelector('[class*="nav-bar"]') ||
                            document.querySelector('[class*="header"]') ||
                            document.querySelector('nav');

                // Find toolbar buttons to locate the navbar correctly
                const newChatButton = Array.from(document.querySelectorAll('button')).find(btn =>
                    btn.textContent.includes('New Chat') || btn.textContent.includes('Clear')
                );
                const downloadButton = Array.from(document.querySelectorAll('button')).find(btn =>
                    btn.textContent.includes('Download') || btn.textContent.includes('Export')
                );

                console.log('🔵 SurveyButton: Found navBar?', !!navBar, 'Found NewChat?', !!newChatButton, 'Found Download?', !!downloadButton);

                // Use the common parent of toolbar buttons as the navbar
                if (newChatButton && downloadButton) {
                    // Find common parent that contains both buttons
                    let commonParent = downloadButton.parentElement;
                    while (commonParent && !commonParent.contains(newChatButton)) {
                        commonParent = commonParent.parentElement;
                    }
                    if (commonParent) {
                        navBar = commonParent;
                    }
                } else if (downloadButton) {
                    // Fallback to Download button's parent
                    navBar = downloadButton.parentElement;
                } else if (newChatButton) {
                    // Fallback to New Chat button's parent
                    navBar = newChatButton.parentElement;
                }

                const actionGroup = document.getElementById('toolbar-action-group');
                if (actionGroup) {
                    navBar = actionGroup;
                }

                // If still no nav bar found and we've tried enough times, create a basic container as fallback
                if (!navBar && attempts >= maxAttempts) {
                    navBar = document.createElement('div');
                    navBar.style.cssText = `
                        position: fixed;
                        top: 0;
                        left: 0;
                        right: 0;
                        height: 60px;
                        background: #ffffff;
                        border-bottom: 1px solid #e5e7eb;
                        display: flex;
                        align-items: center;
                        justify-content: flex-end;
                        padding: 0 20px;
                        gap: 12px;
                        z-index: 9998;
                        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
                    `;
                    navBar.id = 'toolbar-action-group';
                    document.body.appendChild(navBar);
                    document.body.style.paddingTop = '60px';
                } else if (!navBar) {
                    setTimeout(tryCreateButton, 500);
                    return;
                }

                // Check if button already exists to avoid duplicates
                if (document.getElementById('survey-button')) {
                    return;
                }

                this.insertSurveyButton(navBar);
            }; // Arrow functions inherit this context

            // Start trying to create the button
            tryCreateButton();
        }

        insertSurveyButton(navBar) {
            // Ensure the nav bar behaves as a flex row
            if (navBar && navBar.classList) {
                navBar.classList.add('flex');
                navBar.classList.add('items-center');
                navBar.classList.add('gap-3');
            }

            const button = document.createElement('button');
            button.id = 'survey-button';
            button.type = 'button';
            button.className = 'toolbar-button toolbar-button--survey';

            button.innerHTML = `
                <span class="toolbar-button-icon">📋</span>
                <span class="toolbar-button-label">Survey</span>
                <span id="survey-badge" class="toolbar-badge">0</span>
            `;

            button.onclick = () => this.openSurvey();
            navBar.appendChild(button);

            // Add pulsing animation when surveys are pending
            const style = document.createElement('style');
            style.textContent = `
                @keyframes pulse-survey {
                    0% { box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); }
                    50% { box-shadow: 0 4px 20px rgba(37, 99, 235, 0.4); }
                    100% { box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); }
                }
                .survey-btn.has-pending {
                    animation: pulse-survey 2s infinite;
                }
            `;
            document.head.appendChild(style);
        }

        openSurvey() {
            // Gather current context
            const context = this.gatherContext();

            // Build survey URL
            const params = new URLSearchParams({
                session_id: this.chatmrptSessionId,
                context: JSON.stringify(context),
                trigger: context.trigger || 'manual'
            });

            const surveyUrl = `/survey?${params.toString()}`;

            // Open in new tab
            window.open(surveyUrl, '_blank');

            // Reset badge
            this.updateBadge(0);
        }

        gatherContext() {
            const context = {
                timestamp: new Date().toISOString(),
                page_url: window.location.href,
                session_id: this.chatmrptSessionId
            };

            // Check if in arena mode
            const arenaSection = document.querySelector('[class*="arena"]');
            if (arenaSection) {
                context.mode = 'arena';
                // Try to extract model names from the UI
                const modelElements = document.querySelectorAll('[class*="model-name"]');
                if (modelElements.length > 0) {
                    context.models = Array.from(modelElements).map(el => el.textContent);
                }
            }

            // Check for recent actions
            const lastAction = sessionStorage.getItem('last_chatmrpt_action');
            if (lastAction) {
                try {
                    context.last_action = JSON.parse(lastAction);
                } catch (e) {
                    context.last_action = lastAction;
                }
            }

            return context;
        }

        startPolling() {
            // Check for pending surveys every 30 seconds
            this.checkPendingSurveys();
            this.checkInterval = setInterval(() => {
                this.checkPendingSurveys();
            }, 30000);
        }

        async checkPendingSurveys() {
            try {
                const response = await fetch(`/survey/api/status/${this.chatmrptSessionId}`);
                const data = await response.json();

                if (data.success) {
                    this.updateBadge(data.pending_count);
                }
            } catch (error) {
                console.error('Failed to check survey status:', error);
            }
        }

        updateBadge(count) {
            const badge = document.getElementById('survey-badge');
            const button = document.getElementById('survey-button');

            if (!badge || !button) {
                return;
            }

            if (count > 0) {
                badge.textContent = count;
                badge.classList.add('toolbar-badge--visible');
                badge.classList.remove('hidden');
                button.classList.add('toolbar-button--accent-active');
            } else {
                badge.classList.remove('toolbar-badge--visible');
                badge.classList.add('hidden');
                button.classList.remove('toolbar-button--accent-active');
            }

            this.pendingSurveys = count;
        }

        setupEventListeners() {
            // Listen for specific ChatMRPT events that should trigger surveys

            // Monitor for arena comparisons
            this.monitorArenaComparisons();

            // Monitor for analysis completions
            this.monitorAnalysisCompletions();

            // Monitor for ITN distributions
            this.monitorITNDistributions();
        }

        monitorArenaComparisons() {
            // Override fetch to detect arena API calls
            const originalFetch = window.fetch;
            window.fetch = async (...args) => {
                const response = await originalFetch(...args);

                // Check if this is an arena comparison
                if (args[0] && args[0].includes('/api/arena/compare')) {
                    // Wait a bit for the UI to update
                    setTimeout(() => {
                        this.createSurveyTrigger('arena_comparison', {
                            models: this.extractArenaModels()
                        });
                    }, 2000);
                }

                return response;
            };
        }

        monitorAnalysisCompletions() {
            // Monitor for analysis API calls
            const observer = new MutationObserver((mutations) => {
                mutations.forEach((mutation) => {
                    // Check for analysis results being added to DOM
                    if (mutation.target.id && mutation.target.id.includes('analysis-results')) {
                        this.createSurveyTrigger('risk_analysis_complete', {
                            analysis_type: this.detectAnalysisType()
                        });
                    }
                });
            });

            // Start observing
            observer.observe(document.body, {
                childList: true,
                subtree: true,
                attributes: true,
                attributeFilter: ['class', 'id']
            });
        }

        monitorITNDistributions() {
            // Similar monitoring for ITN distribution maps
            const observer = new MutationObserver((mutations) => {
                mutations.forEach((mutation) => {
                    // Check for ITN map being added
                    if (mutation.target.id && mutation.target.id.includes('itn-map')) {
                        this.createSurveyTrigger('itn_distribution_generated', {
                            distribution_params: this.extractITNParams()
                        });
                    }
                });
            });

            observer.observe(document.body, {
                childList: true,
                subtree: true
            });
        }

        async createSurveyTrigger(triggerType, context) {
            try {
                const response = await fetch('/survey/api/trigger', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        chatmrpt_session_id: this.chatmrptSessionId,
                        trigger_type: triggerType,
                        context: context
                    })
                });

                if (response.ok) {
                    // Update badge to show new survey
                    this.checkPendingSurveys();

                    // Show notification
                    this.showNotification(`New survey available: ${this.formatTriggerType(triggerType)}`);
                }
            } catch (error) {
                console.error('Failed to create survey trigger:', error);
            }
        }

        formatTriggerType(trigger) {
            const triggerNames = {
                'arena_comparison': 'Arena Comparison',
                'risk_analysis_complete': 'Risk Analysis',
                'itn_distribution_generated': 'ITN Distribution'
            };
            return triggerNames[trigger] || trigger;
        }

        showNotification(message) {
            // Create notification
            const notification = document.createElement('div');
            notification.style.cssText = `
                position: fixed;
                top: 20px;
                right: 20px;
                background: #2563eb;
                color: white;
                padding: 12px 20px;
                border-radius: 8px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                z-index: 10000;
                animation: slideIn 0.3s ease;
            `;
            notification.textContent = message;

            document.body.appendChild(notification);

            // Auto-remove after 5 seconds
            setTimeout(() => {
                notification.style.animation = 'slideOut 0.3s ease';
                setTimeout(() => notification.remove(), 300);
            }, 5000);
        }

        extractArenaModels() {
            // Try to extract model names from the arena UI
            const models = [];
            document.querySelectorAll('[class*="model-"]').forEach(el => {
                const text = el.textContent;
                if (text && text.includes('Model')) {
                    models.push(text);
                }
            });
            return models;
        }

        detectAnalysisType() {
            // Try to detect which type of analysis was performed
            const content = document.body.textContent;
            if (content.includes('Composite Score')) return 'composite';
            if (content.includes('PCA')) return 'pca';
            return 'unknown';
        }

        extractITNParams() {
            // Try to extract ITN distribution parameters
            const params = {};
            // This would need to be adapted based on actual UI structure
            return params;
        }
    }

    // Initialize survey button
    console.log('🔵 SurveyButton: Script loaded, creating instance');
    window.surveyButton = new SurveyButton();
    console.log('🔵 SurveyButton: Instance created:', !!window.surveyButton);
})();
