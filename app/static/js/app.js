/**
 * ChatMRPT Main Application Coordinator
 * Initializes and coordinates all modules for the modular JavaScript architecture
 */

// Import all modules - UPDATED TO USE REFACTORED CHAT MANAGER
import chatManager from './modules/chat/chat-manager-refactored.js';
import sidebarManager from './modules/ui/sidebar.js';
import fileUploader from './modules/upload/file-uploader.js';
import apiClient from './modules/utils/api-client.js';
import DOMHelpers from './modules/utils/dom-helpers.js';
import { SessionDataManager } from './modules/utils/storage.js';
import { DataUploadManager } from './modules/data/data-upload-manager.js';
import variableDisplayManager from './modules/utils/variable-display.js';

/**
 * Theme Manager - Light/Dark Mode Toggle
 */
class ThemeManager {
    constructor() {
        this.currentTheme = this.getStoredTheme() || 'light';
        this.themeToggleInput = document.getElementById('theme-toggle');
        this.init();
    }

    init() {
        // Apply stored theme on load
        this.applyTheme(this.currentTheme);
        
        // Add change listener to theme toggle checkbox
        if (this.themeToggleInput) {
            this.themeToggleInput.checked = this.currentTheme === 'dark';
            this.themeToggleInput.addEventListener('change', () => this.toggleTheme());
        }
    }

    getStoredTheme() {
        return localStorage.getItem('chatmrpt-theme');
    }

    setStoredTheme(theme) {
        localStorage.setItem('chatmrpt-theme', theme);
    }

    applyTheme(theme) {
        document.documentElement.setAttribute('data-theme', theme);
        this.updateToggleState(theme);
        this.setStoredTheme(theme);
    }

    updateToggleState(theme) {
        if (this.themeToggleInput) {
            this.themeToggleInput.checked = theme === 'dark';
        }
    }

    toggleTheme() {
        this.currentTheme = this.currentTheme === 'light' ? 'dark' : 'light';
        this.applyTheme(this.currentTheme);
    }
}

/**
 * Sidebar Manager - Handle hamburger menu sidebar
 */
class SidebarController {
    constructor() {
        this.sidebar = document.getElementById('app-sidebar');
        this.sidebarToggle = document.getElementById('sidebar-toggle');
        this.closeSidebar = document.getElementById('close-sidebar');
        this.init();
    }

    init() {
        if (this.sidebarToggle) {
            this.sidebarToggle.addEventListener('click', () => this.toggleSidebar());
        }
        
        if (this.closeSidebar) {
            this.closeSidebar.addEventListener('click', () => this.hideSidebar());
        }

        // Close sidebar when clicking outside
        document.addEventListener('click', (e) => {
            if (this.sidebar && this.sidebar.classList.contains('show')) {
                if (!this.sidebar.contains(e.target) && !this.sidebarToggle.contains(e.target)) {
                    this.hideSidebar();
                }
            }
        });
    }

    toggleSidebar() {
        if (this.sidebar) {
            this.sidebar.classList.toggle('show');
        }
    }

    showSidebar() {
        if (this.sidebar) {
            this.sidebar.classList.add('show');
        }
    }

    hideSidebar() {
        if (this.sidebar) {
            this.sidebar.classList.remove('show');
        }
    }
}

class ChatMRPTApp {
    constructor() {
        this.initialized = false;
        this.modules = {};
        
        // Module references - FIXED
        this.chatManager = chatManager;
        this.sidebarManager = sidebarManager;
        this.fileUploader = fileUploader;
        this.apiClient = apiClient;
        
        // App state
        this.isReady = false;
        this.version = '2.0';
        
        this.themeManager = null;
        this.sidebarController = null;
        
        this.dataUploadManager = null;
        
        this.init();
    }

    /**
     * Initialize the application
     */
    async init() {
        if (this.initialized) return;

        console.log('🚀 Initializing ChatMRPT v' + this.version);
        
        // Wait for DOM to be ready
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', async () => {
                await this.initializeModules();
            });
        } else {
            await this.initializeModules();
        }
    }

    /**
     * Initialize all modules and setup coordination
     */
    async initializeModules() {
        try {
            console.log('📦 Initializing modules...');

            // Initialize Theme Manager first
            this.themeManager = new ThemeManager();

            // Initialize Sidebar Controller
            this.sidebarController = new SidebarController();

            // Initialize Data Upload Manager
            this.dataUploadManager = new DataUploadManager();

            // Use existing singleton modules (they auto-initialize)
            this.modules = {
                chat: this.chatManager,
                sidebar: this.sidebarManager,
                uploader: this.fileUploader,
                api: this.apiClient
            };

            // Setup inter-module communication
            this.setupModuleCoordination();

            // Setup global event handlers
            this.setupGlobalEventHandlers();

            // Initialize status indicator
            this.initializeStatusIndicator();

            // Initialize report functionality
            this.initializeReportFunctionality();

            // Initialize language functionality  
            this.initializeLanguageFunctionality();

            // Initialize visualization functionality
            this.initializeVisualizationHandlers();

            // Load initial state
            this.loadInitialState();

            // Make chat manager globally available for method switching
            window.chatManager = this.chatManager;

            this.initialized = true;
            this.isReady = true;

            console.log('✅ ChatMRPT initialized successfully');
            
            // Dispatch ready event
            document.dispatchEvent(new CustomEvent('chatMRPTReady', {
                detail: { app: this, version: this.version }
            }));

        } catch (error) {
            console.error('❌ Failed to initialize ChatMRPT:', error);
            this.handleInitializationError(error);
        }
    }

    /**
     * Setup coordination between modules
     */
    setupModuleCoordination() {
        // Listen for theme changes
        document.addEventListener('themeChanged', (e) => {
            console.log('🎨 Theme changed to:', e.detail.theme);
        });

        // Listen for view mode changes
        document.addEventListener('viewModeChanged', (e) => {
            console.log('👁️ View mode changed:', e.detail);
        });

        // Listen for cookies acceptance
        document.addEventListener('cookiesAccepted', (e) => {
            console.log('🍪 Cookies accepted at:', new Date(e.detail.timestamp));
        });

        // Coordinate file upload with chat
        document.addEventListener('filesUploaded', (e) => {
            this.chatManager.addSystemMessage('Files uploaded successfully!');
        });
    }

    /**
     * Setup global event handlers
     */
    setupGlobalEventHandlers() {
        // Global error handler
        window.addEventListener('error', (e) => {
            console.error('Global error:', e.error);
            this.handleGlobalError(e.error);
        });

        // Handle unhandled promise rejections
        window.addEventListener('unhandledrejection', (e) => {
            console.error('Unhandled promise rejection:', e.reason);
            this.handleGlobalError(e.reason);
        });

        // Handle page visibility changes
        document.addEventListener('visibilitychange', () => {
            if (document.hidden) {
                this.onPageHidden();
            } else {
                this.onPageVisible();
            }
        });

        // Handle beforeunload to save state
        window.addEventListener('beforeunload', () => {
            this.saveApplicationState();
        });

        // Setup data load event handling to preload variable metadata
        this.setupDataLoadHandling();
    }

    /**
     * Setup data load event handling to preload variable metadata
     */
    setupDataLoadHandling() {
        // Listen for data upload success
        document.addEventListener('dataUploadSuccess', async (event) => {
            console.log('📊 Data upload successful, preloading variable metadata...');
            
            try {
                // Get available variables from the uploaded data
                const response = await fetch('/api/variables');
                if (response.ok) {
                    const data = await response.json();
                    if (data.status === 'success' && data.variables) {
                        // Preload variable metadata for efficient display
                        await variableDisplayManager.preloadVariables(data.variables);
                        console.log('✅ Variable metadata preloaded for efficient display');
                    }
                }
            } catch (error) {
                console.warn('⚠️ Could not preload variable metadata:', error);
                // Non-critical error, continue without preloading
            }
        });

        // Listen for new session events to clear cache
        document.addEventListener('newSession', () => {
            variableDisplayManager.clearCache();
            console.log('🧹 Variable cache cleared for new session');
        });
    }

    /**
     * Initialize status indicator functionality
     */
    initializeStatusIndicator() {
        const statusIndicator = {
            statusDot: DOMHelpers.getElementById('status-dot'),
            statusText: DOMHelpers.getElementById('status-text'),
            
            updateStatus() {
                const sessionData = SessionDataManager.getSessionData();
                let status = 'ready';
                let text = 'Ready';

                if (sessionData.csvLoaded || sessionData.shapefileLoaded) {
                    status = 'data-loaded';
                    text = 'Data Loaded';
                }

                if (sessionData.analysisComplete) {
                    status = 'analysis-complete';
                    text = 'Analysis Complete';
                }

                if (this.statusDot) {
                    this.statusDot.className = `status-dot ${status}`;
                }
                if (this.statusText) {
                    this.statusText.textContent = text;
                }
            }
        };

        // Make available globally
        window.statusIndicator = statusIndicator;
        
        // Update initial status
        statusIndicator.updateStatus();
    }

    /**
     * Initialize report functionality
     */
    initializeReportFunctionality() {
        const downloadReportBtn = DOMHelpers.getElementById('download-report-btn');
        const generateReportBtn = DOMHelpers.getElementById('generate-report-btn');
        
        // Show report modal when clicking the report icon
        if (downloadReportBtn) {
            downloadReportBtn.addEventListener('click', () => {
                this.handleReportButtonClick();
            });
        }

        // Generate report from modal
        if (generateReportBtn) {
            generateReportBtn.addEventListener('click', async () => {
                await this.handleReportGeneration();
            });
        }
    }

    /**
     * Handle report button click - FIXED to always sync with backend
     */
    async handleReportButtonClick() {
        try {
            console.log('📊 Report button clicked, checking analysis state...');
            
            // ALWAYS check backend state first (most reliable)
            const response = await fetch('/debug/session_state', {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json'
                }
            });
            
            if (response.ok) {
                const backendState = await response.json();
                console.log('🎯 Backend session state:', backendState);
                
                // Check if analysis is actually complete
                if (backendState.session_state && backendState.session_state.analysis_complete) {
                    console.log('✅ Analysis is complete, proceeding with report generation');
                    
                    // Always sync frontend with backend state
                    SessionDataManager.updateSessionData({ 
                        analysisComplete: true,
                        variablesUsed: backendState.session_state.variables_used || []
                    });
                    
                    // Update status indicator
                    if (window.statusIndicator) {
                        window.statusIndicator.updateStatus();
                    }
                    
                    // USER WANTS PDF + DASHBOARD - Generate directly via chat
                    this.chatManager.addSystemMessage('Generating your PDF report and interactive dashboard...');
                    
                    // Send message to generate PDF report (this will also create dashboard)
                    const chatInput = DOMHelpers.getElementById('message-input');
                    if (chatInput) {
                        chatInput.value = 'Generate PDF report';
                        // Trigger the send message functionality
                        this.chatManager.sendMessage();
                    } else {
                        // Direct API call fallback
                        this.generateReportDirectly();
                    }
                    
                } else {
                    // Analysis not complete
                    console.log('❌ Analysis not complete');
                    this.chatManager.addSystemMessage("Please run an analysis before generating a report.");
                }
            } else {
                // Can't check backend state
                console.error('❌ Could not check backend state');
                this.chatManager.addSystemMessage("Could not check analysis status. Please try running the analysis again.");
            }
            
        } catch (error) {
            console.error('🚨 Error in handleReportButtonClick:', error);
            this.chatManager.addSystemMessage("Error checking analysis status. Please try again.");
        }
    }
    
    /**
     * Generate report directly via API (fallback)
     */
    async generateReportDirectly() {
        try {
            const response = await fetch('/generate_report', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ format: 'pdf' })
            });
            
            const result = await response.json();
            
            if (result.status === 'success') {
                this.chatManager.addSystemMessage(`
                    <strong>Report generated successfully!</strong><br>
                    <a href="${result.download_url}" class="btn btn-primary" target="_blank">
                        📄 Download PDF Report
                    </a>
                `);
            } else {
                this.chatManager.addSystemMessage(`Error generating report: ${result.message}`);
            }
        } catch (error) {
            console.error('Error generating report directly:', error);
            this.chatManager.addSystemMessage('Error generating report. Please try again.');
        }
    }

    /**
     * Handle report generation from modal
     */
    async handleReportGeneration() {
        const formatSelect = DOMHelpers.getElementById('report-format');
        const format = formatSelect ? formatSelect.value : 'pdf';
        
        try {
            // Show loading state
            const generateBtn = DOMHelpers.getElementById('generate-report-btn');
            if (generateBtn) {
                generateBtn.disabled = true;
                generateBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Generating...';
            }

            // Create form data
            const formData = new FormData();
            formData.append('report_format', format);
            formData.append('detail_level', 'standard');

            // Make request to generate report
            const response = await fetch('/generate_report', {
                method: 'POST',
                body: formData
            });

            const result = await response.json();

            if (result.status === 'success') {
                // Success - show download link
                this.chatManager.addSystemMessage(`
                    <strong>Report generated successfully!</strong><br>
                    <a href="${result.download_url}" class="btn btn-success" download target="_blank">
                        <i class="fas fa-download"></i> Download ${format.toUpperCase()} Report
                    </a>
                `);

                // Hide modal
                const reportModalElem = DOMHelpers.getElementById('reportModal');
                if (reportModalElem && window.bootstrap) {
                    const reportModal = bootstrap.Modal.getInstance(reportModalElem);
                    if (reportModal) reportModal.hide();
                }

                // Show additional files if available
                if (result.additional_files && result.additional_files.length > 0) {
                    result.additional_files.forEach(file => {
                        this.chatManager.addSystemMessage(`
                            <strong>Additional ${file.type}:</strong><br>
                            <a href="${file.url}" class="btn btn-info" target="_blank">
                                <i class="fas fa-external-link-alt"></i> View ${file.filename}
                            </a>
                        `);
                    });
                }
                
            } else {
                // Error
                this.chatManager.addSystemMessage(`Failed to generate report: ${result.message}`);
            }

        } catch (error) {
            console.error('Report generation error:', error);
            this.chatManager.addSystemMessage('Failed to generate report. Please try again.');
        } finally {
            // Reset button state
            const generateBtn = DOMHelpers.getElementById('generate-report-btn');
            if (generateBtn) {
                generateBtn.disabled = false;
                generateBtn.innerHTML = '<i class="fas fa-file-alt"></i> Generate Report';
            }
        }
    }

    /**
     * Initialize language functionality
     */
    initializeLanguageFunctionality() {
        const languageSelector = DOMHelpers.getElementById('language-selector');
        
        if (languageSelector) {
            languageSelector.addEventListener('change', async (e) => {
                const newLanguage = e.target.value;
                const currentLanguage = SessionDataManager.getSessionData().currentLanguage;
                
                if (newLanguage !== currentLanguage) {
                    try {
                        await this.apiClient.changeLanguage(newLanguage);
                        SessionDataManager.updateSessionData({ currentLanguage: newLanguage });
                        this.chatManager.addSystemMessage(`Language changed to ${this.getLanguageName(newLanguage)}`);
                    } catch (error) {
                        this.chatManager.addSystemMessage('Failed to change language. Please try again.');
                        console.error('Language change error:', error);
                        // Revert selector
                        e.target.value = currentLanguage;
                    }
                }
            });
        }
    }

    /**
     * Initialize visualization handlers
     */
    initializeVisualizationHandlers() {
        // Expand visualization modal
        DOMHelpers.addEventListenerWithDelegation('.expand-visualization-btn', 'click', (e) => {
            e.preventDefault();
            this.expandVisualization(e.target);
        });

        // Universal pagination handler - works for any visualization type
        DOMHelpers.addEventListenerWithDelegation('.pagination-btn', 'click', (e) => {
            e.preventDefault();
            this.handleUniversalPagination(e.target);
        });

        // Legacy pagination support for existing templates
        DOMHelpers.addEventListenerWithDelegation('.prev-composite', 'click', (e) => {
            e.preventDefault();
            this.handlePagination('composite_map', 'prev', e.target);
        });

        DOMHelpers.addEventListenerWithDelegation('.next-composite', 'click', (e) => {
            e.preventDefault();
            this.handlePagination('composite_map', 'next', e.target);
        });

        DOMHelpers.addEventListenerWithDelegation('.prev-boxplot', 'click', (e) => {
            e.preventDefault();
            this.handlePagination('boxplot', 'prev', e.target);
        });

        DOMHelpers.addEventListenerWithDelegation('.next-boxplot', 'click', (e) => {
            e.preventDefault();
            this.handlePagination('boxplot', 'next', e.target);
        });

        // **ADDED: New pagination button handlers for dynamic visualization types**
        DOMHelpers.addEventListenerWithDelegation('.prev-composite-map', 'click', (e) => {
            e.preventDefault();
            this.handlePagination('composite_map', 'prev', e.target);
        });

        DOMHelpers.addEventListenerWithDelegation('.next-composite-map', 'click', (e) => {
            e.preventDefault();
            this.handlePagination('composite_map', 'next', e.target);
        });

        DOMHelpers.addEventListenerWithDelegation('.prev-vulnerability-plot', 'click', (e) => {
            e.preventDefault();
            this.handlePagination('vulnerability_plot', 'prev', e.target);
        });

        DOMHelpers.addEventListenerWithDelegation('.next-vulnerability-plot', 'click', (e) => {
            e.preventDefault();
            this.handlePagination('vulnerability_plot', 'next', e.target);
        });

        DOMHelpers.addEventListenerWithDelegation('.prev-variable-map', 'click', (e) => {
            e.preventDefault();
            this.handlePagination('variable_map', 'prev', e.target);
        });

        DOMHelpers.addEventListenerWithDelegation('.next-variable-map', 'click', (e) => {
            e.preventDefault();
            this.handlePagination('variable_map', 'next', e.target);
        });
    }

    /**
     * Expand visualization in modal
     * @param {HTMLElement} button - Expand button
     */
    expandVisualization(button) {
        const vizContainer = button.closest('.visualization-container');
        if (!vizContainer) return;

        const iframe = vizContainer.querySelector('iframe');
        const img = vizContainer.querySelector('img.viz-image');
        const titleElem = vizContainer.querySelector('.visualization-title');
        const title = titleElem ? titleElem.textContent : 'Visualization';

        const modalElem = DOMHelpers.getElementById('visualizationModal');
        const modalBody = DOMHelpers.getElementById('visualizationModalBody');
        const modalLabel = DOMHelpers.getElementById('visualizationModalLabel');

        if (!modalElem || !modalBody) return;

        // Set title
        if (modalLabel) modalLabel.textContent = title;

        // Clear and set content
        DOMHelpers.clearChildren(modalBody);

        if (iframe) {
            const newIframe = DOMHelpers.createElement('iframe', {
                src: iframe.src,
                style: 'width: 100%; height: 100%; border: none; border-radius: 8px;'
            });
            
            // Enhanced fullscreen iframe setup
            this.setupFullscreenIframe(newIframe, modalBody);
            modalBody.appendChild(newIframe);
            
        } else if (img) {
            const newImg = DOMHelpers.createElement('img', {
                src: img.src,
                style: 'max-width: 100%; max-height: 100%; object-fit: contain; border-radius: 8px;'
            });
            modalBody.appendChild(newImg);
        }

        // Show modal with enhanced features
        if (window.bootstrap) {
            const modal = new bootstrap.Modal(modalElem);
            modal.show();
            
            // Add fullscreen enhancements after modal is shown
            modalElem.addEventListener('shown.bs.modal', () => {
                this.enhanceFullscreenModal(modalElem, modalBody);
            }, { once: true });
        }
    }

    /**
     * Setup enhanced fullscreen iframe
     */
    setupFullscreenIframe(iframe, container) {
        // Enable all interactive features
        iframe.style.pointerEvents = 'auto';
        iframe.allowFullscreen = true;
        iframe.allow = 'fullscreen';
        
        // Add loading indicator
        const loadingDiv = DOMHelpers.createElement('div', {
            className: 'fullscreen-loading',
            style: `
                position: absolute;
                top: 50%;
                left: 50%;
                transform: translate(-50%, -50%);
                color: var(--text-secondary);
                font-size: 1.2rem;
                z-index: 10;
            `
        }, '<i class="fas fa-spinner fa-spin"></i> <span style="margin-left: 8px;">Loading fullscreen view...</span>');
        
        container.appendChild(loadingDiv);
        
        // Handle iframe load
        iframe.addEventListener('load', () => {
            loadingDiv.style.display = 'none';
            this.optimizeFullscreenContent(iframe);
        });
        
        // Add interaction enhancements
        iframe.addEventListener('mouseenter', () => {
            iframe.style.filter = 'brightness(1.05)';
            iframe.style.transition = 'filter 0.3s ease';
        });
        
        iframe.addEventListener('mouseleave', () => {
            iframe.style.filter = 'brightness(1)';
        });
    }

    /**
     * Enhance fullscreen modal experience
     */
    enhanceFullscreenModal(modalElem, modalBody) {
        // Add keyboard shortcuts
        const handleKeydown = (e) => {
            switch(e.key) {
                case 'Escape':
                    // Let Bootstrap handle this
                    break;
                case 'f':
                case 'F':
                    if (e.ctrlKey || e.metaKey) {
                        e.preventDefault();
                        this.toggleModalFullscreen(modalElem);
                    }
                    break;
                case '+':
                case '=':
                    if (e.ctrlKey || e.metaKey) {
                        e.preventDefault();
                        this.zoomModal(modalBody, 1.1);
                    }
                    break;
                case '-':
                    if (e.ctrlKey || e.metaKey) {
                        e.preventDefault();
                        this.zoomModal(modalBody, 0.9);
                    }
                    break;
                case '0':
                    if (e.ctrlKey || e.metaKey) {
                        e.preventDefault();
                        this.resetModalZoom(modalBody);
                    }
                    break;
            }
        };
        
        document.addEventListener('keydown', handleKeydown);
        
        // Clean up event listener when modal is hidden
        modalElem.addEventListener('hidden.bs.modal', () => {
            document.removeEventListener('keydown', handleKeydown);
        }, { once: true });
        
        // Add zoom controls
        this.addModalZoomControls(modalElem, modalBody);
        
        // Enable touch gestures for mobile
        this.enableModalTouchGestures(modalBody);
    }

    /**
     * Add zoom controls to modal
     */
    addModalZoomControls(modalElem, modalBody) {
        const modalHeader = modalElem.querySelector('.modal-header');
        if (!modalHeader) return;
        
        const zoomControls = DOMHelpers.createElement('div', {
            className: 'modal-zoom-controls',
            style: 'display: flex; gap: 8px; margin-left: auto; margin-right: 16px;'
        });
        
        const zoomInBtn = DOMHelpers.createElement('button', {
            className: 'btn btn-sm btn-outline-secondary',
            title: 'Zoom In (Ctrl/Cmd + +)',
            type: 'button'
        }, '<i class="fas fa-search-plus"></i>');
        
        const zoomOutBtn = DOMHelpers.createElement('button', {
            className: 'btn btn-sm btn-outline-secondary',
            title: 'Zoom Out (Ctrl/Cmd + -)',
            type: 'button'
        }, '<i class="fas fa-search-minus"></i>');
        
        const resetZoomBtn = DOMHelpers.createElement('button', {
            className: 'btn btn-sm btn-outline-secondary',
            title: 'Reset Zoom (Ctrl/Cmd + 0)',
            type: 'button'
        }, '<i class="fas fa-expand-arrows-alt"></i>');
        
        const fullscreenBtn = DOMHelpers.createElement('button', {
            className: 'btn btn-sm btn-outline-primary',
            title: 'Toggle Fullscreen (Ctrl/Cmd + F)',
            type: 'button'
        }, '<i class="fas fa-expand"></i>');
        
        // Add event listeners
        zoomInBtn.addEventListener('click', () => this.zoomModal(modalBody, 1.2));
        zoomOutBtn.addEventListener('click', () => this.zoomModal(modalBody, 0.8));
        resetZoomBtn.addEventListener('click', () => this.resetModalZoom(modalBody));
        fullscreenBtn.addEventListener('click', () => this.toggleModalFullscreen(modalElem));
        
        zoomControls.appendChild(zoomInBtn);
        zoomControls.appendChild(zoomOutBtn);
        zoomControls.appendChild(resetZoomBtn);
        zoomControls.appendChild(fullscreenBtn);
        
        // Insert before close button
        const closeBtn = modalHeader.querySelector('.btn-close');
        modalHeader.insertBefore(zoomControls, closeBtn);
    }

    /**
     * Zoom modal content
     */
    zoomModal(modalBody, factor) {
        const currentScale = parseFloat(modalBody.dataset.scale || '1');
        const newScale = Math.max(0.5, Math.min(3, currentScale * factor));
        
        modalBody.style.transform = `scale(${newScale})`;
        modalBody.style.transformOrigin = 'center center';
        modalBody.dataset.scale = newScale.toString();
        
        // Adjust modal body overflow for zoomed content
        if (newScale > 1) {
            modalBody.style.overflow = 'auto';
        } else {
            modalBody.style.overflow = 'hidden';
        }
    }

    /**
     * Reset modal zoom
     */
    resetModalZoom(modalBody) {
        modalBody.style.transform = 'scale(1)';
        modalBody.style.overflow = 'hidden';
        modalBody.dataset.scale = '1';
    }

    /**
     * Toggle modal fullscreen
     */
    toggleModalFullscreen(modalElem) {
        const modalDialog = modalElem.querySelector('.modal-dialog');
        if (!modalDialog) return;
        
        if (modalDialog.classList.contains('modal-fullscreen')) {
            modalDialog.classList.remove('modal-fullscreen');
            modalDialog.classList.add('modal-xl');
        } else {
            modalDialog.classList.remove('modal-xl');
            modalDialog.classList.add('modal-fullscreen');
        }
    }

    /**
     * Enable touch gestures for mobile
     */
    enableModalTouchGestures(modalBody) {
        let initialDistance = 0;
        let initialScale = 1;
        
        modalBody.addEventListener('touchstart', (e) => {
            if (e.touches.length === 2) {
                initialDistance = this.getTouchDistance(e.touches[0], e.touches[1]);
                initialScale = parseFloat(modalBody.dataset.scale || '1');
            }
        });
        
        modalBody.addEventListener('touchmove', (e) => {
            if (e.touches.length === 2) {
                e.preventDefault();
                const currentDistance = this.getTouchDistance(e.touches[0], e.touches[1]);
                const scale = initialScale * (currentDistance / initialDistance);
                const clampedScale = Math.max(0.5, Math.min(3, scale));
                
                modalBody.style.transform = `scale(${clampedScale})`;
                modalBody.dataset.scale = clampedScale.toString();
            }
        });
    }

    /**
     * Get distance between two touch points
     */
    getTouchDistance(touch1, touch2) {
        const dx = touch1.clientX - touch2.clientX;
        const dy = touch1.clientY - touch2.clientY;
        return Math.sqrt(dx * dx + dy * dy);
    }

    /**
     * Optimize fullscreen content
     */
    optimizeFullscreenContent(iframe) {
        try {
            // Try to access iframe content for optimization
            const iframeDoc = iframe.contentDocument || iframe.contentWindow.document;
            if (iframeDoc) {
                // Optimize for fullscreen viewing
                const body = iframeDoc.body;
                if (body) {
                    body.style.margin = '0';
                    body.style.padding = '0';
                    body.style.overflow = 'auto';
                }
                
                // Optimize Plotly charts for fullscreen
                const plotlyDivs = iframeDoc.querySelectorAll('.plotly-graph-div');
                plotlyDivs.forEach(div => {
                    div.style.width = '100%';
                    div.style.height = '100vh';
                    
                    // Trigger Plotly resize if available
                    if (iframeDoc.defaultView && iframeDoc.defaultView.Plotly) {
                        setTimeout(() => {
                            iframeDoc.defaultView.Plotly.Plots.resize(div);
                        }, 100);
                    }
                });
            }
        } catch (error) {
            console.debug('Cannot optimize iframe content (cross-origin):', error.message);
        }
    }

    /**
     * Handle universal pagination for any visualization type
     * @param {HTMLElement} button - Button element that was clicked
     */
    async handleUniversalPagination(button) {
        try {
            // Get pagination metadata from the button or container
            const container = button.closest('.visualization-container');
            if (!container) {
                console.error('❌ No visualization container found for pagination button');
                return;
            }

            // Extract pagination data - improved direction detection
            let direction = 'next';
            
            // Check button classes and content for direction
            if (button.classList.contains('prev') || 
                button.classList.contains('prev-composite') || 
                button.classList.contains('prev-boxplot') ||
                button.classList.contains('prev-composite-map') ||
                button.classList.contains('prev-vulnerability-plot') ||
                button.classList.contains('prev-variable-map') ||
                button.innerHTML.includes('chevron-left') ||
                button.innerHTML.includes('arrow-left') ||
                button.title.toLowerCase().includes('previous')) {
                direction = 'prev';
            }
            
            console.log('🔄 Pagination direction detected:', direction);
            
            const vizType = container.dataset.vizType || this.detectVisualizationType(container);
            const currentPage = parseInt(container.dataset.currentPage || '1');
            const totalPages = parseInt(container.dataset.totalPages || '1');
            
            console.log('📊 Pagination data:', { vizType, currentPage, totalPages, direction });
            
            // Get additional metadata
            const metadata = this.extractVisualizationMetadata(container);

            // **FIXED: Use specific endpoints for known visualization types**
            let endpoint = '/navigate_visualization';
            let requestBody = {
                viz_type: vizType,
                direction: direction,
                current_page: currentPage,
                total_pages: totalPages,
                metadata: metadata
            };
            
            // Use specific endpoints for composite maps and box plots
            if (vizType === 'composite_map') {
                endpoint = '/navigate_composite_map';
                requestBody = { 
                    direction: direction, 
                    current_page: currentPage 
                };
            } else if (vizType === 'boxplot' || vizType === 'vulnerability_boxplot' || vizType === 'vulnerability_plot') {
                endpoint = '/navigate_boxplot';
                requestBody = { 
                    direction: direction, 
                    current_page: currentPage 
                };
            }

            console.log('🌐 Using endpoint:', endpoint, 'with body:', requestBody);

            // Call navigation endpoint
            const response = await fetch(endpoint, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(requestBody)
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const result = await response.json();
            
            console.log('📥 Navigation response:', result);

            if (result.status === 'success') {
                // Update visualization in place
                this.updateVisualizationFromResult(container, result);
                
                // Show AI response if available
                if (result.ai_response) {
                    this.chatManager.addAssistantMessage(result.ai_response);
                }
                
                console.log('✅ Pagination successful');
            } else {
                console.error('❌ Pagination failed:', result.message);
                this.chatManager.addSystemMessage(result.message || 'Navigation failed');
            }

        } catch (error) {
            console.error('💥 Universal pagination error:', error);
            this.chatManager.addSystemMessage(`Failed to navigate: ${error.message}`);
        }
    }

    /**
     * Detect visualization type from container - improved detection
     * @param {HTMLElement} container - Visualization container
     * @returns {string} Visualization type
     */
    detectVisualizationType(container) {
        // First check data attribute - this is now the primary method
        if (container.dataset.vizType) {
            console.log('✅ Found vizType in data attribute:', container.dataset.vizType);
            return container.dataset.vizType;
        }
        
        // Check for specific button classes (legacy support)
        if (container.querySelector('.prev-composite, .next-composite, .prev-composite-map, .next-composite-map')) {
            console.log('🔍 Detected composite_map from button classes');
            return 'composite_map';
        } else if (container.querySelector('.prev-boxplot, .next-boxplot, .prev-vulnerability-plot, .next-vulnerability-plot')) {
            console.log('🔍 Detected boxplot from button classes');
            return 'boxplot';
        } else if (container.querySelector('.prev-variable-map, .next-variable-map')) {
            console.log('🔍 Detected variable_map from button classes');
            return 'variable_map';
        }
        
        // Check iframe source
        const iframe = container.querySelector('iframe');
        if (iframe && iframe.src) {
            if (iframe.src.includes('composite_map')) {
                console.log('🔍 Detected composite_map from iframe src');
                return 'composite_map';
            } else if (iframe.src.includes('variable_map')) {
                console.log('🔍 Detected variable_map from iframe src');
                return 'variable_map';
            } else if (iframe.src.includes('boxplot') || iframe.src.includes('vulnerability_plot')) {
                console.log('🔍 Detected boxplot from iframe src');
                return 'boxplot';
            }
        }
        
        // Check image source (legacy support)
        const img = container.querySelector('img.viz-image');
        if (img && img.src) {
            if (img.src.includes('composite_map')) {
                console.log('🔍 Detected composite_map from image src');
                return 'composite_map';
            } else if (img.src.includes('variable_map')) {
                console.log('🔍 Detected variable_map from image src');
                return 'variable_map';
            } else if (img.src.includes('boxplot') || img.src.includes('vulnerability_plot')) {
                console.log('🔍 Detected boxplot from image src');
                return 'boxplot';
            }
        }
        
        // Check title for hints (final fallback)
        const titleElement = container.querySelector('.visualization-title');
        if (titleElement) {
            const title = titleElement.textContent.toLowerCase();
            if (title.includes('composite') || title.includes('vulnerability map')) {
                console.log('🔍 Detected composite_map from title');
                return 'composite_map';
            } else if (title.includes('box') || title.includes('plot')) {
                console.log('🔍 Detected boxplot from title');
                return 'boxplot';
            } else if (title.includes('variable')) {
                console.log('🔍 Detected variable_map from title');
                return 'variable_map';
            }
        }
        
        console.warn('⚠️ Could not detect visualization type, using unknown');
        return 'unknown';
    }

    /**
     * Extract visualization metadata from container
     * @param {HTMLElement} container - Visualization container
     * @returns {Object} Metadata object
     */
    extractVisualizationMetadata(container) {
        const metadata = {};
        
        // Extract from data attributes
        Object.keys(container.dataset).forEach(key => {
            if (key.startsWith('viz')) {
                metadata[key.replace('viz', '').toLowerCase()] = container.dataset[key];
            }
        });

        // Extract from form elements (like wards per page)
        const select = container.querySelector('.wards-per-page');
        if (select) {
            metadata.variables_per_page = parseInt(select.value) || 5;
        }

        return metadata;
    }

    /**
     * Update visualization from navigation result
     * @param {HTMLElement} container - Visualization container
     * @param {Object} result - Navigation result
     */
    updateVisualizationFromResult(container, result) {
        // Update iframe or image source
        const iframe = container.querySelector('iframe');
        const img = container.querySelector('img.viz-image');

        if (iframe && result.image_path) {
            iframe.src = result.image_path + '?t=' + Date.now();
        } else if (img && result.image_path) {
            img.src = result.image_path + '?t=' + Date.now();
        }

        // Update container metadata
        if (result.current_page) {
            container.dataset.currentPage = result.current_page;
        }
        if (result.total_pages) {
            container.dataset.totalPages = result.total_pages;
        }
        if (result.viz_type) {
            container.dataset.vizType = result.viz_type;
        }

        // Update pagination controls
        this.updatePaginationControlsFromResult(container, result);
    }

    /**
     * Update pagination controls from result
     * @param {HTMLElement} container - Visualization container
     * @param {Object} result - Navigation result
     */
    updatePaginationControlsFromResult(container, result) {
        const currentPage = result.current_page || 1;
        const totalPages = result.total_pages || 1;

        // Update pagination info
        const paginationInfo = container.querySelector('.pagination-info');
        if (paginationInfo) {
            paginationInfo.textContent = `Page ${currentPage} of ${totalPages}`;
        }

        // Update button states
        const prevBtns = container.querySelectorAll('.prev, .prev-composite, .prev-boxplot, .prev-composite-map, .prev-vulnerability-plot, .prev-variable-map');
        const nextBtns = container.querySelectorAll('.next, .next-composite, .next-boxplot, .next-composite-map, .next-vulnerability-plot, .next-variable-map');

        prevBtns.forEach(btn => {
            btn.disabled = currentPage <= 1;
        });

        nextBtns.forEach(btn => {
            btn.disabled = currentPage >= totalPages;
        });
    }

    /**
     * Legacy pagination handler for backwards compatibility
     * @param {string} type - Visualization type
     * @param {string} direction - Direction (prev/next)
     * @param {HTMLElement} button - Button element
     */
    async handlePagination(type, direction, button) {
        try {
            const container = button.closest('.visualization-container');
            if (!container) return;

            // Set visualization type in container if not already set
            if (!container.dataset.vizType) {
                container.dataset.vizType = type;
            }

            // Use the new universal handler
            await this.handleUniversalPagination(button);
            
        } catch (error) {
            console.error('Legacy pagination error:', error);
            this.chatManager.addSystemMessage('Failed to navigate. Please try again.');
        }
    }

    /**
     * Get language name from code
     * @param {string} code - Language code
     * @returns {string} Language name
     */
    getLanguageName(code) {
        const languages = {
            'en': 'English',
            'ha': 'Hausa',
            'yo': 'Yoruba',
            'ig': 'Igbo',
            'fr': 'French',
            'ar': 'Arabic'
        };
        return languages[code] || code.toUpperCase();
    }

    /**
     * Load initial application state
     */
    loadInitialState() {
        const sessionData = SessionDataManager.getSessionData();
        
        // Set initial language
        const languageSelector = DOMHelpers.getElementById('language-selector');
        if (languageSelector && sessionData.currentLanguage) {
            languageSelector.value = sessionData.currentLanguage;
        }

        // Update status indicator
        if (window.statusIndicator) {
            window.statusIndicator.updateStatus();
        }
    }

    /**
     * Save application state before page unload
     */
    saveApplicationState() {
        try {
            // Save conversation history
            const chatHistory = this.chatManager.getChatHistory();
            SessionDataManager.saveConversationHistory(chatHistory);

            console.log('💾 Application state saved');
        } catch (error) {
            console.error('Failed to save application state:', error);
        }
    }

    /**
     * Handle page hidden
     */
    onPageHidden() {
        console.log('👁️ Page hidden');
        this.saveApplicationState();
    }

    /**
     * Handle page visible
     */
    onPageVisible() {
        console.log('👁️ Page visible');
        // Could refresh status or check for updates
    }

    /**
     * Handle global errors
     * @param {Error} error - Error object
     */
    handleGlobalError(error) {
        console.error('💥 Global error handled:', error);
        
        // Show user-friendly error message
        if (this.chatManager) {
            this.chatManager.addSystemMessage('An unexpected error occurred. Please refresh the page if issues persist.');
        }
    }

    /**
     * Handle initialization errors
     * @param {Error} error - Error object
     */
    handleInitializationError(error) {
        console.error('🚨 Initialization failed:', error);
        
        // Show fallback error message
        const errorDiv = document.createElement('div');
        errorDiv.className = 'alert alert-danger';
        errorDiv.innerHTML = `
            <h4>Application Failed to Load</h4>
            <p>There was an error initializing the application. Please refresh the page to try again.</p>
            <details>
                <summary>Technical Details</summary>
                <pre>${error.message}\n${error.stack}</pre>
            </details>
        `;
        
        document.body.insertBefore(errorDiv, document.body.firstChild);
    }

    /**
     * Get application info
     * @returns {Object} Application information
     */
    getInfo() {
        return {
            version: this.version,
            initialized: this.initialized,
            ready: this.isReady,
            modules: Object.keys(this.modules),
            sessionData: SessionDataManager.getSessionData(),
            settings: SessionDataManager.getSettings()
        };
    }

    /**
     * Reset application to initial state
     */
    reset() {
        console.log('🔄 Resetting application...');
        
        // Reset all modules
        this.chatManager.clearChat();
        this.fileUploader.reset();
        this.sidebarManager.clearNotifications();
        
        // Reset session data
        SessionDataManager.resetSessionData();
        
        // Update UI
        if (window.statusIndicator) {
            window.statusIndicator.updateStatus();
        }
        
        // Reload welcome messages
        this.chatManager.loadWelcomeMessages();
        
        console.log('✅ Application reset complete');
    }
}

// Initialize the application
const app = new ChatMRPTApp();

// Make available globally for debugging and external access
window.ChatMRPTApp = app;
window.chatManager = app.chatManager;
window.sidebarManager = app.sidebarManager;
window.fileUploader = app.fileUploader;
window.themeManager = app.themeManager;
window.sidebarController = app.sidebarController;
window.dataUploadManager = app.dataUploadManager;
window.variableDisplayManager = variableDisplayManager;

// CRITICAL FIX: Debug utilities for scroll issues
window.debugScroll = () => {
    if (window.chatManager && window.chatManager.messageHandler) {
        window.chatManager.messageHandler.debugScrollInfo();
    }
};

window.forceScrollToBottom = () => {
    if (window.chatManager && window.chatManager.messageHandler) {
        window.chatManager.messageHandler.scrollToBottom();
    }
};

export default app; 