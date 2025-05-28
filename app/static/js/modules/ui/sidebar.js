/**
 * Sidebar Manager Module
 * Handles sidebar toggle, settings, and UI state management
 */

import DOMHelpers from '../utils/dom-helpers.js';
import { SessionDataManager } from '../utils/storage.js';

export class SidebarManager {
    constructor() {
        this.sidebar = null;
        this.sidebarToggle = null;
        this.closeSidebarBtn = null;
        this.isOpen = false;
        
        this.init();
    }

    /**
     * Initialize sidebar manager
     */
    init() {
        this.sidebar = DOMHelpers.getElementById('app-sidebar');
        this.sidebarToggle = DOMHelpers.getElementById('sidebar-toggle');
        this.closeSidebarBtn = DOMHelpers.getElementById('close-sidebar');
        
        this.setupEventListeners();
        this.setupSettingsHandlers();
        this.loadSettings();
    }

    /**
     * Setup event listeners for sidebar functionality
     */
    setupEventListeners() {
        // Toggle sidebar on button click
        if (this.sidebarToggle) {
            this.sidebarToggle.addEventListener('click', (e) => {
                e.preventDefault();
                this.toggle();
            });
        }

        // Close sidebar on close button click
        if (this.closeSidebarBtn) {
            this.closeSidebarBtn.addEventListener('click', () => {
                this.close();
            });
        }

        // Close sidebar when clicking outside
        document.addEventListener('click', (e) => {
            if (this.isOpen && 
                this.sidebar && 
                !this.sidebar.contains(e.target) && 
                e.target !== this.sidebarToggle && 
                !this.sidebarToggle.contains(e.target)) {
                this.close();
            }
        });

        // Close sidebar on Escape key
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && this.isOpen) {
                this.close();
            }
        });
    }

    /**
     * Setup settings handlers for dark mode, compact view, etc.
     */
    setupSettingsHandlers() {
        // Dark mode toggle
        const darkModeToggle = DOMHelpers.getElementById('dark-mode-toggle');
        if (darkModeToggle) {
            darkModeToggle.addEventListener('change', (e) => {
                this.toggleDarkMode(e.target.checked);
            });
        }

        // Compact view toggle
        const compactViewToggle = DOMHelpers.getElementById('compact-view-toggle');
        if (compactViewToggle) {
            compactViewToggle.addEventListener('change', (e) => {
                this.toggleCompactView(e.target.checked);
            });
        }

        // Cookie acceptance
        const cookieBanner = DOMHelpers.getElementById('cookie-banner');
        const acceptCookiesBtn = DOMHelpers.getElementById('accept-cookies');
        
        if (acceptCookiesBtn) {
            acceptCookiesBtn.addEventListener('click', () => {
                this.acceptCookies();
                if (cookieBanner) {
                    DOMHelpers.hide(cookieBanner);
                }
            });
        }
    }

    /**
     * Load saved settings
     */
    loadSettings() {
        const settings = SessionDataManager.getSettings();

        // Apply dark mode
        if (settings.theme === 'dark') {
            this.toggleDarkMode(true, false);
            const darkModeToggle = DOMHelpers.getElementById('dark-mode-toggle');
            if (darkModeToggle) darkModeToggle.checked = true;
        }

        // Apply compact view
        if (settings.compactView) {
            this.toggleCompactView(true, false);
            const compactViewToggle = DOMHelpers.getElementById('compact-view-toggle');
            if (compactViewToggle) compactViewToggle.checked = true;
        }

        // Show/hide cookie banner
        const cookieBanner = DOMHelpers.getElementById('cookie-banner');
        if (cookieBanner) {
            if (settings.cookiesAccepted) {
                DOMHelpers.hide(cookieBanner);
            } else {
                DOMHelpers.show(cookieBanner);
            }
        }
    }

    /**
     * Toggle sidebar open/closed
     */
    toggle() {
        if (this.isOpen) {
            this.close();
        } else {
            this.open();
        }
    }

    /**
     * Open sidebar
     */
    open() {
        if (!this.sidebar) return;

        this.isOpen = true;
        DOMHelpers.addClass(this.sidebar, 'open');
        DOMHelpers.addClass(document.body, 'sidebar-open');
        
        // Add ARIA attributes for accessibility
        this.sidebar.setAttribute('aria-hidden', 'false');
        if (this.sidebarToggle) {
            this.sidebarToggle.setAttribute('aria-expanded', 'true');
        }

        // Focus on close button for keyboard accessibility
        if (this.closeSidebarBtn) {
            setTimeout(() => this.closeSidebarBtn.focus(), 100);
        }
    }

    /**
     * Close sidebar
     */
    close() {
        if (!this.sidebar) return;

        this.isOpen = false;
        DOMHelpers.removeClass(this.sidebar, 'open');
        DOMHelpers.removeClass(document.body, 'sidebar-open');
        
        // Update ARIA attributes
        this.sidebar.setAttribute('aria-hidden', 'true');
        if (this.sidebarToggle) {
            this.sidebarToggle.setAttribute('aria-expanded', 'false');
        }

        // Return focus to toggle button
        if (this.sidebarToggle) {
            this.sidebarToggle.focus();
        }
    }

    /**
     * Toggle dark mode
     * @param {boolean} enabled - Enable dark mode
     * @param {boolean} save - Save setting to storage
     */
    toggleDarkMode(enabled, save = true) {
        if (enabled) {
            DOMHelpers.addClass(document.body, 'dark-mode');
        } else {
            DOMHelpers.removeClass(document.body, 'dark-mode');
        }

        if (save) {
            SessionDataManager.updateSettings({ 
                theme: enabled ? 'dark' : 'light' 
            });
        }

        // Dispatch custom event for other modules to listen to
        document.dispatchEvent(new CustomEvent('themeChanged', {
            detail: { theme: enabled ? 'dark' : 'light' }
        }));
    }

    /**
     * Toggle compact view
     * @param {boolean} enabled - Enable compact view
     * @param {boolean} save - Save setting to storage
     */
    toggleCompactView(enabled, save = true) {
        if (enabled) {
            DOMHelpers.addClass(document.body, 'compact-view');
        } else {
            DOMHelpers.removeClass(document.body, 'compact-view');
        }

        if (save) {
            SessionDataManager.updateSettings({ compactView: enabled });
        }

        // Dispatch custom event
        document.dispatchEvent(new CustomEvent('viewModeChanged', {
            detail: { compactView: enabled }
        }));
    }

    /**
     * Accept cookies and privacy policy
     */
    acceptCookies() {
        SessionDataManager.updateSettings({ cookiesAccepted: true });
        
        // Dispatch custom event
        document.dispatchEvent(new CustomEvent('cookiesAccepted', {
            detail: { timestamp: Date.now() }
        }));
    }

    /**
     * Get current sidebar state
     * @returns {Object} Sidebar state
     */
    getState() {
        return {
            isOpen: this.isOpen,
            settings: SessionDataManager.getSettings()
        };
    }

    /**
     * Update sidebar content
     * @param {string} section - Section to update
     * @param {string} content - New content HTML
     */
    updateContent(section, content) {
        const sectionElement = this.sidebar?.querySelector(`.sidebar-section.${section}`);
        if (sectionElement) {
            sectionElement.innerHTML = content;
        }
    }

    /**
     * Add custom section to sidebar
     * @param {string} title - Section title
     * @param {string} content - Section content HTML
     * @param {string} className - Optional CSS class
     */
    addSection(title, content, className = '') {
        if (!this.sidebar) return;

        const sidebarContent = this.sidebar.querySelector('.sidebar-content');
        if (!sidebarContent) return;

        const sectionDiv = DOMHelpers.createElement('div', {
            className: `sidebar-section ${className}`
        }, `
            <h4>${title}</h4>
            ${content}
        `);

        sidebarContent.appendChild(sectionDiv);
    }

    /**
     * Remove section from sidebar
     * @param {string} className - Section CSS class to remove
     */
    removeSection(className) {
        if (!this.sidebar) return;

        const section = this.sidebar.querySelector(`.sidebar-section.${className}`);
        if (section) {
            section.remove();
        }
    }

    /**
     * Show notification in sidebar
     * @param {string} message - Notification message
     * @param {string} type - Notification type (info, success, warning, error)
     * @param {number} duration - Auto-hide duration in ms (0 = no auto-hide)
     */
    showNotification(message, type = 'info', duration = 5000) {
        const notificationDiv = DOMHelpers.createElement('div', {
            className: `sidebar-notification notification-${type}`
        }, `
            <div class="notification-content">
                <i class="fas fa-${this.getNotificationIcon(type)}"></i>
                <span>${message}</span>
                <button class="notification-close" aria-label="Close notification">
                    <i class="fas fa-times"></i>
                </button>
            </div>
        `);

        // Add to sidebar
        if (this.sidebar) {
            const sidebarContent = this.sidebar.querySelector('.sidebar-content');
            if (sidebarContent) {
                sidebarContent.insertBefore(notificationDiv, sidebarContent.firstChild);
            }
        }

        // Add close handler
        const closeBtn = notificationDiv.querySelector('.notification-close');
        if (closeBtn) {
            closeBtn.addEventListener('click', () => {
                notificationDiv.remove();
            });
        }

        // Auto-hide if duration specified
        if (duration > 0) {
            setTimeout(() => {
                if (notificationDiv.parentNode) {
                    notificationDiv.remove();
                }
            }, duration);
        }

        return notificationDiv;
    }

    /**
     * Get icon for notification type
     * @param {string} type - Notification type
     * @returns {string} Font Awesome icon name
     */
    getNotificationIcon(type) {
        const icons = {
            info: 'info-circle',
            success: 'check-circle',
            warning: 'exclamation-triangle',
            error: 'times-circle'
        };
        return icons[type] || 'info-circle';
    }

    /**
     * Clear all notifications
     */
    clearNotifications() {
        if (!this.sidebar) return;

        const notifications = this.sidebar.querySelectorAll('.sidebar-notification');
        notifications.forEach(notification => notification.remove());
    }
}

// Create and export singleton instance
const sidebarManager = new SidebarManager();

export default sidebarManager; 