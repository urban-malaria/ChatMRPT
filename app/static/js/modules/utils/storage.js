/**
 * Storage Utilities Module
 * Handles session and local storage with JSON serialization
 */

export class StorageManager {
    /**
     * Set item in localStorage
     * @param {string} key - Storage key
     * @param {any} value - Value to store (will be JSON stringified)
     * @returns {boolean} Success status
     */
    static setLocal(key, value) {
        try {
            localStorage.setItem(key, JSON.stringify(value));
            return true;
        } catch (error) {
            console.error('Error setting localStorage item:', error);
            return false;
        }
    }

    /**
     * Get item from localStorage
     * @param {string} key - Storage key
     * @param {any} defaultValue - Default value if key doesn't exist
     * @returns {any} Stored value or default value
     */
    static getLocal(key, defaultValue = null) {
        try {
            const item = localStorage.getItem(key);
            return item ? JSON.parse(item) : defaultValue;
        } catch (error) {
            console.error('Error getting localStorage item:', error);
            return defaultValue;
        }
    }

    /**
     * Remove item from localStorage
     * @param {string} key - Storage key
     * @returns {boolean} Success status
     */
    static removeLocal(key) {
        try {
            localStorage.removeItem(key);
            return true;
        } catch (error) {
            console.error('Error removing localStorage item:', error);
            return false;
        }
    }

    /**
     * Clear all localStorage items
     * @returns {boolean} Success status
     */
    static clearLocal() {
        try {
            localStorage.clear();
            return true;
        } catch (error) {
            console.error('Error clearing localStorage:', error);
            return false;
        }
    }

    /**
     * Set item in sessionStorage
     * @param {string} key - Storage key
     * @param {any} value - Value to store (will be JSON stringified)
     * @returns {boolean} Success status
     */
    static setSession(key, value) {
        try {
            sessionStorage.setItem(key, JSON.stringify(value));
            return true;
        } catch (error) {
            console.error('Error setting sessionStorage item:', error);
            return false;
        }
    }

    /**
     * Get item from sessionStorage
     * @param {string} key - Storage key
     * @param {any} defaultValue - Default value if key doesn't exist
     * @returns {any} Stored value or default value
     */
    static getSession(key, defaultValue = null) {
        try {
            const item = sessionStorage.getItem(key);
            return item ? JSON.parse(item) : defaultValue;
        } catch (error) {
            console.error('Error getting sessionStorage item:', error);
            return defaultValue;
        }
    }

    /**
     * Remove item from sessionStorage
     * @param {string} key - Storage key
     * @returns {boolean} Success status
     */
    static removeSession(key) {
        try {
            sessionStorage.removeItem(key);
            return true;
        } catch (error) {
            console.error('Error removing sessionStorage item:', error);
            return false;
        }
    }

    /**
     * Clear all sessionStorage items
     * @returns {boolean} Success status
     */
    static clearSession() {
        try {
            sessionStorage.clear();
            return true;
        } catch (error) {
            console.error('Error clearing sessionStorage:', error);
            return false;
        }
    }

    /**
     * Check if storage is available
     * @param {string} type - 'localStorage' or 'sessionStorage'
     * @returns {boolean} Availability status
     */
    static isStorageAvailable(type = 'localStorage') {
        try {
            const storage = window[type];
            const testKey = '__storage_test__';
            storage.setItem(testKey, 'test');
            storage.removeItem(testKey);
            return true;
        } catch (error) {
            return false;
        }
    }

    /**
     * Get storage usage information
     * @param {string} type - 'localStorage' or 'sessionStorage'
     * @returns {Object} Storage usage info
     */
    static getStorageInfo(type = 'localStorage') {
        if (!this.isStorageAvailable(type)) {
            return { available: false, total: 0, used: 0, remaining: 0 };
        }

        try {
            const storage = window[type];
            let totalUsed = 0;
            let itemCount = 0;

            for (let key in storage) {
                if (storage.hasOwnProperty(key)) {
                    totalUsed += storage[key].length + key.length;
                    itemCount++;
                }
            }

            // Estimate total storage (typically 5-10MB for localStorage)
            const estimatedTotal = 5 * 1024 * 1024; // 5MB in bytes
            
            return {
                available: true,
                total: estimatedTotal,
                used: totalUsed,
                remaining: estimatedTotal - totalUsed,
                itemCount: itemCount,
                usagePercentage: (totalUsed / estimatedTotal * 100).toFixed(2)
            };
        } catch (error) {
            console.error('Error getting storage info:', error);
            return { available: false, total: 0, used: 0, remaining: 0 };
        }
    }
}

/**
 * Session Data Manager for ChatMRPT specific data
 */
export class SessionDataManager {
    static SESSION_KEY = 'chatmrpt_session';
    static SETTINGS_KEY = 'chatmrpt_settings';

    /**
     * Get current session data
     * @returns {Object} Session data object
     */
    static getSessionData() {
        return StorageManager.getSession(this.SESSION_KEY, {
            csvLoaded: false,
            shapefileLoaded: false,
            analysisComplete: false,
            currentLanguage: 'en',
            currentCompositePage: 1,
            totalCompositePages: 1,
            currentBoxPlotPage: 1,
            totalBoxPlotPages: 1,
            boxPlotWardsPerPage: 20,
            lastVisualization: null,
            conversationContext: {},
            analysisMetadata: {}
        });
    }

    /**
     * Update session data
     * @param {Object} updates - Partial data to update
     * @returns {boolean} Success status
     */
    static updateSessionData(updates) {
        const currentData = this.getSessionData();
        const newData = { ...currentData, ...updates };
        return StorageManager.setSession(this.SESSION_KEY, newData);
    }

    /**
     * Reset session data to defaults
     * @returns {boolean} Success status
     */
    static resetSessionData() {
        return StorageManager.removeSession(this.SESSION_KEY);
    }

    /**
     * Get user settings
     * @returns {Object} Settings object
     */
    static getSettings() {
        return StorageManager.getLocal(this.SETTINGS_KEY, {
            theme: 'light',
            language: 'en',
            compactView: false,
            autoScroll: true,
            cookiesAccepted: false
        });
    }

    /**
     * Update user settings
     * @param {Object} updates - Partial settings to update
     * @returns {boolean} Success status
     */
    static updateSettings(updates) {
        const currentSettings = this.getSettings();
        const newSettings = { ...currentSettings, ...updates };
        return StorageManager.setLocal(this.SETTINGS_KEY, newSettings);
    }

    /**
     * Reset settings to defaults
     * @returns {boolean} Success status
     */
    static resetSettings() {
        return StorageManager.removeLocal(this.SETTINGS_KEY);
    }

    /**
     * Save conversation history
     * @param {Array} messages - Message history array
     * @returns {boolean} Success status
     */
    static saveConversationHistory(messages) {
        const sessionData = this.getSessionData();
        sessionData.conversationHistory = messages;
        return StorageManager.setSession(this.SESSION_KEY, sessionData);
    }

    /**
     * Get conversation history
     * @returns {Array} Message history array
     */
    static getConversationHistory() {
        const sessionData = this.getSessionData();
        return sessionData.conversationHistory || [];
    }

    /**
     * Clear conversation history
     * @returns {boolean} Success status
     */
    static clearConversationHistory() {
        const sessionData = this.getSessionData();
        sessionData.conversationHistory = [];
        return StorageManager.setSession(this.SESSION_KEY, sessionData);
    }

    /**
     * Save available variables for autocomplete
     * @param {Array<string>} variables - Available variables
     * @returns {boolean} Success status
     */
    static saveAvailableVariables(variables) {
        return StorageManager.setSession('chatmrpt_variables', variables);
    }

    /**
     * Get available variables
     * @returns {Array<string>} Available variables
     */
    static getAvailableVariables() {
        return StorageManager.getSession('chatmrpt_variables', []);
    }
}

export default { StorageManager, SessionDataManager }; 