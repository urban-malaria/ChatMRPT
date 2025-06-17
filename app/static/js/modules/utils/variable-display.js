/**
 * Variable Display Utility Module
 * Handles intelligent variable display names using LLM-powered backend
 * No more hardcoded variable mappings!
 */

class VariableDisplayManager {
    constructor() {
        this.cache = new Map();
        this.pendingRequests = new Map();
        
        console.log('🎯 VariableDisplayManager initialized - LLM-powered variable display');
    }

    /**
     * Get display name for a variable using intelligent backend
     * @param {string} varCode - Variable code/column name
     * @returns {Promise<string>} Display name
     */
    async getDisplayName(varCode) {
        if (!varCode) return 'Unknown Variable';
        
        // Check cache first
        if (this.cache.has(varCode)) {
            return this.cache.get(varCode).display_name;
        }
        
        // Check if request is already pending
        if (this.pendingRequests.has(varCode)) {
            return await this.pendingRequests.get(varCode);
        }
        
        // Create new request
        const requestPromise = this._fetchVariableMetadata(varCode);
        this.pendingRequests.set(varCode, requestPromise);
        
        try {
            const metadata = await requestPromise;
            return metadata.display_name;
        } finally {
            this.pendingRequests.delete(varCode);
        }
    }

    /**
     * Get comprehensive metadata for a variable
     * @param {string} varCode - Variable code/column name  
     * @returns {Promise<Object>} Full metadata object
     */
    async getMetadata(varCode) {
        if (!varCode) {
            return {
                display_name: 'Unknown Variable',
                description: 'No description available',
                unit: 'Unknown',
                category: 'Uncategorized',
                type: 'unknown'
            };
        }
        
        // Check cache first
        if (this.cache.has(varCode)) {
            return this.cache.get(varCode);
        }
        
        // Check if request is already pending
        if (this.pendingRequests.has(varCode)) {
            return await this.pendingRequests.get(varCode);
        }
        
        // Create new request
        const requestPromise = this._fetchVariableMetadata(varCode);
        this.pendingRequests.set(varCode, requestPromise);
        
        try {
            return await requestPromise;
        } finally {
            this.pendingRequests.delete(varCode);
        }
    }

    /**
     * Format variable for display with optional unit and description
     * @param {string} varCode - Variable code
     * @param {Object} options - Formatting options
     * @returns {Promise<string>} Formatted display text
     */
    async formatVariable(varCode, options = {}) {
        const {
            includeUnit = true,
            includeDescription = false,
            fallback = varCode
        } = options;
        
        try {
            const metadata = await this.getMetadata(varCode);
            
            let displayText = metadata.display_name;
            
            if (includeUnit && metadata.unit && metadata.unit !== 'Unknown') {
                displayText += ` (${metadata.unit})`;
            }
            
            if (includeDescription && metadata.description) {
                displayText += ` - ${metadata.description}`;
            }
            
            return displayText;
        } catch (error) {
            console.warn('Error formatting variable:', varCode, error);
            return fallback;
        }
    }

    /**
     * Get all variable metadata and organize by category
     * @returns {Promise<Object>} Variables organized by category
     */
    async getAllVariableMetadata() {
        try {
            const response = await fetch('/api/variable_metadata', {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json'
                }
            });
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const data = await response.json();
            
            if (data.status === 'success') {
                // Cache all variables
                Object.entries(data.variables).forEach(([varCode, metadata]) => {
                    this.cache.set(varCode, metadata);
                });
                
                return {
                    variables: data.variables,
                    categories: data.categories,
                    count: data.count
                };
            } else {
                throw new Error(data.message || 'Failed to get variable metadata');
            }
            
        } catch (error) {
            console.error('Error getting all variable metadata:', error);
            throw error;
        }
    }

    /**
     * Preload metadata for multiple variables (batch optimization)
     * @param {string[]} varCodes - Array of variable codes to preload
     * @returns {Promise<void>}
     */
    async preloadVariables(varCodes) {
        if (!Array.isArray(varCodes) || varCodes.length === 0) return;
        
        console.log(`🔄 Preloading metadata for ${varCodes.length} variables...`);
        
        // Filter out already cached variables
        const uncachedVars = varCodes.filter(varCode => !this.cache.has(varCode));
        
        if (uncachedVars.length === 0) {
            console.log('✅ All variables already cached');
            return;
        }
        
        try {
            // Use the batch endpoint
            const result = await this.getAllVariableMetadata();
            console.log(`✅ Preloaded metadata for ${Object.keys(result.variables).length} variables`);
        } catch (error) {
            console.error('Error preloading variables:', error);
            // Fallback to individual requests for uncached variables
            await Promise.allSettled(
                uncachedVars.map(varCode => this.getMetadata(varCode))
            );
        }
    }

    /**
     * Replace variable codes with display names in text
     * @param {string} text - Text containing variable codes
     * @param {string[]} varCodes - Known variable codes to replace
     * @returns {Promise<string>} Text with display names
     */
    async replaceVariableCodesInText(text, varCodes = []) {
        if (!text || varCodes.length === 0) return text;
        
        let updatedText = text;
        
        // Replace each variable code with its display name
        for (const varCode of varCodes) {
            try {
                const displayName = await this.getDisplayName(varCode);
                
                // Replace exact matches (case-insensitive)
                const regex = new RegExp(`\\b${varCode.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'gi');
                updatedText = updatedText.replace(regex, displayName);
                
            } catch (error) {
                console.warn(`Failed to get display name for ${varCode}:`, error);
            }
        }
        
        return updatedText;
    }

    /**
     * Clear the cache (useful for new data uploads)
     */
    clearCache() {
        this.cache.clear();
        this.pendingRequests.clear();
        console.log('🧹 Variable metadata cache cleared');
    }

    /**
     * Get cache statistics
     * @returns {Object} Cache stats
     */
    getCacheStats() {
        return {
            cachedVariables: this.cache.size,
            pendingRequests: this.pendingRequests.size,
            cacheEntries: Array.from(this.cache.keys())
        };
    }

    /**
     * Private method to fetch variable metadata from API
     * @param {string} varCode - Variable code
     * @returns {Promise<Object>} Variable metadata
     * @private
     */
    async _fetchVariableMetadata(varCode) {
        try {
            const response = await fetch(`/api/variable_metadata?variable=${encodeURIComponent(varCode)}`, {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json'
                }
            });
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const data = await response.json();
            
            if (data.status === 'success') {
                // Cache the result
                this.cache.set(varCode, data.metadata);
                return data.metadata;
            } else {
                throw new Error(data.message || 'Failed to get variable metadata');
            }
            
        } catch (error) {
            console.error(`Error fetching metadata for ${varCode}:`, error);
            
            // Return fallback metadata
            const fallback = this._createFallbackMetadata(varCode);
            this.cache.set(varCode, fallback);
            return fallback;
        }
    }

    /**
     * Create fallback metadata when API fails
     * @param {string} varCode - Variable code
     * @returns {Object} Fallback metadata
     * @private
     */
    _createFallbackMetadata(varCode) {
        // Simple fallback logic
        let displayName = varCode;
        if (varCode.includes('_')) {
            displayName = varCode.split('_')
                .map(word => word.charAt(0).toUpperCase() + word.slice(1))
                .join(' ');
        } else {
            displayName = varCode.charAt(0).toUpperCase() + varCode.slice(1);
        }
        
        return {
            display_name: displayName,
            description: `Data variable: ${displayName}`,
            unit: 'Unknown',
            category: 'Uncategorized',
            type: 'continuous'
        };
    }
}

// Create and export singleton instance
const variableDisplayManager = new VariableDisplayManager();

// Make available globally for easy access
window.variableDisplayManager = variableDisplayManager;

export default variableDisplayManager; 