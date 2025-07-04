/**
 * File Uploader Module
 * Handles file uploads, validation, and progress tracking
 */

import DOMHelpers from '../utils/dom-helpers.js';
import { SessionDataManager } from '../utils/storage.js';
import apiClient from '../utils/api-client.js';

export class FileUploader {
    constructor() {
        this.uploadModal = null;
        this.uploadButton = null;
        this.uploadFilesBtn = null;
        this.csvFileInput = null;
        this.shapefileInput = null;
        this.filesUploadStatus = null;
        this.useSampleDataBtn = null;
        
        this.allowedCsvTypes = ['.csv', '.xlsx', '.xls'];
        this.allowedShapefileTypes = ['.zip'];
        
        this.maxCsvSize = 50 * 1024 * 1024; // 50MB
        this.maxShapefileSize = 100 * 1024 * 1024; // 100MB
        
        this.chatManagerReady = false;
        
        this.init();
    }

    /**
     * Initialize file uploader
     */
    init() {
        console.log('📁 FileUploader initializing...');
        this.initElements();
        this.setupEventListeners();
        this.setupModal();
        
        this.waitForChatManager();
        
        console.log('✅ FileUploader initialized');
    }

    /**
     * Wait for chat manager to be ready before enabling full functionality
     */
    waitForChatManager() {
        if (window.chatManager && typeof window.chatManager.addSystemMessage === 'function') {
            this.chatManagerReady = true;
            console.log('✅ Chat manager is ready for file upload messages');
            return;
        }
        
        document.addEventListener('chatMRPTReady', () => {
            if (window.chatManager && typeof window.chatManager.addSystemMessage === 'function') {
                this.chatManagerReady = true;
                console.log('✅ Chat manager is ready for file upload messages');
            }
        });
        
        let attempts = 0;
        const checkInterval = setInterval(() => {
            attempts++;
            if (window.chatManager && typeof window.chatManager.addSystemMessage === 'function') {
                this.chatManagerReady = true;
                console.log('✅ Chat manager is ready for file upload messages (via polling)');
                clearInterval(checkInterval);
            } else if (attempts >= 20) {
                console.warn('⚠️ Chat manager not ready after 10 seconds, proceeding without chat messages');
                clearInterval(checkInterval);
            }
        }, 500);
    }

    /**
     * Initialize DOM elements
     */
    initElements() {
        this.uploadButton = DOMHelpers.getElementById('upload-button');
        this.uploadFilesBtn = DOMHelpers.getElementById('upload-files-btn');
        this.csvFileInput = DOMHelpers.getElementById('csv-upload');
        this.shapefileInput = DOMHelpers.getElementById('shapefile-upload');
        this.filesUploadStatus = DOMHelpers.getElementById('files-upload-status');
        this.useSampleDataBtn = DOMHelpers.getElementById('use-sample-data-btn-modal');

        // Initialize Bootstrap modal
        const uploadModalElem = DOMHelpers.getElementById('uploadModal');
        if (uploadModalElem && window.bootstrap) {
            this.uploadModal = new bootstrap.Modal(uploadModalElem);
        }
    }

    /**
     * Setup event listeners
     */
    setupEventListeners() {
        // Show upload modal
        if (this.uploadButton) {
            this.uploadButton.addEventListener('click', () => {
                this.showUploadModal();
            });
        }

        // Upload files button
        if (this.uploadFilesBtn) {
            this.uploadFilesBtn.addEventListener('click', () => {
                this.uploadFiles();
            });
        }

        // Sample data button
        if (this.useSampleDataBtn) {
            this.useSampleDataBtn.addEventListener('click', () => {
                this.loadSampleData();
            });
        }

        // File input validation
        if (this.csvFileInput) {
            this.csvFileInput.addEventListener('change', () => {
                this.validateCsvFile();
            });
        }

        if (this.shapefileInput) {
            this.shapefileInput.addEventListener('change', () => {
                this.validateShapefileFile();
            });
        }

        // Handle sample data link in chat (using event delegation)
        DOMHelpers.addEventListenerWithDelegation('#use-sample-data-btn-initial', 'click', (e) => {
            e.preventDefault();
            this.loadSampleData();
        });
    }

    /**
     * Setup modal behavior
     */
    setupModal() {
        const uploadModalElem = DOMHelpers.getElementById('uploadModal');
        if (uploadModalElem) {
            uploadModalElem.addEventListener('shown.bs.modal', () => {
                // Focus on first input when modal opens
                if (this.csvFileInput) {
                    this.csvFileInput.focus();
                }
            });

            uploadModalElem.addEventListener('hidden.bs.modal', () => {
                // Clear status when modal closes
                this.clearUploadStatus();
            });
        }
    }

    /**
     * Show upload modal
     */
    showUploadModal() {
        if (this.uploadModal) {
            this.uploadModal.show();
        }
    }

    /**
     * Hide upload modal
     */
    hideUploadModal() {
        if (this.uploadModal) {
            this.uploadModal.hide();
        }
    }

    /**
     * Upload files to backend
     */
    async uploadFiles() {
        const csvFile = this.csvFileInput?.files[0] || null;
        const shapeFile = this.shapefileInput?.files[0] || null;

        // Validate that at least one file is selected
        if (!csvFile && !shapeFile) {
            this.setUploadStatus('Please select at least one file to upload', 'error');
            return;
        }

        // Validate file types
        if (csvFile && !this.isValidCsvFile(csvFile)) {
            this.setUploadStatus('Invalid file format. Please upload a CSV or Excel file', 'error');
            return;
        }

        if (shapeFile && !this.isValidShapefileFile(shapeFile)) {
            this.setUploadStatus('Invalid shapefile format. Please upload a ZIP file', 'error');
            return;
        }

        try {
            this.setUploadStatus('Uploading files...', 'info');
            
            const response = await apiClient.uploadFiles(csvFile, shapeFile);
            
            // Check if we have data even if status is error
            if (response.csv_result && response.csv_result.data && response.csv_result.data.length > 0) {
                response.csv_result.status = 'success';
            }
            
            if (response.status === 'success' || 
                (response.csv_result && response.csv_result.status === 'success') ||
                (response.shapefile_result && response.shapefile_result.status === 'success')) {
                this.handleUploadSuccess(response, csvFile, shapeFile);
            } else {
                let errorMessage = 'One or more file uploads failed';
                if (response.csv_result && response.csv_result.message) {
                    errorMessage = response.csv_result.message;
                } else if (response.shapefile_result && response.shapefile_result.message) {
                    errorMessage = response.shapefile_result.message;
                }
                this.handleUploadError(errorMessage);
            }
        } catch (error) {
            console.error('Upload error:', error);
            this.handleUploadError(error.message || 'Failed to upload files');
        }
    }

    /**
     * Load sample data
     */
    async loadSampleData() {
        try {
            this.setUploadStatus('Loading sample data...', 'pending');
            this.disableUploadButton(true);

            const response = await apiClient.loadSampleData();

            if (response.status === 'success') {
                this.handleSampleDataSuccess(response);
            } else {
                this.handleUploadError(response.message || 'Failed to load sample data');
            }
        } catch (error) {
            this.handleUploadError(error.message || 'Failed to load sample data');
        } finally {
            this.disableUploadButton(false);
        }
    }

    /**
     * Handle upload success
     * @param {Object} response - Upload response
     * @param {File} csvFile - Uploaded CSV file
     * @param {File} shapeFile - Uploaded shapefile
     */
    handleUploadSuccess(response, csvFile, shapeFile) {
        this.setUploadStatus('Files uploaded successfully!', 'success');

        // Update session data
        const updates = {};
        if (csvFile) updates.csvLoaded = true;
        if (shapeFile) updates.shapefileLoaded = true;
        SessionDataManager.updateSessionData(updates);

        // Update UI status
        this.updateSessionStatus();

        // Add Phase 1 enhanced success message to chat
        this.addChatMessage(() => {
            // Show upload type detection results
            this.displayUploadResults(response, csvFile, shapeFile);
            
            // Send proactive trigger based on upload type
            this.sendProactiveMessage(response, csvFile, shapeFile);
        });

        // Close modal after delay
        setTimeout(() => {
            this.hideUploadModal();
            this.clearFileInputs();
        }, 2000);
    }

    /**
     * Handle sample data success
     * @param {Object} response - Sample data response
     */
    handleSampleDataSuccess(response) {
        this.setUploadStatus('Sample data loaded successfully!', 'success');
        
        // Update session data
        SessionDataManager.updateSessionData({
            csvLoaded: true,
            shapefileLoaded: true
        });

        // Update UI status
        this.updateSessionStatus();

        // Add success message to chat - WITH IMPROVED SAFETY CHECK
        this.addChatMessage(() => {
            window.chatManager.addSystemMessage('Sample data loaded successfully!');
            
            if (response.message) {
                window.chatManager.addAssistantMessage(response.message);
            }
            
            // Send proactive trigger message to epidemiologist
            setTimeout(() => {
                window.chatManager.sendMessage("I've loaded the sample data. Can you analyze what's available and recommend what analysis to run?");
            }, 1000);
        });

        // Close modal after delay
        setTimeout(() => {
            this.hideUploadModal();
        }, 1500);
    }

    /**
     * Handle upload error
     * @param {string} errorMessage - Error message
     */
    handleUploadError(errorMessage) {
        this.setUploadStatus(errorMessage, 'error');
        
        // Add error message to chat - WITH IMPROVED SAFETY CHECK
        this.addChatMessage(() => {
            window.chatManager.addSystemMessage(`Upload failed: ${errorMessage}`);
        });
    }

    /**
     * Safely add a message to chat when ready
     * @param {Function} messageFunction - Function to execute when chat manager is ready
     */
    addChatMessage(messageFunction) {
        if (this.chatManagerReady && window.chatManager && typeof window.chatManager.addSystemMessage === 'function') {
            try {
                messageFunction();
            } catch (error) {
                console.error('Error adding chat message:', error);
            }
        } else {
            console.warn('Chat manager not ready, retrying in 500ms...');
            // Retry after a short delay
            setTimeout(() => {
                if (window.chatManager && typeof window.chatManager.addSystemMessage === 'function') {
                    try {
                        messageFunction();
                    } catch (error) {
                        console.error('Error adding delayed chat message:', error);
                    }
                } else {
                    console.warn('Chat manager still not ready, message will be skipped');
                }
            }, 500);
        }
    }

    /**
     * Validate CSV file
     * @returns {boolean} Validation result
     */
    validateCsvFile() {
        const file = this.csvFileInput?.files[0];
        if (!file) return true;

        if (!this.isValidCsvFile(file)) {
            this.setUploadStatus('Invalid CSV/Excel file format', 'warning');
            return false;
        }

        this.clearUploadStatus();
        return true;
    }

    /**
     * Validate shapefile
     * @returns {boolean} Validation result
     */
    validateShapefileFile() {
        const file = this.shapefileInput?.files[0];
        if (!file) return true;

        if (!this.isValidShapefileFile(file)) {
            this.setUploadStatus('Invalid shapefile format. Please upload a ZIP file', 'warning');
            return false;
        }

        this.clearUploadStatus();
        return true;
    }

    /**
     * Check if file is valid CSV/Excel
     * @param {File} file - File to validate
     * @returns {boolean} Validation result
     */
    isValidCsvFile(file) {
        const extension = this.getFileExtension(file.name);
        const isValidType = this.allowedCsvTypes.includes(extension);
        const isValidSize = file.size <= this.maxCsvSize;
        
        return isValidType && isValidSize;
    }

    /**
     * Check if file is valid shapefile ZIP
     * @param {File} file - File to validate
     * @returns {boolean} Validation result
     */
    isValidShapefileFile(file) {
        const extension = this.getFileExtension(file.name);
        const isValidType = this.allowedShapefileTypes.includes(extension);
        const isValidSize = file.size <= this.maxShapefileSize;
        
        return isValidType && isValidSize;
    }

    /**
     * Get file extension
     * @param {string} filename - File name
     * @returns {string} File extension
     */
    getFileExtension(filename) {
        return filename.toLowerCase().substring(filename.lastIndexOf('.'));
    }

    /**
     * Set upload status message
     * @param {string} message - Status message
     * @param {string} type - Status type (success, error, warning, pending)
     */
    setUploadStatus(message, type) {
        if (!this.filesUploadStatus) return;

        this.filesUploadStatus.textContent = message;
        this.filesUploadStatus.className = `upload-status ${type}`;

        // Add spinner for pending status
        if (type === 'pending') {
            const spinner = DOMHelpers.createElement('span', {
                className: 'spinner-border spinner-border-sm me-2'
            });
            this.filesUploadStatus.insertBefore(spinner, this.filesUploadStatus.firstChild);
        }
    }

    /**
     * Clear upload status
     */
    clearUploadStatus() {
        if (this.filesUploadStatus) {
            this.filesUploadStatus.textContent = '';
            this.filesUploadStatus.className = 'upload-status';
        }
    }

    /**
     * Disable/enable upload button
     * @param {boolean} disabled - Disable state
     */
    disableUploadButton(disabled) {
        if (this.uploadFilesBtn) {
            this.uploadFilesBtn.disabled = disabled;
        }
        if (this.useSampleDataBtn) {
            this.useSampleDataBtn.disabled = disabled;
        }
    }

    /**
     * Clear file inputs
     */
    clearFileInputs() {
        if (this.csvFileInput) {
            this.csvFileInput.value = '';
        }
        if (this.shapefileInput) {
            this.shapefileInput.value = '';
        }
        this.clearUploadStatus();
    }

    /**
     * Update session status indicator
     */
    updateSessionStatus() {
        if (window.statusIndicator) {
            window.statusIndicator.updateStatus();
        }
    }

    /**
     * Get upload progress
     * @returns {Object} Upload progress information
     */
    getUploadProgress() {
        const sessionData = SessionDataManager.getSessionData();
        return {
            csvLoaded: sessionData.csvLoaded,
            shapefileLoaded: sessionData.shapefileLoaded,
            bothLoaded: sessionData.csvLoaded && sessionData.shapefileLoaded,
            analysisReady: sessionData.csvLoaded && sessionData.shapefileLoaded
        };
    }

    /**
     * Display Phase 1 upload results in chat
     * @param {Object} response - Backend response with enhanced structure
     * @param {File} csvFile - Uploaded CSV file
     * @param {File} shapeFile - Uploaded shapefile
     */
    displayUploadResults(response, csvFile, shapeFile) {
        // 1. Simple, clear confirmation  
        const fileSize1 = (csvFile.size / (1024 * 1024)).toFixed(1);
        const fileSize2 = shapeFile ? (shapeFile.size / (1024 * 1024)).toFixed(1) : null;
        
        if (csvFile && shapeFile) {
            window.chatManager.addSystemMessage(`Perfect! Received **${csvFile.name}** (${fileSize1}MB) and **${shapeFile.name}** (${fileSize2}MB)`);
        } else {
            window.chatManager.addSystemMessage(`Perfect! Received **${csvFile.name}** (${fileSize1}MB)`);
        }

        if (response.data_summary && response.data_summary.total_rows) {
            const summary = response.data_summary;
            
            // 2. Data discovery message with variable categories
            const variableCategories = this.categorizeVariables(summary.column_names, summary.column_types);
            const recordLabel = this.detectRecordType(summary.column_names);
            
            const discoveryMessage = `I've analyzed your dataset and here's what I found:

**${summary.total_rows} ${recordLabel}** across **${summary.total_columns} variables** including:

${variableCategories}`;
            
            window.chatManager.addAssistantMessage(discoveryMessage);
            
            // 3. Data completeness info
            const completenessInfo = this.generateCompletenessInfo(summary.data_completeness);
            if (completenessInfo) {
                window.chatManager.addAssistantMessage(completenessInfo);
            }
            
            // 4. Data sample
            if (summary.preview_rows && summary.preview_rows.length > 0) {
                const sampleMessage = this.generateProfessionalDataSample(summary.preview_rows, summary.column_names);
                window.chatManager.addAssistantMessage(sampleMessage);
            }
            
            // 5. Ready for analysis - following workflow diagram
            setTimeout(() => {
                const nextStepsMessage = `**Ready for Analysis!** 

I can proceed with the full malaria risk analysis (composite scoring + PCA) to generate risk maps and ward rankings, or we can first explore your data variables.

**Option 1:** Proceed with complete analysis (recommended)
**Option 2:** Visualize variables first (e.g., "show me the distribution of pfpr variable")

May I proceed with the comprehensive risk analysis?`;
                window.chatManager.addAssistantMessage(nextStepsMessage);
            }, 300);
        }
    }







    /**
     * Generate completeness information with missing variables
     * @param {Object} completenessData - Data completeness object
     * @returns {string} Completeness information
     */
    generateCompletenessInfo(completenessData) {
        if (!completenessData) return null;
        
        const overall = completenessData.overall || 100;
        const byColumn = completenessData.by_column || {};
        
        // Find variables with missing data
        const missingVars = Object.entries(byColumn)
            .filter(([, completeness]) => completeness < 100)
            .sort(([, a], [, b]) => a - b)
            .slice(0, 3); // Show worst 3
        
        if (missingVars.length === 0 || overall >= 99) {
            return `Your data is **${overall.toFixed(1)}% complete** with excellent quality scores.`;
        }
        
        const missingList = missingVars.map(([col, completeness]) => 
            `${col} (${(100 - completeness).toFixed(1)}% missing)`
        ).join(', ');
        
        return `Your data is **${overall.toFixed(1)}% complete**. Variables with missing values: ${missingList}. During analysis, these will be imputed using spatial neighbor means.`;
    }





    /**
     * Send proactive message based on upload type
     * @param {Object} response - Backend response
     * @param {File} csvFile - Uploaded CSV file
     * @param {File} shapeFile - Uploaded shapefile
     */
    sendProactiveMessage(response, csvFile, shapeFile) {
        // Don't send automatic proactive messages that trigger analysis
        // The "Ready for Analysis" section already provides clear next steps
        // Let the user decide what to do next
        return;
    }

    /**
     * Categorize variables into meaningful groups
     * @param {Array} columnNames - All column names
     * @param {Object} columnTypes - Column type mapping
     * @returns {string} Formatted variable categories
     */
    categorizeVariables(columnNames, columnTypes) {
        if (!columnNames || columnNames.length === 0) return '';
        
        const categories = {
            environmental: [],
            demographic: [],
            geographic: [],
            health: [],
            other: []
        };
        
        // Categorize each column
        columnNames.forEach(col => {
            const colLower = col.toLowerCase();
            
            if (colLower.includes('rainfall') || colLower.includes('temperature') || 
                colLower.includes('ndvi') || colLower.includes('humidity') || 
                colLower.includes('elevation')) {
                categories.environmental.push(col);
            } else if (colLower.includes('population') || colLower.includes('density') || 
                       colLower.includes('housing') || colLower.includes('urban')) {
                categories.demographic.push(col);
            } else if (colLower.includes('ward') || colLower.includes('lga') || 
                       colLower.includes('state') || colLower.includes('region')) {
                categories.geographic.push(col);
            } else if (colLower.includes('health') || colLower.includes('facility') || 
                       colLower.includes('distance') || colLower.includes('pfpr') ||
                       colLower.includes('tpr') || colLower.includes('test')) {
                categories.health.push(col);
            } else {
                categories.other.push(col);
            }
        });
        
        // Format output
        const output = [];
        if (categories.environmental.length > 0) {
            output.push(`• Environmental factors (${categories.environmental.join(', ')})`);
        }
        if (categories.demographic.length > 0) {
            output.push(`• Demographic data (${categories.demographic.join(', ')})`);
        }
        if (categories.geographic.length > 0) {
            output.push(`• Geographic indicators (${categories.geographic.join(', ')})`);
        }
        if (categories.health.length > 0) {
            output.push(`• Health-related variables (${categories.health.join(', ')})`);
        }
        
        return output.join('\n');
    }

    /**
     * Detect record type from column names
     * @param {Array} columnNames - Column names
     * @returns {string} Record type label
     */
    detectRecordType(columnNames) {
        if (!columnNames) return 'records';
        
        const colsLower = columnNames.map(c => c.toLowerCase()).join(' ');
        
        if (colsLower.includes('ward')) return 'wards';
        if (colsLower.includes('district')) return 'districts';
        if (colsLower.includes('region')) return 'regions';
        if (colsLower.includes('village')) return 'villages';
        if (colsLower.includes('community')) return 'communities';
        if (colsLower.includes('facility')) return 'facilities';
        
        return 'locations';
    }

    /**
     * Generate professional data sample
     * @param {Array} previewRows - Sample rows
     * @param {Array} columnNames - Column names
     * @returns {string} Formatted data sample
     */
    generateProfessionalDataSample(previewRows, columnNames) {
        if (!previewRows || previewRows.length === 0) return '';
        
        const sampleSize = Math.min(previewRows.length, 5);
        const rows = previewRows.slice(0, sampleSize);
        
        // Find identifier column
        const idCol = this.findIdentifierColumn(columnNames);
        const recordType = this.detectRecordType(columnNames);
        
        let sampleText = `**Data Sample** (showing ${sampleSize} of ${sampleSize} ${recordType}):\n\n`;
        
        rows.forEach((row, index) => {
            const wardNum = index + 1;
            const identifier = row[idCol] || `${recordType.slice(0, -1)} ${wardNum}`;
            
            // Get key data points
            const keyData = this.extractKeyDataPoints(row, columnNames);
            
            sampleText += `**${recordType.slice(0, -1).charAt(0).toUpperCase() + recordType.slice(0, -1).slice(1)} ${wardNum}**: **${identifier}**`;
            
            if (keyData.length > 0) {
                sampleText += ` • ${keyData.join(' • ')}`;
            }
            
            sampleText += '\n';
        });
        
        return sampleText;
    }

    /**
     * Find the identifier column (ward name, etc)
     * @param {Array} columnNames - Column names
     * @returns {string} Identifier column name
     */
    findIdentifierColumn(columnNames) {
        const patterns = ['wardname', 'ward_name', 'name', 'district', 'region', 'location'];
        
        for (const col of columnNames) {
            const colLower = col.toLowerCase().replace(/[_\s-]/g, '');
            for (const pattern of patterns) {
                if (colLower.includes(pattern)) {
                    return col;
                }
            }
        }
        
        return columnNames[0]; // Fallback to first column
    }

    /**
     * Extract key data points for display
     * @param {Object} row - Data row
     * @param {Array} columnNames - Column names
     * @returns {Array} Key data points formatted
     */
    extractKeyDataPoints(row, columnNames) {
        const keyData = [];
        const seen = new Set();
        
        // Priority columns to show
        const priorities = [
            { pattern: 'lgacode', format: (val) => `LGACode: ${val}` },
            { pattern: 'urban', format: (val) => `Urban: ${val}` },
            { pattern: 'housing_quality', format: (val) => `housing_quality: ${this.formatNumber(val)}` },
            { pattern: 'mean_rainfall', format: (val) => `mean_rainfall: ${this.formatNumber(val)}` },
            { pattern: 'pfpr', format: (val) => `pfpr: ${this.formatNumber(val)}` },
            { pattern: 'tpr', format: (val) => `tpr: ${this.formatNumber(val)}` },
            { pattern: 'test_positivity', format: (val) => `test_positivity: ${this.formatNumber(val)}` }
        ];
        
        // Match columns to priorities
        for (const priority of priorities) {
            for (const col of columnNames) {
                const colLower = col.toLowerCase().replace(/[_\s-]/g, '');
                if (colLower.includes(priority.pattern) && !seen.has(priority.pattern)) {
                    const value = row[col];
                    if (value !== null && value !== undefined && value !== '') {
                        keyData.push(priority.format(value));
                        seen.add(priority.pattern);
                        break;
                    }
                }
            }
        }
        
        return keyData.slice(0, 4); // Show max 4 data points
    }

    /**
     * Format numbers intelligently
     * @param {*} value - Value to format
     * @returns {string} Formatted value
     */
    formatNumber(value) {
        if (typeof value !== 'number') return value;
        
        // Very small numbers (likely percentages as decimals)
        if (value > 0 && value < 1) {
            return value.toFixed(6).replace(/\.?0+$/, '');
        }
        
        // Larger numbers
        if (value > 1000) {
            return value.toFixed(1);
        }
        
        // Regular numbers
        return value.toFixed(2).replace(/\.?0+$/, '');
    }

    /**
     * Reset uploader state
     */
    reset() {
        this.clearFileInputs();
        this.clearUploadStatus();
        this.disableUploadButton(false);
        
        // Reset session data
        SessionDataManager.updateSessionData({
            csvLoaded: false,
            shapefileLoaded: false,
            analysisComplete: false
        });
        
        this.updateSessionStatus();
    }
}

// Create and export singleton instance
const fileUploader = new FileUploader();

export default fileUploader; 