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

        // TPR elements
        this.tprFileInput = DOMHelpers.getElementById('tpr-file-upload');
        this.uploadTprBtn = DOMHelpers.getElementById('upload-tpr-btn');
        this.tprUploadStatus = DOMHelpers.getElementById('tpr-upload-status');
        this.downloadContent = DOMHelpers.getElementById('download-content');

        // Debug TPR elements
        console.log('🔍 TPR Elements Debug:');
        console.log('  - tprFileInput:', !!this.tprFileInput, this.tprFileInput?.id);
        console.log('  - uploadTprBtn:', !!this.uploadTprBtn, this.uploadTprBtn?.id);
        console.log('  - tprUploadStatus:', !!this.tprUploadStatus, this.tprUploadStatus?.id);
        console.log('  - downloadContent:', !!this.downloadContent, this.downloadContent?.id);

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

        // TPR upload button
        if (this.uploadTprBtn) {
            this.uploadTprBtn.addEventListener('click', () => {
                console.log('🖱️ TPR upload button clicked!');
                this.uploadTprFile();
            });
            console.log('✅ TPR upload button found and event listener added');
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
        // 1. Basic confirmation
        window.chatManager.addSystemMessage(`Files uploaded successfully: ${csvFile.name} and ${shapeFile.name}`);

        // 2. What did I find in your data? (the main story)
        if (response.data_summary && response.data_summary.total_rows) {
            const summary = response.data_summary;
            
            const completenessInfo = this.generateCompletenessInfo(summary.data_completeness);
            const variableTypes = this.generateVariableTypesSummary(summary.column_types, summary.column_names);
            
            const discoveryMessage = `I've analyzed your dataset and here's what I found:

**${summary.total_rows} wards** with **${summary.total_columns} variables** including:
${variableTypes}

${completenessInfo}`;
            
            window.chatManager.addAssistantMessage(discoveryMessage);
            
            // 3. Show actual data sample (properly formatted)
            if (summary.preview_rows && summary.preview_rows.length > 0) {
                const sampleMessage = this.generateCleanDataSample(summary.preview_rows, summary.column_names);
                window.chatManager.addAssistantMessage(sampleMessage);
            }
            
            // 4. Only mention issues if there are real problems
            if (summary.data_quality_assessment?.issues?.length > 0) {
                const issuesMessage = `**Data Quality Notes:**
${summary.data_quality_assessment.issues.map(issue => `• ${issue}`).join('\n')}

Your data is still excellent quality and ready for analysis.`;
                
                window.chatManager.addAssistantMessage(issuesMessage);
            }
        }

        // 5. What you can do now
        const nextStepsMessage = `**Ready for Analysis!**

I can help you create:
• **Risk maps** showing high-vulnerability areas
• **Ward rankings** to identify priority locations  
• **Statistical analysis** to find key risk factors
• **Intervention targeting** recommendations

Would you like me to proceed with the composite score and PCA analysis? *(This will create acomprehensive risk assessment with ward rankings.)*`;

        window.chatManager.addAssistantMessage(nextStepsMessage);
    }

    /**
     * Generate clean, readable data sample
     * @param {Array} previewRows - First 5 rows of data
     * @param {Array} columnNames - All column names  
     * @returns {string} Clean data sample
     */
    generateCleanDataSample(previewRows, columnNames) {
        if (!previewRows || previewRows.length === 0) return '';
        
        const sampleSize = Math.min(previewRows.length, 5);
        const rows = previewRows.slice(0, sampleSize);
        
        // Find the most meaningful columns dynamically
        const availableColumns = Object.keys(rows[0]);
        const keyColumns = this.selectKeyColumns(availableColumns);
        
        let sampleText = `**Data Sample** (${sampleSize} wards):\n\n`;
        
        rows.forEach((row, index) => {
            const wardName = row['WardName'] || row['ward_name'] || `Ward ${index + 1}`;
            const details = keyColumns.map(col => {
                const value = row[col];
                if (col === 'WardName' || col === 'ward_name') return null; // Skip ward name in details
                
                // Format numeric values nicely
                if (typeof value === 'number') {
                    return `${col}: ${value > 1 ? Math.round(value * 100) / 100 : (value * 100).toFixed(1)}`;
                }
                return `${col}: ${value || 'N/A'}`;
            }).filter(Boolean).join(', ');
            
            sampleText += `• **${wardName}** - ${details}\n`;
        });
        
        return sampleText;
    }

    /**
     * Select the most meaningful columns for preview
     * @param {Array} availableColumns - All available column names
     * @returns {Array} Selected key columns
     */
    selectKeyColumns(availableColumns) {
        // Priority columns in order of importance
        const priorityColumns = [
            'WardName', 'ward_name', 'Urban', 'urban', 'LGACode', 'tpr', 'housing_quality', 
            'population', 'pfpr', 'mean_rainfall', 'elevation', 'flood', 'Source'
        ];
        
        const selected = [];
        for (const col of priorityColumns) {
            if (availableColumns.includes(col) && selected.length < 4) {
                selected.push(col);
            }
        }
        
        // If we don't have enough, add some others
        if (selected.length < 3) {
            const remaining = availableColumns.filter(col => !selected.includes(col)).slice(0, 3);
            selected.push(...remaining);
        }
        
        return selected;
    }

    /**
     * Generate completeness information with missing variables
     * @param {Object} completenessData - Data completeness object
     * @returns {string} Completeness information
     */
    generateCompletenessInfo(completenessData) {
        if (!completenessData) return 'Your data appears complete with excellent quality.';
        
        const overall = completenessData.overall || 100;
        const byColumn = completenessData.by_column || {};
        
        // Find variables with missing data
        const missingVars = Object.entries(byColumn)
            .filter(([, completeness]) => completeness < 100)
            .sort(([, a], [, b]) => a - b)
            .slice(0, 3); // Show worst 3
        
        if (missingVars.length === 0) {
            return `Your data is **${overall}% complete** with excellent quality.`;
        }
        
        const missingList = missingVars.map(([col, completeness]) => 
            `${col} (${(100 - completeness).toFixed(1)}% missing)`
        ).join(', ');
        
        return `Your data is **${overall}% complete**. Variables with missing values: ${missingList}. During analysis, these will be imputed using spatial neighbor means.`;
    }

    /**
     * Generate variable types summary based on actual data
     * @param {Object} columnTypes - Column type mapping
     * @param {Array} columnNames - All column names
     * @returns {string} Variable types summary
     */
    generateVariableTypesSummary(columnTypes, columnNames) {
        if (!columnTypes || !columnNames) return '';
        
        const typeGroups = {
            numeric: [],
            categorical: [],
            text: []
        };
        
        Object.entries(columnTypes).forEach(([col, type]) => {
            if (typeGroups[type]) {
                typeGroups[type].push(col);
            }
        });
        
        const summary = [];
        if (typeGroups.numeric.length > 0) {
            // Show some example numeric variables
            const examples = typeGroups.numeric.slice(0, 3).join(', ');
            summary.push(`• **${typeGroups.numeric.length} quantitative variables** (${examples}${typeGroups.numeric.length > 3 ? ', ...' : ''})`);
        }
        if (typeGroups.categorical.length > 0) {
            const examples = typeGroups.categorical.slice(0, 2).join(', ');
            summary.push(`• **${typeGroups.categorical.length} categorical variables** (${examples}${typeGroups.categorical.length > 2 ? ', ...' : ''})`);
        }
        if (typeGroups.text.length > 0) {
            summary.push(`• **${typeGroups.text.length} identifier variables** (ward names, codes)`);
        }
        
        return summary.join('\n');
    }

    /**
     * Send proactive message based on upload type
     * @param {Object} response - Backend response
     * @param {File} csvFile - Uploaded CSV file
     * @param {File} shapeFile - Uploaded shapefile
     */
    sendProactiveMessage(response, csvFile, shapeFile) {
        const uploadType = response.upload_type;
        let proactiveMessage = '';
        
        switch (uploadType) {
            case 'csv_shapefile':
                proactiveMessage = "Data upload and analysis complete. Please proceed with comprehensive malaria risk analysis using both composite scoring and PCA methods for optimal results.";
                break;
            case 'tpr_only':
                proactiveMessage = "TPR dataset uploaded successfully. Please initiate climate variable enhancement and spatial analysis pipeline for comprehensive risk assessment.";
                break;
            case 'csv_only':
                proactiveMessage = "CSV dataset processed successfully. Please begin statistical analysis - note that geographic visualization will be limited without boundary data.";
                break;
            default:
                proactiveMessage = "Dataset upload complete. Please specify your preferred analysis methodology for malaria risk assessment.";
        }
        
        // Proactive message removed - let user initiate analysis themselves
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

    /**
     * Upload TPR file for processing
     */
    async uploadTprFile() {
        console.log('📊 Starting TPR upload...');
        
        const tprFileInput = DOMHelpers.getElementById('tpr-file-upload');
        console.log('🔍 TPR Debug - tprFileInput element:', tprFileInput);
        console.log('🔍 TPR Debug - tprFileInput exists:', !!tprFileInput);
        console.log('🔍 TPR Debug - files length:', tprFileInput?.files?.length || 0);
        
        if (!tprFileInput || !tprFileInput.files || tprFileInput.files.length === 0) {
            this.setTprUploadStatus('Please select a TPR file to upload', 'error');
            return;
        }
        
        const file = tprFileInput.files[0];
        console.log('🔍 TPR Debug - Selected file:', file);
        console.log('🔍 TPR Debug - File name:', file.name);
        console.log('🔍 TPR Debug - File size:', file.size);
        console.log('🔍 TPR Debug - File type:', file.type);
        
        // Validate file type
        const allowedExtensions = ['.csv', '.xlsx', '.xls'];
        const fileExtension = '.' + file.name.split('.').pop().toLowerCase();
        
        if (!allowedExtensions.includes(fileExtension)) {
            this.setTprUploadStatus('Invalid file format. Please upload a CSV or Excel file', 'error');
            return;
        }
        
        try {
            this.setTprUploadStatus('Processing TPR data...', 'info');
            this.disableTprUploadButton(true);
            
            // Create FormData
            const formData = new FormData();
            formData.append('csv_file', file);
            console.log('🔍 TPR Debug - FormData created with csv_file key');
            console.log('🔍 TPR Debug - FormData entries:', [...formData.entries()]);
            
            const response = await apiClient.uploadFiles(formData, null);
            
            console.log('🔍 TPR Response:', response);
            
            // Handle different response statuses
            if (response.status === 'success') {
                this.handleTprUploadSuccess(response);
            } else if (response.status === 'requires_state_selection') {
                console.log('🗺️ Multiple states detected, handling state selection');
                this.handleStateSelection(response);
            } else {
                this.handleTprUploadError(response.message || 'TPR upload failed');
            }
            
        } catch (error) {
            console.error('TPR upload error:', error);
            this.handleTprUploadError(error.message || 'Failed to upload TPR file');
        } finally {
            this.disableTprUploadButton(false);
        }
    }
    
    /**
     * Handle state selection when multiple states are detected
     */
    handleStateSelection(response) {
        console.log('🗺️ Showing state selection interface');
        
        const tprUploadStatus = DOMHelpers.getElementById('tpr-upload-status');
        if (!tprUploadStatus) return;
        
        const availableStates = response.available_states || [];
        
        const stateSelectionHTML = `
            <div class="alert alert-info">
                <h6><i class="fas fa-map-marked-alt me-2"></i>Multiple States Detected</h6>
                <p class="mb-3">Your TPR file contains data for ${availableStates.length} states. Please select one state for analysis:</p>
                <div class="d-grid gap-2">
                    ${availableStates.map(state => `
                        <button type="button" class="btn btn-outline-primary state-select-btn" data-state="${state}">
                            <i class="fas fa-map-marker-alt me-2"></i>${state}
                        </button>
                    `).join('')}
                </div>
            </div>
        `;
        
        tprUploadStatus.innerHTML = stateSelectionHTML;
        
        // Add event listeners for state selection buttons
        const stateButtons = tprUploadStatus.querySelectorAll('.state-select-btn');
        stateButtons.forEach(button => {
            button.addEventListener('click', () => {
                const selectedState = button.getAttribute('data-state');
                this.processSelectedState(selectedState);
            });
        });
    }
    
    /**
     * Process the selected state
     */
    async processSelectedState(selectedState) {
        console.log('🗺️ Processing TPR data for state:', selectedState);
        
        try {
            this.setTprUploadStatus(`Processing TPR data for ${selectedState}...`, 'info');
            
            // Get the original file again
            const tprFileInput = DOMHelpers.getElementById('tpr-file-upload');
            const file = tprFileInput.files[0];
            
            // Create FormData with selected state
            const formData = new FormData();
            formData.append('csv_file', file);
            formData.append('selected_state', selectedState);
            
            const response = await apiClient.uploadFiles(formData, null);
            
            if (response.status === 'success') {
                this.handleTprUploadSuccess(response, selectedState);
            } else {
                this.handleTprUploadError(response.message || 'State processing failed');
            }
            
        } catch (error) {
            console.error('State processing error:', error);
            this.handleTprUploadError(error.message || 'Failed to process selected state');
        }
    }
    
    /**
     * Handle successful TPR upload
     */
    handleTprUploadSuccess(response, selectedState = null) {
        const stateName = selectedState || response.target_state || 'the selected state';
        
        this.setTprUploadStatus(`TPR data processed successfully for ${stateName}!`, 'success');
        
        // Update session data
        SessionDataManager.updateSessionData({
            csvLoaded: true,
            shapefileLoaded: true,  // TPR processing includes boundaries
            tprProcessed: true,
            selectedState: stateName
        });
        
        this.updateSessionStatus();
        
        // Show download options
        this.showTprDownloadOptions(response);
        
        // Add success message to chat
        this.addChatMessage(() => {
            window.chatManager.addSystemMessage(`TPR data successfully processed for ${stateName}`);
            
            if (response.message) {
                window.chatManager.addAssistantMessage(response.message);
            }
            
            const convergenceResult = response.convergence_result || {};
            const enhancedMessage = `🧬 **TPR Data Successfully Processed**

📊 **Summary:**
• Wards extracted: ${convergenceResult.extracted_wards || response.extracted_wards || 'N/A'}
• Environmental variables: ${convergenceResult.variables_included || response.variables_included || 'N/A'}
• State: ${convergenceResult.selected_state || stateName}
• Shapefile included: ${convergenceResult.has_shapefile ? 'Yes' : 'No'}

Your data is now ready for region-aware malaria risk analysis!`;
            
            window.chatManager.addAssistantMessage(enhancedMessage);
        });
        
        // Close modal after delay
        setTimeout(() => {
            this.hideUploadModal();
        }, 3000);
    }
    
    /**
     * Handle TPR upload error
     */
    handleTprUploadError(errorMessage) {
        this.setTprUploadStatus(`Processing failed: ${errorMessage}`, 'error');
        console.error('TPR upload error:', errorMessage);
    }
    
    /**
     * Show download options for processed TPR data
     */
    showTprDownloadOptions(response) {
        const downloadContent = DOMHelpers.getElementById('download-content');
        if (!downloadContent) return;
        
        // Get convergence result information
        const convergenceResult = response.convergence_result || {};
        const stateName = convergenceResult.state_name || convergenceResult.selected_state || 'processed state';
        const wardCount = convergenceResult.extracted_wards || 0;
        const hasShapefile = convergenceResult.has_shapefile || false;
        const actualSelectedState = convergenceResult.selected_state || stateName;
        
        downloadContent.innerHTML = `
            <div class="mt-3">
                <h6><i class="fas fa-download me-2"></i>Download Your Convergence Data:</h6>
                <p class="text-muted small">Ready for main workflow: ${wardCount} wards from ${actualSelectedState}</p>
                <div class="d-grid gap-2">
                    <a href="/api/download/convergence-csv/${stateName}" class="btn btn-success btn-sm">
                        <i class="fas fa-table me-2"></i>Download ${stateName}_plus.csv
                    </a>
                    <a href="/api/download/convergence-shapefile/${stateName}" class="btn btn-info btn-sm">
                        <i class="fas fa-map me-2"></i>Download ${stateName}_state.zip
                    </a>
                </div>
                <div class="mt-2">
                    <small class="text-muted">
                        <i class="fas fa-info-circle me-1"></i>
                        These files are ready for the main ChatMRPT analysis workflow.
                    </small>
                </div>
            </div>
        `;
    }
    
    /**
     * Set TPR upload status
     */
    setTprUploadStatus(message, type = 'info') {
        const statusElement = DOMHelpers.getElementById('tpr-upload-status');
        if (!statusElement) return;
        
        const iconMap = {
            'info': 'fas fa-info-circle text-info',
            'success': 'fas fa-check-circle text-success', 
            'error': 'fas fa-exclamation-circle text-danger',
            'pending': 'fas fa-spinner fa-spin text-primary'
        };
        
        const alertClass = {
            'info': 'alert-info',
            'success': 'alert-success',
            'error': 'alert-danger', 
            'pending': 'alert-primary'
        };
        
        statusElement.innerHTML = `
            <div class="alert ${alertClass[type]} mb-0">
                <i class="${iconMap[type]} me-2"></i>${message}
            </div>
        `;
    }
    
    /**
     * Disable/enable TPR upload button
     */
    disableTprUploadButton(disabled) {
        if (this.uploadTprBtn) {
            this.uploadTprBtn.disabled = disabled;
            if (disabled) {
                this.uploadTprBtn.innerHTML = '<i class="fas fa-spinner fa-spin me-2"></i>Processing...';
            } else {
                this.uploadTprBtn.innerHTML = '<i class="fas fa-magic me-2"></i>Process TPR Data';
            }
        }
    }
}

// Create and export singleton instance
const fileUploader = new FileUploader();

export default fileUploader; 