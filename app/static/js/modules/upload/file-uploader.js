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
        
        this.init();
    }

    /**
     * Initialize file uploader
     */
    init() {
        this.initElements();
        this.setupEventListeners();
        this.setupModal();
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
     * Handle successful upload
     * @param {Object} response - Upload response
     * @param {File|null} csvFile - CSV file
     * @param {File|null} shapeFile - Shapefile
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

        // Add success message to chat
        if (window.chatManager) {
            window.chatManager.addSystemMessage(
                `Files uploaded successfully! ${csvFile ? `CSV: ${csvFile.name}` : ''} ${shapeFile ? `Shapefile: ${shapeFile.name}` : ''}`
            );

            if (response.message) {
                window.chatManager.addAssistantMessage(response.message);
            }
            
            // Add analysis prompt with Start Analysis button if available
            if (response.analysis_prompt) {
                window.chatManager.addAssistantMessage(response.analysis_prompt);
            }
        }

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

        // Add success message to chat
        if (window.chatManager) {
            window.chatManager.addSystemMessage('Sample data loaded successfully!');
            
            if (response.message) {
                window.chatManager.addAssistantMessage(response.message);
            }
        }

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
        
        // Add error message to chat
        if (window.chatManager) {
            window.chatManager.addSystemMessage(`Upload failed: ${errorMessage}`);
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
        const isValidSize = file.size <= 50 * 1024 * 1024; // 50MB limit
        
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
        const isValidSize = file.size <= 100 * 1024 * 1024; // 100MB limit
        
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