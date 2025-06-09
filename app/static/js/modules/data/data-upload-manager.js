/**
 * Data Upload Manager Module
 * Handles data upload coordination, session management, and upload state tracking
 */

import { SessionDataManager } from '../utils/storage.js';
import apiClient from '../utils/api-client.js';
import DOMHelpers from '../utils/dom-helpers.js';

export class DataUploadManager {
    constructor() {
        this.isInitialized = false;
        this.uploadState = {
            hasData: false,
            csvFile: null,
            shapeFile: null,
            isUploading: false,
            lastUploadTime: null
        };
        
        this.init();
    }

    /**
     * Initialize the data upload manager
     */
    init() {
        if (this.isInitialized) return;
        
        console.log('📊 DataUploadManager initializing...');
        this.loadUploadState();
        this.setupEventListeners();
        this.isInitialized = true;
        console.log('✅ DataUploadManager initialized');
    }

    /**
     * Setup event listeners for upload-related events
     */
    setupEventListeners() {
        // Listen for successful uploads from FileUploader
        document.addEventListener('fileUploadSuccess', (event) => {
            this.handleUploadSuccess(event.detail);
        });

        // Listen for upload errors
        document.addEventListener('fileUploadError', (event) => {
            this.handleUploadError(event.detail);
        });

        // Listen for session changes
        document.addEventListener('sessionChanged', (event) => {
            this.handleSessionChange(event.detail);
        });
    }

    /**
     * Handle successful file upload
     */
    handleUploadSuccess(uploadData) {
        this.uploadState.hasData = true;
        this.uploadState.csvFile = uploadData.csvFile || null;
        this.uploadState.shapeFile = uploadData.shapeFile || null;
        this.uploadState.isUploading = false;
        this.uploadState.lastUploadTime = new Date().toISOString();
        
        this.saveUploadState();
        this.notifyUploadStateChange();
        
        console.log('✅ Upload state updated:', this.uploadState);
    }

    /**
     * Handle upload error
     */
    handleUploadError(errorData) {
        this.uploadState.isUploading = false;
        this.saveUploadState();
        this.notifyUploadStateChange();
        
        console.error('❌ Upload error:', errorData);
    }

    /**
     * Handle session change
     */
    handleSessionChange(sessionData) {
        // Reset upload state for new session
        this.resetUploadState();
        console.log('🔄 Upload state reset for new session');
    }

    /**
     * Check if data has been uploaded
     */
    hasUploadedData() {
        return this.uploadState.hasData;
    }

    /**
     * Get current upload state
     */
    getUploadState() {
        return { ...this.uploadState };
    }

    /**
     * Set uploading status
     */
    setUploading(isUploading) {
        this.uploadState.isUploading = isUploading;
        this.saveUploadState();
        this.notifyUploadStateChange();
    }

    /**
     * Reset upload state
     */
    resetUploadState() {
        this.uploadState = {
            hasData: false,
            csvFile: null,
            shapeFile: null,
            isUploading: false,
            lastUploadTime: null
        };
        this.saveUploadState();
        this.notifyUploadStateChange();
    }

    /**
     * Load upload state from session storage
     */
    loadUploadState() {
        const sessionData = SessionDataManager.getSessionData();
        const savedState = sessionData.uploadState;
        if (savedState) {
            this.uploadState = { ...this.uploadState, ...savedState };
        }
    }

    /**
     * Save upload state to session storage
     */
    saveUploadState() {
        SessionDataManager.updateSessionData({ uploadState: this.uploadState });
    }

    /**
     * Notify other modules of upload state change
     */
    notifyUploadStateChange() {
        const event = new CustomEvent('uploadStateChanged', {
            detail: this.getUploadState()
        });
        document.dispatchEvent(event);
    }

    /**
     * Get upload status summary
     */
    getUploadSummary() {
        return {
            hasData: this.uploadState.hasData,
            fileCount: (this.uploadState.csvFile ? 1 : 0) + (this.uploadState.shapeFile ? 1 : 0),
            isUploading: this.uploadState.isUploading,
            lastUpload: this.uploadState.lastUploadTime
        };
    }

    /**
     * Check if upload is in progress
     */
    isUploading() {
        return this.uploadState.isUploading;
    }

    /**
     * Reset for new session
     */
    reset() {
        this.resetUploadState();
        console.log('🔄 DataUploadManager reset');
    }
}

// Create and export singleton instance
const dataUploadManager = new DataUploadManager();
export default dataUploadManager; 