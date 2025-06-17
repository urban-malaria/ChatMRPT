/**
 * API Client Module
 * Handles all backend communication and HTTP requests
 */

class APIClient {
    constructor() {
        this.baseURL = '';
        this.defaultHeaders = {
            'Content-Type': 'application/json',
        };
    }

    /**
     * Send message to backend endpoint
     * @param {string} message - The message to send
     * @param {string} language - Language code
     * @returns {Promise<Object>} Response data
     */
    async sendMessage(message, language = 'en') {
        try {
            const response = await fetch('/send_message', {
                method: 'POST',
                headers: this.defaultHeaders,
                body: JSON.stringify({
                    message: message,
                    language: language
                })
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            return await response.json();
        } catch (error) {
            console.error('Error sending message:', error);
            throw error;
        }
    }

    /**
     * Upload files to backend
     * @param {File|null} csvFile - CSV/Excel file
     * @param {File|null} shapeFile - Shapefile ZIP
     * @returns {Promise<Object>} Upload response
     */
    async uploadFiles(csvFile, shapeFile) {
        const formData = new FormData();
        
        if (csvFile) {
            formData.append('csv_file', csvFile);
        }
        if (shapeFile) {
            formData.append('shapefile', shapeFile);
        }

        try {
            const response = await fetch('/upload_both_files', {
                method: 'POST',
                body: formData
            });

            if (!response.ok) {
                throw new Error(`Upload failed with status: ${response.status}`);
            }

            return await response.json();
        } catch (error) {
            console.error('Error uploading files:', error);
            throw error;
        }
    }

    /**
     * Load sample data
     * @returns {Promise<Object>} Sample data response
     */
    async loadSampleData() {
        try {
            const response = await fetch('/load_sample_data', {
                method: 'POST',
                headers: this.defaultHeaders,
                body: JSON.stringify({})
            });

            if (!response.ok) {
                throw new Error(`Failed to load sample data: ${response.status}`);
            }

            return await response.json();
        } catch (error) {
            console.error('Error loading sample data:', error);
            throw error;
        }
    }

    /**
     * Run analysis with selected variables
     * @param {Array<string>|null} variables - Custom variables array
     * @returns {Promise<Object>} Analysis response
     */
    async runAnalysis(variables = null) {
        try {
            const response = await fetch('/run_analysis', {
                method: 'POST',
                headers: this.defaultHeaders,
                body: JSON.stringify({
                    variables: variables
                })
            });

            if (!response.ok) {
                throw new Error(`Analysis failed with status: ${response.status}`);
            }

            return await response.json();
        } catch (error) {
            console.error('Error running analysis:', error);
            throw error;
        }
    }

    /**
     * Generate report in specified format
     * @param {string} format - Report format (pdf, html, markdown)
     * @returns {Promise<void>} Downloads the report
     */
    async generateReport(format) {
        try {
            const response = await fetch('/generate_report', {
                method: 'POST',
                headers: this.defaultHeaders,
                body: JSON.stringify({
                    format: format
                })
            });

            if (!response.ok) {
                throw new Error(`Report generation failed: ${response.status}`);
            }

            // Handle file download
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.style.display = 'none';
            a.href = url;
            a.download = `malaria_risk_report.${format}`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
        } catch (error) {
            console.error('Error generating report:', error);
            throw error;
        }
    }

    /**
     * Request visualization with retry logic
     * @param {Object} vizInfo - Visualization information
     * @param {number} attempt - Current attempt number
     * @param {number} maxAttempts - Maximum attempts
     * @returns {Promise<Object>} Visualization response
     */
    async requestVisualization(vizInfo, attempt = 1, maxAttempts = 2) {
        try {
            const response = await fetch('/generate_visualization', {
                method: 'POST',
                headers: this.defaultHeaders,
                body: JSON.stringify(vizInfo)
            });

            if (!response.ok) {
                if (attempt < maxAttempts) {
                    console.warn(`Visualization attempt ${attempt} failed, retrying...`);
                    return this.requestVisualization(vizInfo, attempt + 1, maxAttempts);
                }
                throw new Error(`Visualization failed after ${maxAttempts} attempts`);
            }

            return await response.json();
        } catch (error) {
            console.error('Error requesting visualization:', error);
            throw error;
        }
    }

    /**
     * Fetch available variables for autocomplete
     * @returns {Promise<Array<string>>} Available variables
     */
    async fetchAvailableVariables() {
        try {
            const response = await fetch('/get_available_variables', {
                method: 'GET',
                headers: this.defaultHeaders
            });

            if (!response.ok) {
                throw new Error(`Failed to fetch variables: ${response.status}`);
            }

            const data = await response.json();
            return data.variables || [];
        } catch (error) {
            console.error('Error fetching available variables:', error);
            return [];
        }
    }

    /**
     * Change language setting
     * @param {string} language - New language code
     * @returns {Promise<Object>} Language change response
     */
    async changeLanguage(language) {
        try {
            const response = await fetch('/change_language', {
                method: 'POST',
                headers: this.defaultHeaders,
                body: JSON.stringify({
                    language: language
                })
            });

            if (!response.ok) {
                throw new Error(`Language change failed: ${response.status}`);
            }

            return await response.json();
        } catch (error) {
            console.error('Error changing language:', error);
            throw error;
        }
    }
}

// Create and export a singleton instance
const apiClient = new APIClient();

export default apiClient; 