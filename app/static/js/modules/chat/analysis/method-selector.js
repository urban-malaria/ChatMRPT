/**
 * Method Selector Module
 * Handles dual method selection and analysis method switching
 */

export class MethodSelector {
    constructor(messageHandler) {
        this.messageHandler = messageHandler;
        this.setupGlobalMethods();
    }

    setupGlobalMethods() {
        // Make selectAnalysisMethod available globally for dual method cards
        window.selectAnalysisMethod = this.selectAnalysisMethod.bind(this);
        window.switchMethod = this.switchMethod.bind(this);
    }

    /**
     * Select analysis method from dual method cards
     * @param {string} method - The method to select ('composite' or 'pca')
     */
    selectAnalysisMethod(method) {
        console.log(`🎯 User selected method: ${method}`);
        
        // Generate the message that will trigger the detailed results handler
        const message = `I want to view the detailed results for the ${method === 'composite' ? 'Composite Score' : 'PCA'} method`;
        
        console.log(`📤 Sending method selection message: ${message}`);
        
        // Send the message through the existing chat system
        this.messageHandler.sendMessage(message);
    }

    /**
     * Switch between analysis methods (MEAN/PCA)
     * @param {string} method - The method to switch to ('mean' or 'pca')
     */
    async switchMethod(method) {
        try {
            // Add visual feedback
            const buttons = document.querySelectorAll('.method-btn');
            buttons.forEach(btn => btn.classList.remove('active'));
            
            // Find and activate the clicked button
            const targetBtn = Array.from(buttons).find(btn => 
                btn.textContent.toLowerCase().includes(method.toLowerCase())
            );
            if (targetBtn) {
                targetBtn.classList.add('active');
            }
            
            // Send switch command to backend
            const switchMessage = `Switch to ${method} view`;
            
            // Use the message handler to send the message
            await this.messageHandler.sendMessage(switchMessage);
            
        } catch (error) {
            console.error('Error switching method:', error);
            this.messageHandler.addSystemMessage('Error switching method. Please try again.');
        }
    }
} 