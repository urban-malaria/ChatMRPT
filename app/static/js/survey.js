/**
 * Survey JavaScript Handler
 * Manages survey interactions, question navigation, and response submission
 */

class SurveyHandler {
    constructor() {
        this.sessionId = null;
        this.questions = [];
        this.currentQuestionIndex = 0;
        this.responses = {};
        this.startTime = null;
        this.questionStartTime = null;
        this.config = window.surveyConfig || {};

        this.init();
    }

    async init() {
        // Show context information
        this.showContextInfo();

        // Check for saved draft
        this.loadDraft();

        // Set up auto-save
        setInterval(() => this.saveDraft(), 30000); // Auto-save every 30 seconds
    }

    showContextInfo() {
        const contextDiv = document.getElementById('contextInfo');
        if (this.config.context && typeof this.config.context === 'object') {
            const items = [];
            if (this.config.context.trigger_type) {
                items.push(`<li>Activity: ${this.formatTriggerType(this.config.context.trigger_type)}</li>`);
            }
            if (this.config.context.models) {
                items.push(`<li>Models compared: ${this.config.context.models.join(', ')}</li>`);
            }
            // Only show the box when there is something to say.
            if (items.length) {
                contextDiv.innerHTML = `<div class="context-details"><h4>About this survey:</h4><ul>${items.join('')}</ul></div>`;
            }
        }
    }

    formatTriggerType(trigger) {
        const triggerNames = {
            'arena_comparison': 'Arena Mode Comparison',
            'risk_analysis_complete': 'Risk Analysis',
            'itn_distribution_generated': 'ITN Distribution',
            'tpr_analysis_complete': 'TPR Analysis'
        };
        return triggerNames[trigger] || trigger;
    }

    async startSurvey() {
        console.log('[DEBUG] Starting survey...');
        console.log('[DEBUG] Config:', this.config);

        try {
            this.showSection('loadingScreen');

            // Start survey session
            // Generate a session ID if none provided (for testing)
            const sessionId = this.config.chatmrptSession || 'test_' + Date.now();
            const triggerType = this.config.trigger || 'general_assessment';

            console.log('[DEBUG] Session ID:', sessionId);
            console.log('[DEBUG] Trigger Type:', triggerType);
            console.log('[DEBUG] Context:', this.config.context || {});

            const requestBody = {
                chatmrpt_session_id: sessionId,
                trigger_type: triggerType,
                context: this.config.context || {}
            };

            console.log('[DEBUG] Request body:', requestBody);
            console.log('[DEBUG] Sending POST to /survey/api/start');

            const response = await fetch('/survey/api/start', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(requestBody)
            });

            console.log('[DEBUG] Response status:', response.status);
            const data = await response.json();
            console.log('[DEBUG] Response data:', data);

            if (!data.success) {
                console.error('[DEBUG] Survey start failed:', data.error);
                throw new Error(data.error || 'Failed to start survey');
            }

            this.sessionId = data.session_id;

            // Validate and clean questions array
            if (!data.questions || !Array.isArray(data.questions)) {
                console.error('[DEBUG] Invalid questions data received:', data.questions);
                throw new Error('Invalid questions data received from server');
            }

            // Filter out any null/undefined questions and validate structure
            this.questions = data.questions.filter(q => {
                if (!q || typeof q !== 'object') {
                    console.warn('[DEBUG] Filtering out invalid question:', q);
                    return false;
                }
                if (!q.id || !q.type || !q.text) {
                    console.warn('[DEBUG] Question missing required fields:', q);
                    return false;
                }

                // Ensure numeric values are numbers, not strings
                if (q.type === 'scale') {
                    if (q.min !== undefined) q.min = parseInt(q.min, 10);
                    if (q.max !== undefined) q.max = parseInt(q.max, 10);
                }

                return true;
            });

            this.startTime = Date.now();

            console.log('[DEBUG] Session ID set:', this.sessionId);
            console.log('[DEBUG] Questions after validation:', this.questions.length);
            console.log('[DEBUG] First few questions:', this.questions.slice(0, 3));

            // Extra debug - check if first question has all properties
            if (this.questions && this.questions.length > 0) {
                console.log('[DEBUG] First question full details:', JSON.stringify(this.questions[0], null, 2));
                console.log('[DEBUG] First question type:', this.questions[0].type);
                console.log('[DEBUG] First question text:', this.questions[0].text);

                // Check a scale question if we have one
                const scaleQ = this.questions.find(q => q.type === 'scale');
                if (scaleQ) {
                    console.log('[DEBUG] Scale question found:', {
                        id: scaleQ.id,
                        min: scaleQ.min,
                        max: scaleQ.max,
                        typeOfMin: typeof scaleQ.min,
                        typeOfMax: typeof scaleQ.max
                    });
                }
            }

            if (this.questions.length === 0) {
                throw new Error('No valid questions received from server');
            }

            // Show first question
            console.log('[DEBUG] Showing first question...');
            this.showQuestion(0);
            this.showSection('questionContainer');
            document.getElementById('navigation').style.display = 'flex';
            console.log('[DEBUG] Survey UI updated');

        } catch (error) {
            console.error('[DEBUG] Error in startSurvey:', error);
            console.error('[DEBUG] Error stack:', error.stack);
            this.showError(error.message);
        }
    }

    showQuestion(index) {
        console.log('[DEBUG] showQuestion called with index:', index);
        console.log('[DEBUG] Total questions:', this.questions ? this.questions.length : 0);

        if (!this.questions || this.questions.length === 0) {
            console.error('[DEBUG] No questions available to show!');
            return;
        }

        if (index < 0 || index >= this.questions.length) {
            console.error('[DEBUG] Invalid question index:', index);
            return;
        }

        this.currentQuestionIndex = index;
        this.questionStartTime = Date.now();

        const question = this.questions[index];
        console.log('[DEBUG] Current question:', question);

        const container = document.getElementById('questionContent');
        if (!container) {
            console.error('[DEBUG] questionContent element not found!');
            return;
        }

        // Update question number
        const questionNumberEl = document.getElementById('questionNumber');
        if (questionNumberEl) {
            questionNumberEl.textContent = `Question ${index + 1} of ${this.questions.length}`;
            console.log('[DEBUG] Updated question number display');
        } else {
            console.error('[DEBUG] questionNumber element not found!');
        }

        // Update progress bar
        this.updateProgress();

        // Render question based on type
        const questionHTML = this.renderQuestion(question);
        console.log('[DEBUG] Rendered question HTML length:', questionHTML ? questionHTML.length : 0);
        container.innerHTML = questionHTML;

        // Load saved response if exists
        if (this.responses[question.id]) {
            console.log('[DEBUG] Loading saved response for question:', question.id);
            this.loadSavedResponse(question.id, this.responses[question.id]);
        }

        // Update navigation buttons
        this.updateNavigation();

        // Start time tracking
        this.startTimeTracking();
        console.log('[DEBUG] Question display complete');
    }

    renderQuestion(question) {
        let html = '';

        console.log('[DEBUG] renderQuestion called with:', question);

        if (!question) {
            console.error('[DEBUG] No question object provided to renderQuestion!');
            return '<div class="error">No question data available</div>';
        }

        // Validate question structure
        if (!question.id || !question.type) {
            console.error('[DEBUG] Question missing required fields:', question);
            return `
                <div class="error-message" style="color: red; padding: 1rem; border: 1px solid red; border-radius: 4px;">
                    Error: Invalid question structure<br>
                    ID: ${question.id || 'missing'}<br>
                    Type: ${question.type || 'missing'}<br>
                    Text: ${question.text ? question.text.substring(0, 50) + '...' : 'missing'}
                </div>
            `;
        }

        try {
            switch (question.type) {
            case 'text':
                html = `
                    <div class="question-text">${question.text}</div>
                    <input type="text"
                           class="input-text"
                           id="response_${question.id}"
                           placeholder="Type your answer here..."
                           ${question.required ? 'required' : ''}>
                `;
                break;

            case 'textarea':
                html = `
                    <div class="question-text">${question.text}</div>
                    <textarea class="input-textarea"
                              id="response_${question.id}"
                              rows="5"
                              placeholder="Type your answer here..."
                              ${question.required ? 'required' : ''}></textarea>
                `;
                break;

            case 'radio':
                html = `
                    <div class="question-text">${question.text}</div>
                    <div class="radio-options">
                `;
                question.options.forEach((option, i) => {
                    html += `
                        <div class="radio-option" onclick="surveyHandler.selectRadio('${question.id}', ${i})">
                            <input type="radio"
                                   name="response_${question.id}"
                                   id="response_${question.id}_${i}"
                                   value="${option}"
                                   ${question.required ? 'required' : ''}>
                            <label for="response_${question.id}_${i}">${option}</label>
                        </div>
                    `;
                });
                html += '</div>';

                // Add follow-up if exists
                if (question.follow_up) {
                    html += `
                        <div class="follow-up-question" style="margin-top: 1.5rem;">
                            <div class="question-text">${question.follow_up}</div>
                            ${question.follow_up_type === 'textarea' ?
                                `<textarea class="input-textarea"
                                          id="follow_up_${question.id}"
                                          rows="3"
                                          placeholder="Please explain..."></textarea>` :
                                `<input type="text"
                                        class="input-text"
                                        id="follow_up_${question.id}"
                                        placeholder="Please explain...">`
                            }
                        </div>
                    `;
                }
                break;

            case 'scale':
                // Ensure min/max are numbers
                const minVal = parseInt(question.min !== undefined ? question.min : 0, 10);
                const maxVal = parseInt(question.max !== undefined ? question.max : 100, 10);

                // Debug logging
                console.log('[DEBUG] Scale question:', {
                    id: question.id,
                    originalMin: question.min,
                    originalMax: question.max,
                    parsedMin: minVal,
                    parsedMax: maxVal,
                    diff: maxVal - minVal,
                    shouldUseSlider: (maxVal - minVal) > 10,
                    typeOfMin: typeof question.min,
                    typeOfMax: typeof question.max
                });

                // For 0-100 scales, use a slider. For smaller ranges, show individual options
                if (maxVal - minVal > 10) {
                    console.log('[DEBUG] Using SLIDER for this scale question');
                    // Use slider for large ranges (like 0-100)
                    html = `
                        <div class="question-text">${question.text}</div>
                        <div class="scale-container" style="padding: 1.5rem 0;">
                            <div class="scale-slider-container" style="margin: 0 auto; max-width: 600px;">
                                <div class="scale-labels" style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                                    ${question.labels ? question.labels.map((label, i) =>
                                        `<span style="font-size: 0.875rem; color: #6b7280;">${label}</span>`
                                    ).join('') : `
                                        <span style="font-size: 0.875rem; color: #6b7280;">${minVal}</span>
                                        <span style="font-size: 0.875rem; color: #6b7280;">${Math.floor((maxVal + minVal) / 2)}</span>
                                        <span style="font-size: 0.875rem; color: #6b7280;">${maxVal}</span>
                                    `}
                                </div>
                                <input type="range"
                                       id="response_${question.id}"
                                       min="${minVal}"
                                       max="${maxVal}"
                                       value="${Math.floor((maxVal + minVal) / 2)}"
                                       style="width: 100%; height: 8px; border-radius: 4px;"
                                       oninput="surveyHandler.updateScaleValue('${question.id}', this.value)">
                                <div style="text-align: center; margin-top: 1rem;">
                                    <span style="font-size: 1.5rem; font-weight: bold; color: #2563eb;"
                                          id="scale_value_${question.id}">${Math.floor((maxVal + minVal) / 2)}</span>
                                </div>
                            </div>
                        </div>
                    `;
                } else {
                    console.log('[DEBUG] Using INDIVIDUAL BUTTONS for this scale question');
                    // Show individual options for small ranges (like 1-5)
                    html = `
                        <div class="question-text">${question.text}</div>
                        <div class="scale-container">
                            <div class="scale-labels">
                                ${question.labels ? question.labels.map(label =>
                                    `<span>${label}</span>`).join('') : ''}
                            </div>
                            <div class="scale-options">
                    `;
                    for (let i = minVal; i <= maxVal; i++) {
                        html += `
                            <div class="scale-option"
                                 onclick="surveyHandler.selectScale('${question.id}', ${i})"
                                 id="scale_${question.id}_${i}">
                                ${i}
                            </div>
                        `;
                    }
                    html += `
                            </div>
                        </div>
                    `;
                }
                break;

            case 'model_comparison':
                // Validate question text exists
                if (!question.text) {
                    console.error('[DEBUG] Model comparison question missing text:', question);
                    html = `
                        <div class="error-message" style="color: red; padding: 1rem; border: 1px solid red; border-radius: 4px;">
                            Error: Question text is missing. Question ID: ${question.id || 'unknown'}
                        </div>
                    `;
                    break;
                }

                // For model comparison, display the question prominently
                const cleanQuestion = question.text.replace('(Select your preferred model answer)', '').trim();
                html = `
                    <div class="question-text" style="font-size: 1.25rem; font-weight: 600; color: #1f2937; margin-bottom: 1.5rem;">
                        ${question.text}
                    </div>

                    <div class="model-comparison-container">
                        <!-- Show the actual question being asked -->
                        <div style="padding: 1.5rem; background: #fff; border: 2px solid #2563eb; border-radius: 8px; margin-bottom: 1rem;">
                            <h3 style="margin: 0 0 0.5rem 0; color: #1e40af; font-size: 1.1rem;">
                                Core Question:
                            </h3>
                            <p style="font-size: 1.1rem; margin: 0; color: #374151;">
                                <strong>${cleanQuestion}</strong>
                            </p>
                        </div>

                        <!-- Arena mode reference -->
                        <div style="padding: 1rem; background: #f3f4f6; border-radius: 8px; margin-bottom: 1rem;">
                            <p style="margin: 0; color: #4b5563; font-size: 0.95rem;">
                                <em>You just compared different model responses to this question in Arena mode.</em>
                            </p>
                        </div>

                        <!-- Response area -->
                        <div style="padding: 1.5rem; background: #fafafa; border: 1px solid #e5e7eb; border-radius: 8px;">
                            <label style="display: block; margin-bottom: 0.75rem; font-weight: 500; color: #111827;">
                                Based on the Arena comparison, which model's answer did you prefer and why?
                            </label>
                            <textarea class="input-textarea"
                                      id="response_${question.id}"
                                      rows="5"
                                      placeholder="Describe which model answer you found most accurate, clear, and helpful. What made it better than the others?"
                                      style="width: 100%; font-size: 1rem;"
                                      ${question.required ? 'required' : ''}></textarea>
                        </div>

                        ${question.instruction ? `
                            <div style="margin-top: 1rem; padding: 0.75rem; background: #fef3c7; border-left: 4px solid #f59e0b;">
                                <small style="color: #92400e;">${question.instruction}</small>
                            </div>
                        ` : ''}
                    </div>
                `;
                break;

            case 'rating_matrix':
                html = `
                    <div class="question-text">${question.text}</div>
                    <div class="matrix-container">
                        <table class="rating-matrix">
                            <thead>
                                <tr>
                                    <th></th>
                `;
                // Add column headers (rating values)
                const firstRow = Object.values(question.options)[0];
                firstRow.forEach(value => {
                    html += `<th>${value}</th>`;
                });
                html += `
                                </tr>
                            </thead>
                            <tbody>
                `;
                // Add rows
                Object.entries(question.options).forEach(([rowLabel, values]) => {
                    html += `<tr><th>${rowLabel}</th>`;
                    values.forEach(value => {
                        html += `
                            <td>
                                <input type="radio"
                                       name="matrix_${question.id}_${rowLabel}"
                                       value="${value}"
                                       onclick="surveyHandler.selectMatrix('${question.id}', '${rowLabel}', ${value})">
                            </td>
                        `;
                    });
                    html += '</tr>';
                });
                html += `
                            </tbody>
                        </table>
                    </div>
                `;
                if (question.labels) {
                    html += `<div class="scale-labels" style="margin-top: 1rem;">`;
                    question.labels.forEach((label, i) => {
                        html += `<span>${i + 1}: ${label}</span> `;
                    });
                    html += `</div>`;
                }
                break;
            }
        } catch (error) {
            console.error('[DEBUG] Error rendering question:', error, question);
            return `
                <div class="error-message" style="color: red; padding: 1rem; border: 1px solid red; border-radius: 4px;">
                    Error rendering question: ${error.message}<br>
                    Question ID: ${question.id}<br>
                    Question Type: ${question.type}
                </div>
            `;
        }

        return html;
    }

    selectRadio(questionId, optionIndex) {
        const radio = document.getElementById(`response_${questionId}_${optionIndex}`);
        if (radio) {
            radio.checked = true;
            // Update visual selection
            document.querySelectorAll(`.radio-option`).forEach(opt => {
                opt.classList.remove('selected');
            });
            radio.closest('.radio-option').classList.add('selected');
        }
    }

    selectScale(questionId, value) {
        // Clear previous selection
        document.querySelectorAll(`.scale-option`).forEach(opt => {
            opt.classList.remove('selected');
        });
        // Add selection
        const selected = document.getElementById(`scale_${questionId}_${value}`);
        if (selected) {
            selected.classList.add('selected');
            this.responses[questionId] = value;
        }
    }

    updateScaleValue(questionId, value) {
        // Update the displayed value for slider scales
        const valueDisplay = document.getElementById(`scale_value_${questionId}`);
        if (valueDisplay) {
            valueDisplay.textContent = value;
        }
        // Save the response
        this.responses[questionId] = parseInt(value);
    }

    selectMatrix(questionId, rowLabel, value) {
        if (!this.responses[questionId]) {
            this.responses[questionId] = {};
        }
        this.responses[questionId][rowLabel] = value;
    }

    async nextQuestion() {
        // Save current response
        if (!await this.saveCurrentResponse()) {
            return;
        }

        if (this.currentQuestionIndex < this.questions.length - 1) {
            this.showQuestion(this.currentQuestionIndex + 1);
        }
    }

    previousQuestion() {
        if (this.currentQuestionIndex > 0) {
            this.showQuestion(this.currentQuestionIndex - 1);
        }
    }

    async saveCurrentResponse() {
        const question = this.questions[this.currentQuestionIndex];
        let response = this.getQuestionResponse(question);

        // Validate required questions
        if (question.required && !response) {
            alert('Please answer this question before proceeding.');
            return false;
        }

        // Calculate time spent
        const timeSpent = Math.floor((Date.now() - this.questionStartTime) / 1000);

        // Save response
        this.responses[question.id] = response;

        // Submit to server
        try {
            await fetch('/survey/api/submit', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    session_id: this.sessionId,
                    question_id: question.id,
                    response: response,
                    time_spent: timeSpent,
                    metadata: {
                        question_index: this.currentQuestionIndex,
                        total_questions: this.questions.length
                    }
                })
            });

            return true;
        } catch (error) {
            console.error('Failed to save response:', error);
            // Continue anyway - responses are saved locally
            return true;
        }
    }

    getQuestionResponse(question) {
        switch (question.type) {
            case 'model_comparison':
                // Model comparison uses a textarea for the response
                const modelInput = document.getElementById(`response_${question.id}`);
                return modelInput ? modelInput.value.trim() : '';

            case 'text':
            case 'textarea':
                const input = document.getElementById(`response_${question.id}`);
                const response = input ? input.value.trim() : '';

                // Include follow-up if exists
                if (question.follow_up) {
                    const followUp = document.getElementById(`follow_up_${question.id}`);
                    if (followUp && followUp.value.trim()) {
                        return {
                            main: response,
                            follow_up: followUp.value.trim()
                        };
                    }
                }
                return response;

            case 'radio':
                const selected = document.querySelector(`input[name="response_${question.id}"]:checked`);
                const value = selected ? selected.value : '';

                // Include follow-up if exists
                if (question.follow_up) {
                    const followUp = document.getElementById(`follow_up_${question.id}`);
                    if (followUp && followUp.value.trim()) {
                        return {
                            main: value,
                            follow_up: followUp.value.trim()
                        };
                    }
                }
                return value;

            case 'scale':
                // Check if we're using a slider (for large ranges)
                const slider = document.getElementById(`response_${question.id}`);
                if (slider && slider.type === 'range') {
                    return parseInt(slider.value);
                }
                // Otherwise return from saved responses (for clickable scales)
                return this.responses[question.id] || '';

            case 'rating_matrix':
                return this.responses[question.id] || {};
        }
    }

    loadSavedResponse(questionId, response) {
        const question = this.questions.find(q => q.id === questionId);
        if (!question) return;

        switch (question.type) {
            case 'model_comparison':
                // Model comparison uses a textarea
                const modelInput = document.getElementById(`response_${questionId}`);
                if (modelInput) {
                    modelInput.value = response || '';
                }
                break;

            case 'text':
            case 'textarea':
                const input = document.getElementById(`response_${questionId}`);
                if (input) {
                    if (typeof response === 'object' && response.main) {
                        input.value = response.main;
                        const followUp = document.getElementById(`follow_up_${questionId}`);
                        if (followUp && response.follow_up) {
                            followUp.value = response.follow_up;
                        }
                    } else {
                        input.value = response;
                    }
                }
                break;

            case 'radio':
                if (typeof response === 'object' && response.main) {
                    const radio = document.querySelector(`input[name="response_${questionId}"][value="${response.main}"]`);
                    if (radio) {
                        radio.checked = true;
                        radio.closest('.radio-option').classList.add('selected');
                    }
                    const followUp = document.getElementById(`follow_up_${questionId}`);
                    if (followUp && response.follow_up) {
                        followUp.value = response.follow_up;
                    }
                } else {
                    const radio = document.querySelector(`input[name="response_${questionId}"][value="${response}"]`);
                    if (radio) {
                        radio.checked = true;
                        radio.closest('.radio-option').classList.add('selected');
                    }
                }
                break;

            case 'scale':
                // Check if using a slider
                const slider = document.getElementById(`response_${questionId}`);
                if (slider && slider.type === 'range') {
                    slider.value = response;
                    this.updateScaleValue(questionId, response);
                } else {
                    this.selectScale(questionId, response);
                }
                break;

            case 'rating_matrix':
                if (typeof response === 'object') {
                    Object.entries(response).forEach(([rowLabel, value]) => {
                        const radio = document.querySelector(`input[name="matrix_${questionId}_${rowLabel}"][value="${value}"]`);
                        if (radio) radio.checked = true;
                    });
                }
                break;
        }
    }

    updateProgress() {
        const progress = ((this.currentQuestionIndex + 1) / this.questions.length) * 100;
        document.documentElement.style.setProperty('--progress', `${progress}%`);
        document.getElementById('progressText').textContent =
            `${this.currentQuestionIndex + 1} / ${this.questions.length}`;
    }

    updateNavigation() {
        const prevBtn = document.getElementById('btnPrevious');
        const nextBtn = document.getElementById('btnNext');
        const submitBtn = document.getElementById('btnSubmit');

        // Previous button
        prevBtn.style.display = this.currentQuestionIndex > 0 ? 'block' : 'none';

        // Next/Submit buttons
        if (this.currentQuestionIndex === this.questions.length - 1) {
            nextBtn.style.display = 'none';
            submitBtn.style.display = 'block';
        } else {
            nextBtn.style.display = 'block';
            submitBtn.style.display = 'none';
        }
    }

    startTimeTracking() {
        // Update time display every second
        if (this.timeInterval) clearInterval(this.timeInterval);

        this.timeInterval = setInterval(() => {
            const elapsed = Math.floor((Date.now() - this.questionStartTime) / 1000);
            const minutes = Math.floor(elapsed / 60);
            const seconds = elapsed % 60;
            document.getElementById('timeSpent').textContent =
                `Time on question: ${minutes}:${seconds.toString().padStart(2, '0')}`;
        }, 1000);
    }

    async submitSurvey() {
        // Save current response
        if (!await this.saveCurrentResponse()) {
            return;
        }

        try {
            // Mark survey as complete
            await fetch(`/survey/api/complete/${this.sessionId}`, {
                method: 'POST'
            });

            // Show completion screen
            this.showCompletion();

        } catch (error) {
            this.showError('Failed to complete survey. Your responses have been saved.');
        }
    }

    showCompletion() {
        const totalTime = Math.floor((Date.now() - this.startTime) / 1000);
        const minutes = Math.floor(totalTime / 60);
        const seconds = totalTime % 60;

        const stats = `
            <p>Questions answered: ${Object.keys(this.responses).length} / ${this.questions.length}</p>
            <p>Time taken: ${minutes} minutes ${seconds} seconds</p>
            <p>Session ID: ${this.sessionId}</p>
        `;

        document.getElementById('completionStats').innerHTML = stats;
        this.showSection('completionScreen');

        // Clear draft
        this.clearDraft();
    }

    saveDraft() {
        if (!this.sessionId) return;

        const draft = {
            sessionId: this.sessionId,
            responses: this.responses,
            currentQuestionIndex: this.currentQuestionIndex,
            timestamp: Date.now()
        };

        localStorage.setItem(`survey_draft_${this.config.chatmrptSession}`, JSON.stringify(draft));
    }

    loadDraft() {
        const draftKey = `survey_draft_${this.config.chatmrptSession}`;
        const draft = localStorage.getItem(draftKey);

        if (draft) {
            try {
                const data = JSON.parse(draft);
                // Check if draft is less than 24 hours old
                if (Date.now() - data.timestamp < 24 * 60 * 60 * 1000) {
                    this.responses = data.responses || {};
                    // Optionally restore question index
                    // this.currentQuestionIndex = data.currentQuestionIndex || 0;
                }
            } catch (error) {
                console.error('Failed to load draft:', error);
            }
        }
    }

    clearDraft() {
        localStorage.removeItem(`survey_draft_${this.config.chatmrptSession}`);
    }

    async downloadResponses() {
        try {
            const response = await fetch(`/survey/api/export/${this.sessionId}`);
            const data = await response.json();

            // Create download
            const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `survey_responses_${this.sessionId}.json`;
            a.click();
            URL.revokeObjectURL(url);

        } catch (error) {
            alert('Failed to download responses');
        }
    }

    showSection(sectionId) {
        console.log('[DEBUG] showSection called with:', sectionId);
        const sections = document.querySelectorAll('.survey-section');
        console.log('[DEBUG] Found survey sections:', sections.length);

        sections.forEach(section => {
            console.log('[DEBUG] Hiding section:', section.id);
            section.classList.remove('active');
        });

        const targetSection = document.getElementById(sectionId);
        if (targetSection) {
            console.log('[DEBUG] Showing section:', sectionId);
            targetSection.classList.add('active');
        } else {
            console.error('[DEBUG] Section not found:', sectionId);
        }
    }

    showError(message) {
        console.error('[DEBUG] Showing error:', message);
        const errorEl = document.getElementById('errorMessage');
        if (errorEl) {
            errorEl.textContent = message;
        } else {
            console.error('[DEBUG] errorMessage element not found!');
        }
        this.showSection('errorScreen');
    }
}

// Initialize handler
let surveyHandler;
document.addEventListener('DOMContentLoaded', () => {
    console.log('[DEBUG] DOMContentLoaded - initializing survey handler');
    surveyHandler = new SurveyHandler();
    console.log('[DEBUG] Survey handler initialized:', surveyHandler);
});

// Global functions for HTML onclick handlers
function startSurvey() {
    console.log('[DEBUG] startSurvey() called from HTML button');
    if (!surveyHandler) {
        console.error('[DEBUG] surveyHandler not initialized!');
        return;
    }
    surveyHandler.startSurvey();
}

function nextQuestion() {
    surveyHandler.nextQuestion();
}

function previousQuestion() {
    surveyHandler.previousQuestion();
}

function saveDraft() {
    surveyHandler.saveDraft();
    alert('Draft saved successfully!');
}

function submitSurvey() {
    if (confirm('Are you sure you want to submit your survey responses?')) {
        surveyHandler.submitSurvey();
    }
}

function downloadResponses() {
    surveyHandler.downloadResponses();
}

function showPrivacy() {
    alert('Your survey responses are confidential and will only be used to improve ChatMRPT. No personally identifiable information is collected.');
}