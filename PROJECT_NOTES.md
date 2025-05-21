# ChatMRPT: Comprehensive System Architecture & Development Documentation
________________________________________
**Version:** 2.0 (Enhanced Draft)
**Date:** May 14, 2024
________________________________________

## 1. Introduction & Vision

### The ChatMRPT Solution
ChatMRPT (Malaria Reprioritization Tool with Conversational AI) is an advanced analytical platform designed to assist public health officials, researchers, and field workers in making data-driven decisions for malaria control and elimination programs. It combines robust data processing capabilities with an intuitive conversational interface, guided by Large Language Models (LLMs), to simplify complex geospatial and epidemiological analysis.

**Vision:** To democratize malaria risk analysis by providing an accessible, intelligent, and explanatory tool that empowers users of all technical backgrounds to identify high-risk areas, understand the underlying factors, and optimize resource allocation for interventions. ChatMRPT aims to bridge the gap between raw data and actionable insights, fostering a more proactive and effective approach to malaria surveillance and response.

________________________________________

## 2. System Architecture Overview

ChatMRPT is designed with a modular, microservices-inspired (though monolithically deployed for simplicity in this version) layered architecture. Each layer and component performs a specific function, promoting maintainability, scalability, and adaptability. The system is architected to support both online interactions and potentially offline (batch processing) analytical workflows in future iterations.

### 2.1. Architectural Layers & Flow

The system can be visualized as a series of interacting layers:

1.  **Presentation Layer (Frontend):** User interaction point.
2.  **Application Layer (Backend Server):** Orchestrates requests, manages state, and handles business logic.
3.  **Data Processing & Analytics Layer:** Core engine for data manipulation, statistical analysis, and geospatial operations.
4.  **Knowledge & AI Layer:** Provides contextual understanding, explanations, and intelligent assistance.
5.  **Service & Integration Layer:** Connects to external APIs and libraries.
6.  **Persistence Layer:** Handles data storage (session data, uploaded files, generated reports, logs).

**Typical User Interaction Flow:**

```
User Input (Text/File Upload)
|
V
Frontend Interface (HTML/CSS/JS via Flask Templates)
|
V
Flask Backend Server (routes.py)
|--- Session Management (Flask Session)
|--- Interaction Logging (InteractionLogger)
|
V
Natural Language Understanding (NLU)
|--- LLM-based Intent/Entity Extraction (get_llm_nlu_response)
|--- Rule-based Fallback (parse_message_intent_fallback)
|
V
Action Dispatcher (based on intent)
|
|--- IF Data Operation (Upload, Analysis):
| V
| DataHandler (data_handler.py)
| |--- Load (CSV, Shapefile)
| |--- Clean (Imputation: Spatial, Mean, Mode)
| |--- Normalize (Direct/Inverse Relationships)
| |--- LLM for Variable Suggestion (Composite Scores)
| |--- Composite Score Calculation
| |--- Vulnerability Ranking
| |--- Urban Extent Analysis
| V
| Persistence (Instance Folder: Processed CSVs, SHPs)
|
|--- IF Visualization Request:
| V
| Visualization Engine (visualization.py)
| |--- Access DataHandler
| |--- Plotly for Map/Chart Generation
| V
| Persistence (Instance Folder: HTML/Image Visualizations)
|
|--- IF Report Request:
| V
| Report Generator (report_generator.py)
| |--- Access DataHandler
| |--- LLM for Narrative Generation
| V
| Persistence (Instance Folder: HTML/PDF/DOCX Reports)
|
|--- IF Knowledge/Explanation Request:
| V
| Knowledge Base (kb.py)
| |--- Static Content Retrieval
| |--- LLM for Dynamic Explanation Synthesis (generate_ai_response)
|
V
Response Assembly (Text, Visualization URL, Report URL)
|--- LLM for contextual AI responses (generate_ai_response)
|
V
Frontend Interface (Display to User)
```

### 2.2. Core System Components (Re-iterated with Architectural Context)

*   **Conversational User Interface (Frontend):** Built with Flask templating (HTML, CSS, JavaScript).
*   **Logic-Driven Backend Server (Application Layer):** Flask application handling HTTP requests, session management, and core logic.
*   **Data Processing Engine (Data Processing & Analytics Layer):** Primarily the `DataHandler` class, utilizing Pandas and GeoPandas.
*   **Knowledge Architecture (Knowledge & AI Layer):** `kb.py` for static knowledge and LLM integration for dynamic explanations and NLU.
*   **Visualization Tools (Data Processing & Analytics Layer / Frontend):** `visualization.py` using Plotly, served to the frontend.
*   **Automated Report Generation (Application Layer / Knowledge & AI Layer):** `report_generator.py` using LLM for narratives.
*   **External Service Integration (Service & Integration Layer):** OpenAI API client, geospatial libraries (GeoPandas, PySAL, Shapely).
*   **Persistence Layer:** File system (`instance/` folder for uploads, reports, session-specific intermediate files, SQLite DB for interaction logs).

________________________________________

## 3. Core System Components (Detailed)

### 3.1. Frontend Interface (`index.html`, `main.js`, `styles.css`)

The user interface is designed for clarity, responsiveness, and ease of use, even on low-performance devices or in areas with challenging internet connectivity. It's a single-page application (SPA) style interface rendered by Flask, with dynamic updates handled by JavaScript.

**Key Features:**

*   **Chat Interface:** The primary mode of interaction. Users type natural language queries or commands. Messages are displayed in a familiar chat bubble format.
*   **File Upload Support:**
    *   Modal-based UI for uploading CSV/Excel data and Shapefile (ZIP) archives.
    *   Client-side validation for file types.
    *   Backend processing handles extraction and initial validation.
    *   **Code Snippet Context (main.js - `uploadBothFiles`):** JavaScript `fetch` API is used to send files to dedicated Flask endpoints (`/upload_csv`, `/upload_shapefile`). This allows for separate processing and feedback for each file type before potentially running a combined check.
*   **Interactive Visualizations:**
    *   Generated maps and plots (Plotly HTML) are embedded in iframes within the chat interface or a larger modal.
    *   Features include zoom, pan, hover tooltips, and pagination for multi-page visualizations (e.g., composite maps).
    *   **Code Snippet Context (visualization.py - `create_plotly_html`):** Plotly figures are saved as standalone HTML files in the session-specific `instance/uploads/<session_id>/` directory. A URL like `/serve_viz_file/<session_id>/<filename>` is returned to the frontend, which is then used as the `src` for an iframe. This keeps the main chat interface light.
*   **Accessibility:** Standard HTML elements are used, and consideration is given to keyboard navigation and ARIA attributes where applicable (though more formal accessibility auditing would be a future step). High-contrast elements can be achieved via browser settings or future theme options.
*   **Session Management:**
    *   Flask's server-side sessions are used to store `session_id`, file loading status, analysis completion status, language preference, and limited dialogue context.
    *   The `session_id` is key to associating uploaded files and generated artifacts in the `instance` folder with the user's current interaction.
    *   **Code Snippet Context (routes.py - `index()`):**
        ```python
        if 'session_id' not in session:
            session['session_id'] = str(uuid.uuid4())
            session['conversation_history'] = []
            # ... other initial session variables
        ```
*   **Real-time Feedback:** Loading indicators, typing animations, and status messages keep the user informed about system activity.

### 3.2. Conversational Application Server (Flask Backend - `routes.py`, `__init__.py`)

The backend server, built with Flask, acts as the central nervous system. It manages HTTP requests, user sessions, task orchestration, and communication between different components.

**Key Functionalities:**

*   **Request Handling:** Defines routes for UI rendering, file uploads, message sending, analysis execution, visualization retrieval, and report downloads.
*   **Session Management:** Leverages Flask's session interface to maintain user-specific state across requests (e.g., `session['csv_loaded']`, `session['analysis_complete']`).
*   **Natural Language Understanding (NLU):**
    *   **Primary NLU Engine (LLM-based):** The `get_llm_nlu_response` function in `routes.py` is the core of advanced intent recognition and entity extraction.
        *   **LLM Integration Detail:**
            *   **Prompt Engineering:** A detailed system prompt is constructed, providing the LLM (e.g., GPT-4o) with context about its role, possible intents, extractable entities, current application state (files loaded, analysis run), available variables, and even recent visualization context. This guides the LLM to produce structured JSON output.
            *   **Example System Prompt Snippet from `get_llm_nlu_response`:**
                ```
                You are an expert NLU system for a Malaria Risk Analysis tool.
                Analyze the user message considering the current application state and identify the primary intent and any relevant entities.
                Current state: Analysis previously run = {session_state.get('analysis_complete', False)}.
                Possible intents:
                - run_standard_analysis: User wants to run the default analysis.
                - run_custom_analysis: User wants to run analysis with specific variables.
                - request_visualization: User asks to see a map, plot, or chart.
                Extractable entities:
                - variable_names (list): List of specific variable names mentioned...
                - visualization_type (string): General type like 'map', 'plot'...
                Respond ONLY with a JSON object containing the 'intent' (string) and 'entities' (object).
                ```
            *   **JSON Mode:** The OpenAI API is called with `response_format={ "type": "json_object" }` to enforce structured output, making parsing more reliable.
            *   **Entity Validation:** Extracted entities (especially `variable_names`) are cross-validated against the dataset's actual available variables using functions like `match_variables_to_dataset`.
    *   **Fallback NLU (Rule-based):** If the LLM NLU fails (e.g., API key missing, network error), `parse_message_intent_fallback` in `routes.py` uses regular expressions and keyword matching to determine a more basic intent. This ensures a degree of functionality even without the LLM.
*   **State & Context Management for Dialogue:**
    *   **LLM Integration Detail:** The Flask `session` stores:
        *   `pending_action` and `pending_variables`: For multi-turn interactions like confirming a custom analysis before execution.
        *   `last_visualization`: To provide context to the LLM for follow-up questions about a recently shown visual.
        *   `dialogue_context` (e.g., `last_intent`, `last_topic`): Helps the LLM maintain conversational flow and understand ambiguous follow-ups. This context is fed into the system prompt for NLU and general response generation.
*   **Task Orchestration:** Based on the recognized intent, the server calls appropriate functions in `DataHandler`, `visualization.py`, `report_generator.py`, or `kb.py`.
*   **Response Generation (LLM-Assisted):**
    *   **LLM Integration Detail:** The `generate_ai_response` function in `routes.py` is used for general conversational replies, explanations, and elaborations.
        *   **System Persona & Context:** It constructs a system prompt that defines the AI's persona (helpful Malaria Risk Analysis assistant) and includes the current session state (files loaded, analysis status, last variables used, etc.) and potentially relevant content from the knowledge base (`kb.py`).
        *   **Conversation History:** A limited history of user/assistant messages is included in the prompt to provide conversational context to the LLM.
        *   **Example System Prompt Snippet from `get_system_message`:**
            ```
            You are an AI assistant for the Malaria Reprioritization Tool (MRPT).
            Your role is to help users analyze malaria risk factors...
            Current Session Status:
            - Data Files: Both CSV/Excel and Shapefile are LOADED.
            - Analysis: COMPLETE.
              - Variables used in last analysis: rainfall, temperature
            ```
*   **Interaction Logging (`InteractionLogger` class):**
    *   A dedicated SQLite database (`instance/interactions.db`) stores comprehensive logs of user sessions, messages, file uploads, analysis events, and errors.
    *   This is crucial for debugging, understanding user behavior, auditing, and future model fine-tuning.
    *   The `get_interaction_logger()` helper in `routes.py` retrieves the logger instance.
    *   Admin routes (`/admin/logs`, `/admin/session/<id>`) provide a web interface to view these logs.

### 3.3. Data Processing Core (`data_handler.py`)

The `DataHandler` class is the analytical engine of ChatMRPT, responsible for all data ingestion, cleaning, transformation, statistical operations, and geospatial computations. It maintains the state of the data throughout the analysis pipeline for a given session.

**Key Capabilities & LLM Integration:**

*   **Supported File Formats:**
    *   CSV and Excel (via Pandas `read_csv`, `read_excel`).
    *   Shapefiles (ZIP archives containing `.shp`, `.dbf`, `.shx`, etc., via GeoPandas `read_file`).
*   **Data Loading & Initial Validation:**
    *   `load_csv()`: Robustly loads tabular data, handles common NA values, strips column names, and attempts to identify/rename a `WardName` column.
    *   `load_shapefile()`: Extracts shapefiles from ZIPs, loads via GeoPandas, identifies/creates `WardName`, ensures `UrbanPercent` column (creating random if missing for demo), and standardizes CRS to EPSG:4326.
*   **Data Cleaning (`clean_data`):**
    *   Handles missing values. The primary method is **spatial neighbor mean imputation** (`_handle_na_spatial_mean`) for numeric columns, using `libpysal.weights.Queen` to find neighbors from the shapefile.
    *   If spatial imputation fails or is not applicable (e.g., non-numeric data, no shapefile), it falls back to column-wise mean imputation (`_handle_na_mean`) for numeric data or mode imputation (`_handle_na_mode`) for categorical data.
    *   Records the methods used for imputation (`self.na_handling_methods`) for transparency.
*   **Variable Relationship Determination (`determine_variable_relationships`):**
    *   Assigns 'direct' or 'inverse' relationships to variables concerning malaria risk. Currently, this uses a heuristic approach based on keywords in variable names (e.g., 'distance' implies inverse).
    *   **Potential Future LLM Integration:** An LLM could be prompted with variable names and descriptions to infer these relationships based on epidemiological knowledge, making this step more robust and adaptable to novel datasets.
*   **Data Normalization (`normalize_data`):**
    *   Scales all numeric variables to a 0-1 range.
    *   For 'direct' relationship variables: `(value - min) / (max - min)`.
    *   For 'inverse' relationship variables: Values are first inverted (`1 / (value + epsilon)`), then normalized using the same formula. This ensures that a normalized value of 1 always indicates higher malaria risk contribution.
*   **Composite Variable Selection & Score Calculation (`compute_composite_scores`):**
    *   **LLM Integration Detail (Variable Suggestion):** If no specific variables are provided by the user for custom analysis, the `suggest_composite_variables` method is called.
        *   This method constructs a prompt for an LLM (e.g., GPT-4o), providing the list of available normalized variables and their determined relationships (direct/inverse).
        *   The LLM is asked to act as a malaria epidemiology expert and select 3-5 variables that would create an informative and balanced risk assessment, avoiding redundancy and 'urban' variables (which are handled by urban extent analysis).
        *   **Example Prompt Snippet for LLM in `suggest_composite_variables`:**
            ```
            You are a malaria epidemiology expert... select 3-5 variables...
            Variables:
            rainfall (direct)
            temperature (direct)
            distance_to_water (inverse)
            housing_quality (inverse)
            ...
            Return ONLY a comma-separated list of the variable names...
            ```
        *   A fallback heuristic (`_fallback_variable_selection`) is used if the LLM fails or returns insufficient valid variables.
    *   **Score Calculation:** Once variables are selected (by LLM or user), all possible combinations of these variables (pairs, triplets, etc., up to the full set) are used to create different risk models. For each model, the composite score for a ward is typically the mean of the normalized values of the variables in that model.
    *   Stores `self.composite_scores` (DataFrame of scores) and `self.model_formulas`.
*   **Vulnerability Ranking (`calculate_vulnerability_rankings`):**
    *   Calculates the median composite score for each ward across all generated models.
    *   Ranks wards based on this median score (higher score = higher vulnerability).
    *   Assigns categories (High, Medium, Low) based on rank terciles.
*   **Urban Extent Analysis (`process_urban_extent`):**
    *   Identifies an 'Urban Percentage' column (from CSV or Shapefile).
    *   Classifies wards as above or below user-specified urbanicity thresholds (e.g., 30%, 50%, 75%).
    *   This helps in contextualizing vulnerability, as intervention strategies might differ for highly urban vs. rural areas.

### 3.4. Knowledge Architecture (`kb.py`, LLM Integration in `routes.py`)

ChatMRPT incorporates a knowledge architecture to provide explanations, guidance, and contextual understanding.

*   **Static Knowledge Base (`kb.py`):**
    *   Contains pre-defined textual explanations for:
        *   **Methodologies:** Detailed descriptions of data cleaning (missing values), normalization, composite scoring, vulnerability ranking, and urban extent analysis.
        *   **Variable Categories:** General explanations for environmental, demographic, and epidemiological variables.
        *   **Specific Variable Rationales:** Explanations of why common variables (e.g., rainfall, temperature, NDVI, housing quality) are important for malaria risk.
    *   The `get_knowledge(topic, subtopic)` function retrieves this content.
*   **LLM-Based Dynamic Explanations & Guidance (via `routes.py` `send_message` and NLU intents like `explain_methodology`, `explain_variable`, `request_elaboration`):**
    *   **LLM Integration Detail:**
        *   **Contextual Explanations:** When a user asks for an explanation (e.g., "tell me more about normalization"), if the NLU identifies the intent and topic:
            1.  The system first attempts to retrieve content from the static `kb.py`.
            2.  If found, this content can be directly presented or used as rich context for the LLM.
            3.  The LLM (`generate_ai_response`) is then prompted to synthesize an explanation, potentially using the KB content as a factual basis, but tailoring the language to be conversational and address the user's specific phrasing.
        *   **Answering Follow-up Questions:** If a user asks "why were those variables chosen?" after an analysis, the LLM uses the context of the `analysis_result` (which includes variables used) and potentially KB rationales for those variables to generate an answer.
        *   **Elaboration on Topics:** For the `request_elaboration` intent, the LLM is prompted with the current `user_message`, `session_state`, previous `analysis_result`, and any `context_for_llm` (which might include `kb.py` content or details about the `last_visualization`) to provide more detailed information. The system prompt for `get_llm_nlu_response` specifically instructs the LLM to identify such requests.
*   **Semantic Retrieval (Implicit via LLM):** While not a classic vector-search semantic retrieval system, the LLM's inherent ability to understand semantic similarity allows it to map various user phrasings (e.g., "How do you clean data?" vs. "What do you do about missing values?") to the correct underlying concept and trigger appropriate explanations, often by leveraging the KB content provided in its context.

### 3.5. Visualization Engine (`visualization.py`)

The visualization engine translates processed data into interactive maps and charts using Plotly.

*   **Visual Output Types:**
    *   `create_variable_map()`: Choropleth map of a single raw variable. If missing values were imputed, it can show "before" and "after" maps.
    *   `create_normalized_map()`: Choropleth map of a single normalized variable, colored to reflect its contribution to risk (0-1 scale).
    *   `create_composite_map()`: Paginated subplots showing multiple composite risk score models.
    *   `box_plot_function()`: Generates data for paginated box and whisker plots showing the distribution of composite scores for each ward, ranked by median vulnerability. This is the primary "vulnerability ranking plot."
    *   `create_vulnerability_map()`: A single choropleth map displaying overall ward vulnerability ranks.
    *   `create_urban_extent_map()`: Combines urbanicity (wards above/below a threshold) with vulnerability ranking. Wards meeting the urban threshold are colored by vulnerability; others are grayed out. If the threshold is 0, it defaults to showing the standard vulnerability map.
    *   `create_decision_tree_plot()`: A static HTML visualization illustrating the analysis workflow from data input to priority wards.
*   **Interactivity:**
    *   Plotly maps offer zoom, pan, and hover-over tooltips displaying ward names and values.
    *   Pagination controls are dynamically added for composite maps and box plots via JavaScript (`main.js`).
*   **Output Format:** Visualizations are saved as standalone HTML files (via `create_plotly_html`) and embedded into the frontend using iframes or displayed in a modal. This ensures portability and interactivity without overwhelming the chat UI.
*   **CRS Standardization:** All geospatial visualizations ensure data is in EPSG:4326 (WGS84) using `ensure_wgs84_crs`.

### 3.6. Automated Report Generator (`report_generator.py`)

This module generates comprehensive analysis reports.

*   **LLM Integration Detail (Narrative Generation):**
    *   The `generate_ai_report_html_content` function orchestrates the report.
    *   For each section of the report (Data Overview, Missing Values, Normalization, Composite Scores, Vulnerability, Urban Extent, Conclusion), it:
        1.  Calls specific `summarize_..._for_ai` functions to create concise data summaries from the `DataHandler`'s state.
        2.  Constructs a detailed prompt for the LLM. This prompt instructs the LLM to act as an expert data analyst/epidemiologist, write in professional HTML format for that specific section, interpret the provided data summary, and explain its significance.
        3.  Provides the LLM with a "visualization manifest" listing all generated visualization HTML files (their titles, types, and embeddable URLs like `/serve_viz_file/<session_id>/filename.html`). The LLM is guided to refer to these visualizations by ID and optionally embed 1-2 highly relevant ones directly into the HTML report section using iframe tags.
        *   **Example System Prompt for a Report Section:**
            ```
            You are an expert data analyst... authoring a section for a Malaria Risk Report.
            Write directly in HTML format... Do NOT include <html>, <head>, or <body> tags...
            If you decide to embed a visualization iframe, use the 'Embeddable URL' directly.
            The current session_id for constructing iframe src URLs is: '{session_id}'.
            Focus on interpreting the provided data summary...
            ```
        *   **Example User Prompt for a Report Section:**
            ```
            Develop the content for the report section titled: 'Ward Vulnerability Ranking and Prioritization'.
            Relevant data summary for this section: Based on the median composite risk scores, all 275 wards were ranked... Top 5: WardA, WardB...
            Guidance on visualizations for this section: Crucially mention the vulnerability ranking plot (box plot) and the overall vulnerability map...
            Full manifest of available visualizations:
            - ID VIZ1: 'Vulnerability Ranking Plot' (... URL: /serve_viz_file/...)
            - ID VIZ2: 'Overall Vulnerability Map' (... URL: /serve_viz_file/...)
            Generate the HTML content for this section now.
            ```
    *   The LLM-generated HTML for each section is concatenated to form the full report.
*   **Output Formats:** Currently generates HTML. The stubs for PDF/DOCX indicate that external libraries (like WeasyPrint for PDF, Pandoc or python-docx for DOCX) would be needed for true conversion from the generated HTML. For now, it saves the HTML with a `.pdf.html` or `.docx.html` extension as a placeholder.
*   **Content:** Reports aim to combine quantitative summaries, LLM-generated narratives, and embedded/referenced visualizations.

### 3.7. External Services Integration

*   **AI Services (OpenAI):**
    *   The `openai` Python library is used to interact with GPT models (e.g., GPT-4o).
    *   API key is managed via environment variables (`OPENAI_API_KEY`) and accessed through `current_app.config`.
    *   Used for: NLU (intent/entity), dynamic explanations, contextual conversation, composite variable suggestion, and report narrative generation.
    *   Error handling is in place for API call failures, often falling back to simpler rule-based logic or default behaviors.
*   **Geospatial Libraries:**
    *   **Pandas & GeoPandas:** Core for tabular and vector geospatial data manipulation.
    *   **Shapely:** For geometric operations (used internally by GeoPandas).
    *   **PySAL (libpysal.weights.Queen):** For generating spatial weights matrices (Queen contiguity) used in spatial mean imputation.
    *   **Plotly:** For creating interactive maps and charts.
    *   **pyproj:** (Used internally by GeoPandas) for coordinate reference system transformations. All maps are standardized to WGS84 (EPSG:4326).

________________________________________

## 4. Development Methodology & Project Structure

ChatMRPT was developed iteratively, focusing on building a robust data backend first, then layering conversational AI and advanced visualization/reporting features.

**Phased Approach:**

1.  **Phase 1 – Core Data Handling & Analysis Logic (`data_handler.py`):**
    *   File ingestion (CSV, SHP).
    *   Data cleaning, normalization.
    *   Basic composite scoring and ranking.
    *   Foundation for visualization data preparation.
2.  **Phase 2 – Basic Conversational Layer & API (`routes.py`, `main.js`):**
    *   Flask app setup, basic routes for uploads and triggering analysis.
    *   Initial rule-based intent parsing.
    *   Simple frontend for interaction.
    *   Introduction of LLM for basic Q&A and NLU.
3.  **Phase 3 – Enhanced Visualization & Interaction (`visualization.py`, `main.js`):**
    *   Development of diverse interactive Plotly visualizations (variable maps, normalized maps, composite maps, box plots, urban extent).
    *   Integration of visualization display into the chat flow and modals.
    *   Refinement of NLU to handle visualization requests more accurately.
    *   LLM for explaining visualizations.
4.  **Phase 4 – Knowledge Architecture & Reporting (`kb.py`, `report_generator.py`, LLM enhancements):**
    *   Creation of the static knowledge base.
    *   Advanced LLM integration for dynamic explanations, contextual follow-ups, and report narrative generation.
    *   Multi-format report output structure.
    *   Session interaction logging and admin interface.

**Project File Structure (Key Files):**

```
ChatMRPT/
├── app/
│ ├── init.py # Flask app factory, configuration, blueprint registration
│ ├── routes.py # Main Flask routes, message handling, NLU, AI response generation
│ ├── kb.py # Static knowledge base
│ ├── models/
│ │ ├── analysis.py # Core data processing and analysis logic
│ │ ├── data_handler.py # Core data processing and analysis logic
│ │ ├── visualization.py # Visualization generation functions (Plotly)
│ │ ├── report_generator.py # Report generation logic
│ │ └── interaction_logger.py # Logs user interactions to SQLite
│ ├── static/
│ │ ├── css/styles.css
│ │ └── js/main.js
│ ├── templates/
│ │ ├── index.html # Main chat interface
│ │ ├── admin_logs.html # Admin log viewing pages
│ │ └── admin_session_detail.html
│ └── sample_data/ # Sample CSV and Shapefile for users
├── instance/ # NOT in Git - for uploads, reports, logs, SQLite DB
│ ├── uploads/
│ │ └── <session_id>/ # Session-specific uploaded files & generated viz
│ ├── reports/
│ │ └── <session_id>/ # Session-specific generated reports
│ ├── app.log
│ └── interactions.db
├── mrpt_venv/ # Virtual environment
├── .env # Environment variables (OPENAI_API_KEY, SECRET_KEY)
├── requirements.txt # Python dependencies
└── run.py # Script to run the Flask development server
```
________________________________________

## 5. LLM Integration Deep Dive

LLMs, primarily OpenAI's GPT models, are integral to ChatMRPT's intelligence and user experience. They are not just a chatbot layer but are woven into multiple analytical and explanatory processes.

**Key Areas of LLM Integration:**

1.  **Natural Language Understanding (NLU) (`routes.py: get_llm_nlu_response`):**
    *   **Purpose:** To interpret user's free-text input, identify their primary `intent` (e.g., run analysis, show map, explain concept), and extract relevant `entities` (e.g., variable names, map types, thresholds).
    *   **Mechanism:**
        *   A carefully crafted **system prompt** primes the LLM. This prompt includes:
            *   Its role as an NLU system for this specific tool.
            *   A list of predefined intents and entity types it should recognize.
            *   Current application state (files loaded, analysis status) to provide context.
            *   A list of available variables from the loaded dataset for accurate entity extraction.
            *   Context about the last shown visualization (if any) to understand follow-ups.
            *   Instructions to output a structured JSON object (`{"intent": "...", "entities": {...}}`).
        *   The user's message is passed as the user prompt.
        *   The OpenAI API is called (e.g., `gpt-4o`) with a low `temperature` (e.g., 0.1-0.2) for more deterministic NLU output. `response_format={ "type": "json_object" }` is used.
    *   **Benefit:** Allows for much more flexible and natural user interaction than purely rule-based systems. Handles variations in phrasing and complex requests.

2.  **Conversational AI & Dynamic Explanations (`routes.py: generate_ai_response`, `get_system_message`):**
    *   **Purpose:** To provide context-aware, natural language responses, explanations of analytical steps, interpretations of results, and general assistance.
    *   **Mechanism:**
        *   A **system persona prompt** (`get_system_message`) defines the AI's role as a helpful "Malaria Risk Analysis assistant."
        *   This prompt is dynamically updated with:
            *   Current session status (files loaded, analysis run, language).
            *   Key results from the last analysis if available (variables used, top vulnerable wards).
            *   Relevant snippets from the static knowledge base (`kb.py`) if the user's query pertains to a known topic (e.g., if asking about "rainfall", the KB entry for rainfall might be included in the LLM's context).
        *   A short **conversation history** (last N user/assistant turns) is included in the prompt sent to the LLM.
        *   The LLM (e.g., `gpt-4o` with a moderate `temperature` like 0.7 for more natural, less rigid responses) generates the textual reply.
    *   **Benefit:** Creates a more engaging and helpful user experience. The AI can explain *why* certain steps are taken or what results mean, adapting to the current context.

3.  **Composite Variable Suggestion (`data_handler.py: suggest_composite_variables`):**
    *   **Purpose:** To leverage the LLM's domain knowledge (when available and an API key is configured) to suggest an epidemiologically sound set of 3-5 variables for calculating composite risk scores if the user doesn't specify custom variables.
    *   **Mechanism:**
        *   A prompt is sent to the LLM, asking it to act as a "malaria epidemiology expert."
        *   The prompt includes the list of all available (cleaned and normalized) variables from the user's dataset, along with their determined relationship (direct/inverse) to malaria risk.
        *   The LLM is instructed to select variables considering balance (environmental, demographic), avoiding redundancy, and prioritizing those with strong known links to malaria (e.g., rainfall, temperature, housing quality, elevation), while specifically excluding 'urban' variables.
        *   It's asked to return *only* a comma-separated list of variable names.
    *   **Benefit:** Provides a sensible default set of variables for analysis, especially for users who may not be domain experts. This automates a step that often requires expert knowledge. A heuristic fallback is in place if the LLM is unavailable.

4.  **Report Narrative Generation (`report_generator.py: generate_ai_report_html_content`):**
    *   **Purpose:** To automatically generate human-readable textual explanations and interpretations for different sections of the final analysis report.
    *   **Mechanism:**
        *   For each report section (e.g., "Data Overview," "Vulnerability Ranking"), a specific prompt is constructed.
        *   The prompt instructs the LLM to act as an expert data analyst/epidemiologist writing that part of the report.
        *   It provides the LLM with a concise **data summary** for that section (generated by helper functions like `summarize_vulnerability_rankings_for_ai`).
        *   It also provides a **visualization manifest** – a list of all generated visualization files (HTML plots) with their titles, types, and embeddable URLs. The LLM is guided to refer to these visualizations by ID and can choose to embed 1-2 most relevant ones directly into the HTML report content it generates for that section using `<iframe>` tags. The current `session_id` is provided to correctly construct these iframe `src` URLs.
        *   The LLM is asked to output its response directly in HTML format (without `<html>`, `<head>`, `<body>` tags, as it's a section of a larger document).
    *   **Benefit:** Significantly reduces the manual effort of writing reports. The LLM can synthesize information from the data summaries and provide interpretations, making the reports more insightful than just a collection of tables and charts.

**General LLM Integration Considerations:**

*   **Prompt Engineering:** The quality and specificity of the system and user prompts are critical to getting desired outputs from the LLM. This is an iterative process.
*   **Context Window:** LLMs have limitations on input/output token length. Conversation history and data summaries must be managed to fit.
*   **Cost & Latency:** API calls to services like OpenAI incur costs and add latency. The system uses LLMs for tasks where their value (NLU, complex explanation, suggestion) outweighs these factors. Fallbacks are used where possible.
*   **Factuality & Hallucination:** While LLMs are powerful, they can sometimes "hallucinate" or provide incorrect information.
    *   For NLU, the output is constrained to a predefined set of intents/entities and validated.
    *   For explanations, providing factual context from `kb.py` or data summaries helps ground the LLM.
    *   For critical analytical steps like variable selection, user override is always possible, and the LLM's suggestion is a starting point.
*   **Error Handling:** Code includes `try-except` blocks around LLM API calls.

________________________________________

## 6. Key Code Snippets & Explanations

*(This section would be further populated with specific, illustrative code snippets as development finalizes or if particular areas need deeper technical clarification. For now, some conceptual examples were embedded in section 3 and 5.)*

**Example: DataHandler - Spatial Imputation Logic (Conceptual)**
Within `_handle_na_spatial_mean` in `data_handler.py`:
1.  Load shapefile using `geopandas`.
2.  Generate spatial weights: `w = libpysal.weights.Queen.from_dataframe(gdf)`.
3.  For each ward with a missing value in `column_to_impute`:
    a.  Get its neighbors: `neighbor_indices = w.neighbors[ward_index]`.
    b.  Get values of `column_to_impute` for these `neighbor_indices` from the main data `df`.
    c.  Calculate the mean of valid neighbor values.
    d.  Fill `df.loc[ward_index, column_to_impute]` with this mean.
    e.  Handle cases with no neighbors or no valid neighbor data (e.g., fallback to global mean).

**Example: Route for Handling Messages (Conceptual in `routes.py`)**
```python
@main_bp.route('/send_message', methods=['POST'])
def send_message():
    user_message = request.json.get('message')
    # ... (log user message) ...
    data_handler = get_data_handler()
    session_state = { ... }
    available_vars = get_available_variables(data_handler)
    
    # 1. NLU (LLM or Fallback)
    nlu_result = get_llm_nlu_response(user_message, session_state, available_vars, session.get('last_visualization'))
    if nlu_result is None:
        nlu_result = parse_message_intent_fallback(user_message, session_state, data_handler)
    
    intent = nlu_result.get('intent')
    entities = nlu_result.get('entities', {})

    # 2. Handle Pending Actions (e.g., confirmation for custom analysis)
    # ... (if session.get('pending_action') == 'confirm_custom_analysis') ...

    # 3. Action Dispatch based on intent
    if intent == 'run_standard_analysis':
        # ... call data_handler.run_full_analysis() ...
        # ... construct AI response with results ...
    elif intent == 'request_visualization':
        # ... extract viz_type, variable from entities ...
        # ... call get_visualization_response(data_handler, viz_type, variable) ...
    elif intent == 'explain_methodology':
        # ... get topic from entities ...
        # ... content = kb.get_knowledge('methodology', topic) ...
        # ... ai_response = generate_ai_response(user_message, ..., context_for_llm=content) ...
    # ... (other intents) ...
    else: # General query
        ai_response = generate_ai_response(user_message, session_state, ...)
        
    # ... (log assistant response, update session dialogue_context) ...
    return jsonify({'status': 'success', 'response': ai_response, 'action': ...})

7. Future Enhancements

Development is ongoing, with a focus on expanding capabilities and improving intelligence:

Enhanced Intelligence & Awareness:

More sophisticated dialogue state tracking for complex, multi-turn analytical scenarios.

Improved proactive suggestions based on user's data and analysis stage.

Better handling of ambiguous queries and user corrections.

Google Earth Engine (GEE) Integration:

Allow users to pull near real-time environmental data (rainfall, LST, NDVI, EVI) directly from GEE for specified regions and timeframes, enriching their local datasets.

Advanced Statistical Modeling Options:

Incorporate options for users to explore basic regression models or time-series analysis if their data supports it.

Voice Integration:

Enable users to interact with ChatMRPT using voice commands and receive spoken responses, improving accessibility for field use.

Offline Capabilities:

Develop a downloadable version or a PWA (Progressive Web App) with core analytical features that can function with pre-loaded data in low-connectivity settings. LLM-dependent features would degrade gracefully.

User Customization & Profiles:

Allow users to save preferred variable sets, report templates, or default map settings.

Model Interpretability Tools:

Integrate techniques like SHAP or LIME (if more complex models are introduced) to explain variable importance in risk scores.

Expanded Knowledge Base:

Continuously update kb.py with more variable rationales, methodological details, and case studies.

Fine-tuning LLMs:

Collect anonymized, high-quality interaction logs to potentially fine-tune smaller, open-source LLMs specifically for malaria risk NLU and explanation tasks, potentially reducing reliance on large commercial APIs for some functions.

This document provides a snapshot of the ChatMRPT system. It will be updated as the platform evolves.

