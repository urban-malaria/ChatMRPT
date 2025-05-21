# ChatMRPT: AI-Powered Malaria Risk Prioritization Interface

## Overview

ChatMRPT is an interactive chat-based interface for the Malaria Reprioritization Tool (MRPT), providing stakeholders with an intuitive way to analyze malaria risk factors and prioritize resource allocation for interventions like bed net distribution. This module combines advanced geospatial analysis with a natural language interface to make complex data analysis accessible to non-technical users.

## Folder Structure

The ChatMRPT module has the following structure:
```
ChatMRPT_v1/
│
├── app/                       # Main application package
│   ├── __init__.py            # Flask application initialization
│   ├── routes.py              # API endpoints and request handling
│   ├── utilities.py           # Utility functions
│   ├── ai_utils.py            # AI and LLM utilities
│   ├── kb.py                  # Knowledge base
│   │
│   ├── models/                # Core analytical components
│   │   ├── analysis.py        # Data analysis functions
│   │   ├── data_handler.py    # Data loading and processing
│   │   ├── report_generator.py # Report creation utilities
│   │   ├── visualization.py   # Visualization generation
│   │   └── interaction_logger.py # User interaction logging
│   │
│   ├── services/              # Service layer
│   │   ├── data_service.py    # Data processing service
│   │   ├── analysis_service.py # Analysis orchestration
│   │   ├── visualization_service.py # Visualization generation
│   │   └── service_container.py # Service initialization and management
│   │
│   ├── static/                # Static assets
│   │   ├── css/               # CSS stylesheets
│   │   │   └── styles.css     # Main application styling
│   │   │
│   │   ├── js/                # JavaScript files
│   │   │   └── main.js        # Frontend interaction logic
│   │   │
│   │   └── uploads/           # User uploaded files (not in git)
│   │
│   ├── templates/             # Jinja2 HTML templates
│   │   ├── base_tailwind.html # Base template with Tailwind CSS
│   │   ├── index.html         # Traditional UI interface
│   │   ├── index_tailwind.html # Modern Tailwind UI interface
│   │   ├── admin_logs.html    # Admin logs interface
│   │   └── admin_session_detail.html # Admin session details
│   │
│   └── sample_data/           # Example datasets
│       ├── sample_data_template.csv     # Example tabular data
│       └── sample_boundary_template.zip # Example shapefile
│
├── instance/                  # Instance-specific data (not in git)
│   ├── uploads/               # User uploaded data
│   └── reports/               # Generated reports
│
├── .env                       # Environment variables configuration
├── requirements.txt           # Python package dependencies
└── run.py                     # Application entry point
```

## Key Features

### Data Processing & Analysis
-   **Multi-format Data Support**: Process CSV, Excel, and GIS shapefiles (zipped).
-   **Intelligent Missing Value Handling**: Employs spatial imputation, mean/mode imputation, and other strategies.
-   **Variable Relationship Analysis**: Determines direct/inverse relationships of variables with malaria risk.
-   **Normalization Pipeline**: Scales diverse variables to a common range (0-1) for fair comparison.
-   **Composite Scoring Models**: Generates multiple risk assessment models using different variable combinations.
-   **Urban Extent Analysis**: Applies configurable urban percentage thresholds for targeted intervention planning.
-   **Data Quality Checks**: Identifies potential issues in uploaded data.

### Interactive Visualization
-   **Variable Distribution Maps**: Explore geographic patterns of raw risk factors.
-   **Normalized Variable Maps**: View standardized contributions of variables to risk.
-   **Composite Risk Maps**: Visualize results from multiple risk models.
-   **Vulnerability Ranking Plots & Maps**: Identify and visualize priority areas.
-   **Decision Tree Visualization**: Understand the analysis workflow graphically.
-   **Interactive Map Features**: Tooltips, zoom, pan for enhanced data exploration.

### User Experience & AI Interaction
-   **Natural Language Chat Interface**: Interact with the system using plain language commands and questions.
-   **AI-Powered Explanations**: Get detailed explanations for analysis steps, variable importance, ward rankings, and visualization interpretations.
-   **LLM-Assisted Variable Selection**: Option to use AI for selecting optimal variables for analysis.
-   **Guided Analysis Workflow**: The AI assistant can guide users through the analysis process.
-   **Custom Analysis Options**: Users can specify variables for tailored risk assessments.
-   **Comprehensive Reporting**: Generate detailed PDF, HTML, or Markdown reports of analysis findings.
-   **Multi-language Support**: Interface adaptable to different languages.
-   **Interaction Logging**: Detailed logging of user interactions, AI responses, and analysis events for audit and improvement.

## Technical Architecture

### Backend Components (Flask)
-   `app/__init__.py`: Initializes the Flask application, configures extensions (Flask-Session, logging), loads environment variables, and sets up paths.
-   `app/routes.py`: Defines all API endpoints, handles user requests, manages session data, and orchestrates calls to data handlers, analysis functions, and AI utilities.
-   `app/models/`:
    -   `data_handler.py`: Manages loading, cleaning, and preprocessing of CSV and shapefile data.
    -   `analysis.py`: Contains the core algorithms for data quality checks, missing value imputation, normalization, composite scoring, vulnerability ranking, and urban extent analysis. Includes `AnalysisMetadata` for tracking.
    -   `visualization.py`: Generates interactive maps and plots using Plotly.
    -   `report_generator.py`: Compiles analysis results into structured reports (PDF, HTML, MD).
    -   `interaction_logger.py`: Logs all significant user, system, and AI interactions to an SQLite database.
-   `app/services/`:
    -   `data_service.py`: Handles data transformation and preparation.
    -   `analysis_service.py`: Coordinates analysis operations.
    -   `visualization_service.py`: Manages the creation of visualizations.
    -   `service_container.py`: Initializes and provides access to all services.
-   `app/ai_utils.py`: Houses the `LLMManager` for interacting with language models (e.g., OpenAI GPT), prompt templates, intent extraction, and AI-driven explanation generation.
-   `app/kb.py`: Provides a knowledge base of domain-specific information (methodologies, variable rationales) to support AI explanations.
-   `app/utilities.py`: Contains helper functions for data validation, GIS operations, data processing, file handling, and error management.

### Frontend Components
-   `app/templates/base_tailwind.html`: Base template providing structure for Tailwind UI pages.
-   `app/templates/index.html`: The traditional interface built with Bootstrap.
-   `app/templates/index_tailwind.html`: The modern Tailwind CSS interface.
-   `app/templates/admin_*.html`: Templates for the admin dashboard to view interaction logs.
-   `app/static/js/main.js`: Handles client-side logic, AJAX calls to the backend, dynamic UI updates (chat messages, visualizations), and event handling.
-   `app/static/css/styles.css`: Provides custom styling for the application, including responsiveness and dark mode.

### Data Flow
1.  User interacts with the chat interface (`index.html` or `index_tailwind.html`, `main.js`).
2.  Messages are sent to Flask backend (`routes.py`).
3.  `LLMManager` (`ai_utils.py`) processes user input for intent and entities.
4.  Based on intent, `routes.py` calls appropriate functions in service layer (`data_service.py`, `analysis_service.py`, `visualization_service.py`).
5.  Services coordinate with models (`DataHandler`, `analysis.py`, `visualization.py`) to process data and generate results.
6.  `visualization.py` generates Plotly charts, saved to `instance/uploads/<session_id>/` and served via a dedicated route.
7.  `InteractionLogger` records events to `instance/interactions.db`.
8.  Results, visualizations, and AI explanations are sent back to the frontend.
9.  Reports are generated and saved in `instance/reports/`.

## Implementation Details

### Data Processing
-   **Integrity & Geospatial Alignment**: Handled by `DataHandler` and `utilities.py`.
-   **Ward Name Matching**: `DataHandler` attempts to reconcile ward names.
-   **Missing Value Imputation**: `analysis.py` provides multiple strategies, orchestrated by `DataHandler`.

### Analytical Methods
-   **Normalization & Composite Scores**: Implemented in `analysis.py`.
-   **Vulnerability Ranking & Urban Extent**: Implemented in `analysis.py`.

## Usage

1.  **Data Preparation**:
    *   Prepare a CSV or Excel file with ward-level data. Key variables might include environmental factors (rainfall, temperature), demographic data, and epidemiological indicators (e.g., Test Positivity Rate - TPR).
    *   Prepare a shapefile (zipped) containing the geographical boundaries for the wards.
    *   Ensure both datasets have a common column for joining, typically "WardName".
2.  **Launch Application**:
    *   Run `python run.py` from the `ChatMRPT_v1` root directory.
    *   Access the application in your web browser (usually `http://127.0.0.1:5000/`).
3.  **Analysis**:
    *   Upload your data files using the chat interface's upload button or load the sample data.
    *   Interact with the AI assistant via chat:
        *   Ask it to "Run the analysis."
        *   Request specific visualizations: "Show me a map of rainfall," "Generate the vulnerability plot."
        *   Ask for explanations: "Explain why Ward X is highly vulnerable," "What does the normalization step do?"
        *   Request custom analysis: "Run analysis using only rainfall and temperature."
4.  **Interpretation & Export**:
    *   Examine the generated visualizations to understand risk patterns.
    *   Identify high-priority areas for intervention based on vulnerability rankings.
    *   Generate a comprehensive report (PDF, HTML, MD) for stakeholders.
    *   Download individual visualizations.

## Development

### Requirements
-   Python 3.8+
-   Flask
-   Pandas, GeoPandas, NumPy
-   Plotly, WeasyPrint (for PDF reports, optional)
-   OpenAI Python client (for AI features)
-   python-dotenv
-   Other dependencies as listed in `requirements.txt`.

### Setup & Installation
```bash
# 1. Clone the repository (if you haven't already)
# git clone <repository_url>
# cd ChatMRPT_v1

# 2. Create a Python virtual environment
python -m venv venv

# 3. Activate the virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate

# 4. Install dependencies
pip install -r requirements.txt

# 5. Configure Environment Variables
# Create a .env file in the ChatMRPT_v1 root directory.
# Add the following, replacing with your actual values:
#
# SECRET_KEY=your_very_strong_random_secret_key_here
# FLASK_DEBUG=True 
# OPENAI_API_KEY=sk-your_openai_api_key_here
# OPENAI_MODEL_NAME=gpt-4o # Or your preferred model
# ADMIN_KEY=your_secure_admin_key_for_logs # For accessing /admin/logs
#

# 6. Run the application
python run.py