# ChatMRPT - Malaria Risk Prioritization Tool 🦟

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Flask](https://img.shields.io/badge/Flask-2.0+-green.svg)](https://flask.palletsprojects.com/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**ChatMRPT** is an advanced AI-powered malaria epidemiologist that provides intelligent data analysis, visualization, and insights for malaria control programs. Built with a modern modular architecture, it combines conversational AI with comprehensive data analysis tools.

## 🌟 Key Features

### 🧠 **AI-Powered Malaria Epidemiologist**
- **Conversational Interface**: Natural language interaction for data analysis
- **Nigeria State Tracking**: Comprehensive monitoring of malaria programs across all 36 states + FCT
- **Multi-Format Data Support**: CSV, Excel, Shapefiles with intelligent parsing
- **Ward-Level Analysis**: Detailed sub-state analysis and prioritization
- **Intelligent Variable Categorization**: LLM-powered analysis of uploaded datasets into epidemiological categories
- **Settlement Analysis**: Interactive building classification maps with 786K+ footprints and satellite validation

### 📊 **Advanced Analytics & Visualization**
- **Comprehensive Analysis Tools**: Statistical analysis, spatial analysis, and data processing
- **Interactive Maps**: Choropleth, vulnerability, and composite risk visualizations  
- **Dual Basemap System**: Street Map ↔ High-Resolution Satellite Imagery (Esri World Imagery) for ground-truthing
- **Settlement Visualization**: Building classification maps with transparent overlays for rooftop validation
- **Statistical Analysis**: Advanced statistical tools, correlation analysis, and ML capabilities
- **Risk Prioritization**: Evidence-based ward ranking and classification
- **Knowledge System**: Intelligent educational content and explanations
- **Visual Explanations**: Interactive guidance for analysis interpretation

### 🏗️ **Modern Architecture**
- **Modular Design**: Specialized modules organized by functionality
- **Service Container**: Modern dependency injection system (`app/services/container.py`)
- **Request Interpreter**: Intelligent natural language to tool mapping
- **Session Management**: Persistent conversation and analysis state
- **Blueprint-based Routing**: Organized route structure for maintainability

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Flask 2.0+
- Required packages (see requirements.txt)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/urban-malaria/ChatMRPT.git
   cd ChatMRPT
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**
   ```bash
   # Optional but recommended for enhanced AI features
   export OPENAI_API_KEY=your_api_key_here
   ```

4. **Run the application**
   ```bash
   python run.py
   ```

5. **Access the application**
   Open your browser to: `http://localhost:5000`

## 💬 Usage Examples

### Basic Conversational Queries
```
"Hello, who are you?"
"Tell me about malaria in Nigeria"
"What can you help me with?"
```

### Data Analysis Queries
```
"Upload my malaria surveillance data"
"Show me vulnerability maps for Lagos state"
"Which wards have the highest malaria risk?"
"Generate a summary report for the uploaded data"
```

### Specific Analysis Requests
```
"Create a choropleth map of case fatality rates"
"Perform statistical analysis on the uploaded dataset"
"Show me correlation between rainfall and malaria cases"
"Analyze settlement patterns in the data"
"Show me the settlement map"
"Create settlement map for Tudun Wada ward"
"Show settlement statistics"
"Integrate settlement data with analysis"
```

## 🏘️ Settlement Data Integration

ChatMRPT now includes advanced settlement analysis capabilities with interactive building classification maps:

### Features
- **786K+ Building Footprints**: Comprehensive coverage of Kano settlement data
- **AI Classification**: Formal, informal, and non-residential building types
- **Dual Basemap System**: Toggle between Street Map and High-Resolution Satellite Imagery
- **Interactive Controls**: Toggle settlement types, ward boundaries, and administrative labels
- **Ground-Truth Validation**: Transparent overlays allow rooftop verification of AI classifications
- **Natural Language Access**: Request settlement maps through chat interface

### Chat Commands
```bash
# Full city settlement map
"Show me the settlement map"

# Ward-specific detailed view  
"Create settlement map for [Ward Name]"

# Settlement data statistics
"Show settlement statistics"

# Integration with vulnerability analysis
"Integrate settlement data with analysis"
```

### Technical Implementation
- **Dynamic Settlement Loader**: Auto-detects settlement data from multiple locations
- **Scalable Architecture**: Works with any Nigerian state without hardcoding
- **Ward Boundary Integration**: 484 Kano ward boundaries for epidemiological context
- **Esri World Imagery**: High-resolution satellite imagery with proper attribution

## 🤖 Intelligent Variable Categorization

ChatMRPT uses advanced LLM-powered analysis to intelligently categorize uploaded dataset variables:

### How It Works
1. **Upload Data**: Upload your CSV and shapefile data
2. **AI Analysis**: LLM examines column names and categorizes them into epidemiological categories
3. **Dynamic Response**: See exactly which variables were found in your dataset
4. **Smart Recommendations**: Get tailored analysis suggestions based on your data

### Variable Categories
- **Malaria Indicators**: TPR, prevalence, cases, parasitemia, fever rates
- **Environmental Factors**: Rainfall, temperature, elevation, NDVI, water bodies
- **Demographic Factors**: Population density, age, household size, migration
- **Infrastructure Factors**: Roads, markets, schools, health facilities
- **Intervention Factors**: ITN coverage, IRS, treatment access, bed nets
- **Geographic Factors**: Ward names, coordinates, administrative boundaries

### Benefits
- **No Hardcoding**: Works with any variable naming convention
- **Epidemiological Context**: AI understands malaria risk factors
- **Transparent Results**: Shows actual variable names found in your data
- **Robust Fallback**: System remains reliable even if AI analysis fails

## 🏗️ Architecture Overview

### Core Services
- **Service Container** (`app/services/container.py`): Modern dependency injection system
- **LLM Manager** (`app/core/llm_manager.py`): AI conversation management
- **Request Interpreter** (`app/core/request_interpreter.py`): Natural language parsing
- **Session Memory** (`app/services/session_memory.py`): Session state management

### Tool Categories
1. **Knowledge Tools** (`app/tools/knowledge_tools.py`): Educational content and explanations
2. **Data Analysis Tools** (`app/tools/data_analysis_tools.py`): Statistical analysis and processing
3. **Visual Tools** (`app/tools/visual_tools.py`): Maps, charts, and visualizations
4. **Data Tools** (`app/tools/data_tools.py`): Data management and validation
5. **Statistical Tools** (`app/tools/statistical_tools.py`): Advanced statistical analysis
6. **Spatial Tools** (`app/tools/spatial_tools.py`): Geographic and spatial analysis
7. **Settlement Tools** (`app/tools/settlement_tools.py`, `app/tools/settlement_validation_tools.py`, `app/tools/settlement_visualization_tools.py`): Settlement analysis, validation, and interactive mapping
8. **Group Analysis Tools** (`app/tools/group_analysis_tools.py`): Multi-group comparisons
9. **Methodology Tools** (`app/tools/methodology_tools.py`): Research methodology guidance
10. **Visual Explanation Tools** (`app/tools/visual_explanation_tools.py`): Interactive explanations
11. **System Tools** (`app/tools/system_tools.py`): System utilities and diagnostics
12. **Environmental Risk Tools** (`app/tools/environmental_risk_tools.py`): Flood analysis, water proximity, elevation profiling
13. **Intervention Targeting Tools** (`app/tools/intervention_targeting_tools.py`): ITN/IRS prioritization, coverage gap analysis
14. **Memory Tools** (`app/tools/memory_tools.py`): Conversational continuity and analysis context
15. **Scenario Simulation Tools** (`app/tools/scenario_simulation_tools.py`): Coverage impact modeling, variable exclusion testing
16. **Strategic Decision Tools** (`app/tools/strategic_decision_tools.py`): Priority targeting, monitoring recommendations

### Route Structure
- **Core Routes** (`app/web/routes/core_routes.py`): Main application routes
- **Upload Routes** (`app/web/routes/upload_routes.py`): File upload and validation
- **Analysis Routes** (`app/web/routes/analysis_routes.py`): Analysis execution and chat
- **Visualization Routes** (`app/web/routes/visualization_routes.py`): Map and chart generation
- **Reports API** (`app/web/routes/reports_api_routes.py`): Report generation and downloads
- **Debug Routes** (`app/web/routes/debug_routes.py`): Development and diagnostic tools
- **Admin Routes** (`app/web/admin.py`): Administrative functions

## 📁 Project Structure

```
ChatMRPT/
├── app/                          # Main application package
│   ├── core/                     # Core services and managers
│   │   ├── llm_manager.py        # AI conversation management
│   │   ├── request_interpreter.py # Natural language parsing
│   │   ├── session_state.py      # Session state management
│   │   ├── responses.py          # Response formatting
│   │   ├── utils.py              # Core utilities
│   │   ├── decorators.py         # Application decorators
│   │   └── exceptions.py         # Custom exceptions
│   ├── services/                 # Service layer
│   │   ├── container.py          # Dependency injection container
│   │   ├── session_memory.py     # Session memory management
│   │   ├── visual_explanation.py # Visual explanation service
│   │   ├── tools/                # Tool-specific services
│   │   ├── visualization/        # Visualization services
│   │   └── agents/               # AI agent services
│   ├── tools/                    # Analysis and processing tools
│   │   ├── knowledge_tools.py    # Educational content
│   │   ├── data_analysis_tools.py # Statistical analysis
│   │   ├── visual_tools.py       # Visualization generation
│   │   ├── data_tools.py         # Data management
│   │   ├── statistical_tools.py  # Advanced statistics
│   │   ├── spatial_tools.py      # Spatial analysis
│   │   ├── settlement_tools.py   # Settlement analysis
│   │   ├── group_analysis_tools.py # Group comparisons
│   │   ├── methodology_tools.py  # Research methodology
│   │   ├── visual_explanation_tools.py # Interactive explanations
│   │   ├── settlement_validation_tools.py # Settlement validation
│   │   └── system_tools.py       # System utilities
│   ├── web/                      # Web interface layer
│   │   ├── routes/               # Route blueprints
│   │   │   ├── core_routes.py    # Core application routes
│   │   │   ├── upload_routes.py  # File upload handling
│   │   │   ├── analysis_routes.py # Analysis execution
│   │   │   ├── visualization_routes.py # Visualization routes
│   │   │   ├── reports_api_routes.py # Report generation
│   │   │   ├── debug_routes.py   # Debug and diagnostics
│   │   │   └── compatibility.py  # Legacy compatibility
│   │   ├── admin.py              # Administrative interface
│   │   └── utils/                # Web utilities
│   ├── config/                   # Configuration management
│   ├── models/                   # Data models
│   ├── intelligence/             # AI intelligence layer
│   ├── analysis/                 # Analysis modules
│   ├── interaction/              # User interaction handling
│   ├── reports/                  # Report generation
│   ├── prompts/                  # AI prompts and templates
│   ├── data/                     # Application data handling
│   ├── sample_data/              # Sample datasets
│   ├── static/                   # Static assets (CSS, JS, images)
│   ├── templates/                # HTML templates
│   └── routes.py                 # Additional route definitions
├── data/                         # Data storage directory
├── sessions/                     # Session persistence
├── instance/                     # Instance-specific files
├── tests/                        # Test suite
├── run.py                        # Application entry point
├── requirements.txt              # Python dependencies
├── ARCHITECTURE.md               # Architecture documentation
└── README.md                     # This file
```

## 🔧 Configuration

### Environment Variables
```bash
# Flask configuration
FLASK_ENV=development  # or production
DEBUG=True  # for development

# AI Features (Optional but recommended)
OPENAI_API_KEY=your_openai_api_key_here

# Server configuration
HOST=127.0.0.1  # Default: 127.0.0.1
PORT=5000       # Default: 5000
```

### Data Configuration
- Place your malaria dataset files in the `data/` directory
- Supported formats: CSV, Excel (.xlsx), Shapefiles (.shp with supporting files)
- The system automatically detects and processes Nigerian ward-level data

## 🧪 Testing

Run the test suite:
```bash
python -m pytest tests/
```

For specific test categories:
```bash
# Test conversational features
python tests/test_conversational_fix.py

# Test tool execution
python tests/test_final_tool_verification.py

# Test refactoring
python tests/test_refactoring.py
```

## 📊 Current Status

### ✅ Implemented Features
- **Modern Flask Architecture**: Blueprint-based routing with service container
- **AI-powered Chat Interface**: Natural language processing for data analysis
- **Comprehensive Tool Suite**: 11+ specialized tool categories with 100+ individual tools
- **Data Processing**: Multi-format data support (CSV, Excel, Shapefiles)
- **Visualization System**: Interactive maps and charts
- **Session Management**: Persistent conversation state
- **Settlement Analysis**: Advanced settlement validation and analysis
- **Statistical Analysis**: Comprehensive statistical processing capabilities
- **Spatial Analysis**: Geographic data processing and visualization

### 🚧 In Development
- Enhanced machine learning integration
- Advanced time series analysis
- Multi-panel dashboard system
- Performance optimization features

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📖 Documentation

- **Architecture Guide**: See `ARCHITECTURE.md` for detailed architecture documentation
- **Tool Reference**: Complete tool documentation available in the application interface
- **API Documentation**: Available through the debug routes when running in development mode

## 🐛 Bug Reports & Feature Requests

Please use the GitHub Issues tab to report bugs or request features. Include:
- Clear description of the issue/feature
- Steps to reproduce (for bugs)
- Expected vs actual behavior
- System information (OS, Python version, etc.)

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📞 Support

For support and questions:
- Create an issue on GitHub
- Check the documentation in the application
- Review the architecture guide for technical details

---

**ChatMRPT v3.0** - Empowering malaria control through intelligent data analysis 🌍 