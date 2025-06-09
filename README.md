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

### 📊 **Advanced Analytics & Visualization**
- **96+ Analysis Tools**: Comprehensive toolset across 6 categories
- **Interactive Maps**: Choropleth, vulnerability, and composite risk visualizations  
- **Statistical Analysis**: PCA, clustering, correlation analysis
- **Risk Prioritization**: Evidence-based ward ranking and classification

### 🏗️ **Modern Architecture**
- **Modular Design**: 22 specialized modules for maintainability
- **Service Container**: Dependency injection for core services
- **Request Interpreter**: Intelligent natural language to tool mapping
- **Session Management**: Persistent conversation and analysis state

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Flask 2.0+
- Required packages (see requirements.txt)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/ChatMRPT.git
   cd ChatMRPT
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the application**
   ```bash
   python run.py
   ```

4. **Access the application**
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
"Perform PCA analysis on the uploaded dataset"
"Show me correlation between rainfall and malaria cases"
```

## 🏗️ Architecture Overview

### Core Services
- **Service Container** (`app/core/service_container.py`): Dependency injection
- **LLM Manager** (`app/core/llm_manager.py`): AI conversation management
- **Request Interpreter** (`app/core/request_interpreter.py`): Natural language parsing

### Tool Categories (96+ Tools)
1. **Knowledge Tools**: Explanations and educational content
2. **Analysis Tools**: Statistical analysis and data processing
3. **Visualization Tools**: Maps, charts, and interactive displays
4. **Summary Tools**: Report generation and insights
5. **Data Tools**: Upload, validation, and management
6. **Utility Tools**: Helper functions and system operations

### Route Structure
- **Core Routes**: Index, session management, app status
- **Upload Routes**: File upload and validation
- **Analysis Routes**: Analysis execution and AI chat
- **Visualization Routes**: Map and chart generation
- **Reports API**: Report generation and downloads
- **Debug Routes**: Development and diagnostic tools

## 📁 Project Structure

```
ChatMRPT/
├── app/                          # Main application package
│   ├── core/                     # Core services and managers
│   │   ├── service_container.py  # Dependency injection
│   │   ├── llm_manager.py        # AI conversation management
│   │   └── request_interpreter.py # Natural language parsing
│   ├── tools/                    # Analysis and processing tools
│   │   ├── knowledge_tools.py    # Educational and explanatory content
│   │   ├── analysis_tools.py     # Statistical analysis functions
│   │   ├── visualization_tools.py # Map and chart generation
│   │   ├── summary_tools.py      # Report generation
│   │   ├── data_tools.py         # Data management functions
│   │   └── utility_tools.py      # Helper utilities
│   ├── routes/                   # Flask route blueprints
│   │   ├── main_routes.py        # Core application routes
│   │   ├── upload_routes.py      # File upload handling
│   │   ├── analysis_routes.py    # Analysis execution
│   │   └── api_routes.py         # API endpoints
│   ├── static/                   # Static assets (CSS, JS, images)
│   └── templates/                # HTML templates
├── data/                         # Data storage directory
├── sessions/                     # Session persistence
├── run.py                        # Application entry point
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

## 🔧 Configuration

### Environment Variables
Create a `.env` file with:
```
FLASK_ENV=development
SECRET_KEY=your_secret_key_here
OPENAI_API_KEY=your_openai_api_key_here  # Optional for enhanced features
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
python test_conversational_fix.py

# Test tool execution
python test_final_tool_verification.py
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📖 Documentation

- **Architecture Guide**: See `tests/ARCHITECTURE.md` for detailed architecture documentation
- **API Documentation**: Available at `/api/docs` when running the application
- **Tool Reference**: Complete tool documentation in the application interface

## 🐛 Bug Reports & Feature Requests

Please use the GitHub Issues tab to report bugs or request features. Include:
- Clear description of the issue/feature
- Steps to reproduce (for bugs)
- Expected vs actual behavior
- System information (OS, Python version, etc.)

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Nigerian Ministry of Health for malaria surveillance data standards
- WHO malaria elimination framework
- Open source GIS and data analysis communities
- Flask and Python ecosystem contributors

## 📞 Support

For support and questions:
- Create an issue on GitHub
- Check the documentation in the application
- Review the architecture guide for technical details

---

**ChatMRPT** - Empowering malaria control through intelligent data analysis 🌍 