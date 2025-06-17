# ChatMRPT Architecture Documentation

## 🏗️ System Overview

ChatMRPT v3.0 is a modern, AI-powered malaria epidemiologist built with a sophisticated modular architecture. The system combines conversational AI with comprehensive data analysis tools to provide intelligent malaria surveillance and control insights for Nigeria.

### Key Architectural Principles
- **Modular Design**: Clean separation of concerns across specialized modules
- **Service-Oriented Architecture**: Core services managed through modern dependency injection
- **Intelligent Request Processing**: Enhanced natural language to tool execution pipeline with LLM-based classification
- **Session Management**: Persistent conversation and analysis state
- **Extensible Tool System**: 11+ specialized tool categories with 100+ individual tools
- **Blueprint-based Routing**: Organized Flask routing structure for maintainability

## 🧠 Core Architecture Components

### 1. Service Container (`app/services/container.py`)
The modern dependency injection system that manages all core services:

```python
class ServiceContainer:
    - Centralized service management and initialization
    - Configuration management
    - Service lifecycle management
    - Integration with Flask application factory
```

**Responsibilities:**
- Service registration and resolution
- Configuration management and initialization
- Application startup coordination
- Resource management

### 2. LLM Manager (`app/core/llm_manager.py`)
Manages AI conversation capabilities with enhanced ChatMRPT personality:

```python
class LLMManager:
    - personality_prompt: Comprehensive malaria epidemiologist persona
    - conversation_history: Session-based memory
    - response_generation: Context-aware responses
    - fallback_handling: Graceful degradation
```

**Key Features:**
- Nigeria state tracking and malaria expertise
- Multi-format data handling (CSV, Excel, Shapefiles)
- Ward-level analysis capabilities
- Settlement analysis integration
- Adaptive language based on user expertise

### 3. Request Interpreter (`app/core/request_interpreter.py`)
The intelligent brain that converts natural language to tool executions:

```python
class RequestInterpreter:
    - intelligent_classification: Pattern-based and LLM-assisted intent analysis
    - multi_tool_handling: Support for complex multi-tool requests
    - tool_mapping: Natural language to tool conversion
    - parameter_extraction: Context-aware argument parsing
    - execution_planning: Optimal tool selection and sequencing
```

**Enhanced Processing Pipeline:**
1. Intent classification using multiple methods
2. Tool selection based on intent and context
3. Parameter extraction and validation
4. Execution plan generation
5. Tool orchestration and response formatting

### 4. Session Management
- **Session State** (`app/core/session_state.py`): Core session state management
- **Session Memory** (`app/services/session_memory.py`): Enhanced memory management with persistence

## 🛠️ Tool System Architecture

### Tool Categories (100+ Tools)

#### 1. Knowledge Tools (`app/tools/knowledge_tools.py`)
Educational and explanatory content:
- Malaria epidemiology explanations
- Nigeria-specific malaria information
- Ward-level analysis guidance
- Data interpretation assistance
- Settlement analysis explanations

#### 2. Data Analysis Tools (`app/tools/data_analysis_tools.py`)
Statistical analysis and data processing:
- Descriptive statistics
- Correlation analysis
- Advanced statistical methods
- Risk factor identification
- Data quality assessment

#### 3. Visual Tools (`app/tools/visual_tools.py`)
Map and chart generation:
- Choropleth maps
- Vulnerability classifications
- Composite risk visualizations
- Interactive dashboards
- Statistical charts and plots

#### 4. Data Tools (`app/tools/data_tools.py`)
Data management and validation:
- File upload and processing
- Data validation and cleaning
- Format conversion
- Ward name standardization
- Data quality assessment

#### 5. Statistical Tools (`app/tools/statistical_tools.py`)
Advanced statistical analysis:
- Advanced statistical methods
- Regression analysis
- Model fitting and validation
- Statistical testing
- Performance metrics

#### 6. Spatial Tools (`app/tools/spatial_tools.py`)
Geographic and spatial analysis:
- Spatial autocorrelation analysis
- Geographic analysis
- Distance-based analysis
- Spatial clustering identification
- Geospatial data processing

#### 7. Settlement Tools (`app/tools/settlement_tools.py`)
Settlement analysis and validation:
- Settlement pattern analysis
- Validation workflows
- Settlement clustering
- Geographic settlement analysis

#### 8. Group Analysis Tools (`app/tools/group_analysis_tools.py`)
Multi-group comparison and segmentation:
- Cross-group statistical comparisons
- Demographic segmentation analysis
- Intervention group effectiveness assessment
- Population subgroup analysis

#### 9. Methodology Tools (`app/tools/methodology_tools.py`)
Research methodology guidance and validation:
- Study design recommendations
- Statistical method selection guidance
- Data collection methodology advice
- Analysis interpretation guidelines

#### 10. Visual Explanation Tools (`app/tools/visual_explanation_tools.py`)
Interactive analysis interpretation and guidance:
- Step-by-step analysis explanations
- Visual interpretation guidance
- Interactive chart and map explanations
- Analysis workflow demonstrations

#### 11. System Tools (`app/tools/system_tools.py`)
System utilities and diagnostics:
- System diagnostics
- Performance monitoring
- Error handling utilities
- Configuration management

#### 12. Settlement Validation Tools (`app/tools/settlement_validation_tools.py`)
Specialized settlement validation:
- Settlement data validation
- Quality control workflows
- Consistency checking
- Validation reporting

### Core Services

#### Visual Explanation Service (`app/services/visual_explanation.py`)
Centralized service for generating interactive explanations:
- Dynamic explanation generation
- Context-aware guidance
- Multi-format explanation support
- Integration with all tool categories

## 🌐 Web Architecture

### Modern Blueprint Organization
The Flask application uses a modern blueprint-based architecture organized in `app/web/routes/`:

#### Core Routes (`app/web/routes/core_routes.py`)
- Main application interface
- Session initialization and management
- User interface routing
- Application status endpoints

#### Upload Routes (`app/web/routes/upload_routes.py`)
- File upload handling with validation
- Multi-format support (CSV, Excel, Shapefiles)
- Data preview and confirmation
- Error handling and user feedback

#### Analysis Routes (`app/web/routes/analysis_routes.py`)
- Chat interface for conversational analysis
- Tool execution and orchestration
- Real-time response handling
- Session persistence and recovery

#### Visualization Routes (`app/web/routes/visualization_routes.py`)
- Map generation and serving
- Chart creation and customization
- Interactive visualization handling
- Export functionality

#### Reports API Routes (`app/web/routes/reports_api_routes.py`)
- RESTful API endpoints for reports
- Data export and download functionality
- Report generation and management
- API documentation endpoints

#### Debug Routes (`app/web/routes/debug_routes.py`)
- Development and diagnostic tools
- System monitoring endpoints
- Performance metrics
- Debugging utilities

#### Admin Routes (`app/web/admin.py`)
- Administrative interface
- System configuration
- User management
- Application monitoring

#### Compatibility Routes (`app/web/routes/compatibility.py`)
- Legacy system compatibility
- Migration support
- Backward compatibility features

### Additional Route Configuration (`app/routes.py`)
- Static file handling
- Error pages (404, 500, 403)
- Security headers
- Health check endpoints

## 🔄 Data Flow Architecture

### Request Processing Pipeline

```
User Input → Request Interpreter → Tool Selection → Execution → Response
     ↓              ↓                    ↓             ↓          ↓
   Parse          Classify           Map Tools      Execute    Format
   Intent         Intent             & Params       Tools      Response
```

### Detailed Flow:

1. **User Input Processing**
   - Natural language query received via Flask routes
   - Context and session state loaded from session management
   - Input validation and sanitization

2. **Intent Classification**
   - Multi-method classification system in Request Interpreter
   - Pattern-based matching with fallback strategies
   - Context-aware routing to appropriate tools

3. **Tool Selection & Mapping**
   - Intent-to-tool mapping using tool registry
   - Parameter extraction from natural language
   - Execution plan generation
   - Resource validation

4. **Tool Execution**
   - Tool orchestration through service container
   - Error handling and recovery mechanisms
   - Progress tracking and logging
   - Result aggregation

5. **Response Generation**
   - Result formatting and presentation
   - Visualization generation when needed
   - Summary and insights compilation
   - Session state updates and persistence

## 📊 Data Management Architecture

### Storage Strategy
- **Session Data**: Managed by session_memory service with persistence in `sessions/`
- **Uploaded Data**: Temporary processing storage in `instance/uploads/`
- **Analysis Results**: Cached computation results in memory and session
- **Reports**: Generated reports stored in `instance/reports/`
- **Logs**: Application logs in `instance/app.log`

### Data Processing Pipeline
1. **Upload & Validation**: Multi-format file handling with comprehensive validation
2. **Standardization**: Nigerian ward name normalization and data cleaning
3. **Analysis**: Statistical and spatial processing through specialized tools
4. **Visualization**: Map and chart generation using visual tools
5. **Storage**: Result caching and session persistence

## 🔒 Security & Error Handling

### Security Measures
- Input validation and sanitization across all routes
- File upload restrictions and validation
- Session security and isolation
- Security headers applied via middleware
- Error handling with graceful degradation

### Error Handling Strategy
- Comprehensive exception handling in `app/core/exceptions.py`
- User-friendly error pages (404, 500, 403)
- Logging and monitoring through Flask logging system
- Recovery mechanisms and fallbacks
- Graceful degradation for AI features

## 🚀 Performance & Scalability

### Current Optimization Features
- **Service Container**: Efficient service management and initialization
- **Session Management**: Optimized session state handling
- **Tool Caching**: Results caching within sessions
- **Blueprint Architecture**: Modular route organization for maintainability

### Scalability Design
- Modular architecture supports horizontal scaling
- Service-oriented design enables independent scaling
- Session management designed for load balancing
- Tool system designed for parallel execution

## 🧪 Testing & Quality Assurance

### Current Testing Strategy
- **Unit Tests**: Individual component testing in `tests/`
- **Integration Tests**: Service interaction testing
- **End-to-End Tests**: Complete workflow testing
- **Conversational Tests**: AI interaction testing

### Test Files
- `tests/test_conversational_fix.py`: Conversational feature testing
- `tests/test_final_tool_verification.py`: Tool execution testing
- `tests/test_refactoring.py`: Architecture and refactoring tests
- `tests/test_phase3_end_to_end.py`: End-to-end integration tests

## 📋 Configuration & Deployment

### Environment Configuration
```bash
# Flask Environment
FLASK_ENV=development  # or production
DEBUG=True            # for development

# AI Features (Optional but recommended)
OPENAI_API_KEY=your_api_key_here

# Server Configuration
HOST=127.0.0.1       # Default host
PORT=5000            # Default port
```

### Application Startup
The application uses a modern factory pattern in `run.py`:
- Configuration detection and loading
- Service container initialization
- Blueprint registration
- Additional route setup
- Development server startup

### Deployment Considerations
- **Development**: Flask development server via `python run.py`
- **Production**: Gunicorn/uWSGI with Nginx recommended
- **Containerization**: Docker support can be added
- **Cloud Deployment**: Platform-specific configurations available

## 🔮 Current Status & Roadmap

### ✅ Fully Implemented
- **Core Architecture**: Modern Flask application with service container
- **AI Integration**: LLM-powered conversational interface with fallback
- **Tool System**: 11+ specialized tool categories with 100+ tools
- **Data Processing**: Multi-format support with validation
- **Visualization**: Interactive maps and charts
- **Session Management**: Persistent conversation state
- **Settlement Analysis**: Advanced settlement validation and analysis
- **Web Interface**: Modern blueprint-based routing system

### 🚧 In Active Development
- Enhanced machine learning integration
- Advanced time series analysis capabilities
- Multi-panel dashboard system
- Performance optimization features

### 🔮 Future Enhancements
- **Machine Learning Integration**: Random Forest, clustering algorithms, classification models
- **Advanced Analytics**: Time series forecasting, spatial interpolation
- **Enhanced Visualizations**: Multi-panel dashboards, 3D visualizations
- **Performance Optimization**: Caching, parallel processing, optimization

## 📁 Directory Structure Summary

```
ChatMRPT/
├── app/                          # Main application package
│   ├── core/                     # Core system components
│   ├── services/                 # Service layer with dependency injection
│   ├── tools/                    # Comprehensive tool system (11+ categories)
│   ├── web/                      # Web interface layer with blueprints
│   ├── config/                   # Configuration management
│   ├── models/                   # Data models
│   ├── intelligence/             # AI intelligence layer
│   ├── analysis/                 # Analysis modules
│   ├── interaction/              # User interaction handling
│   ├── reports/                  # Report generation
│   ├── prompts/                  # AI prompts and templates
│   ├── data/                     # Application data handling
│   ├── sample_data/              # Sample datasets
│   ├── static/                   # Static web assets
│   ├── templates/                # HTML templates
│   └── routes.py                 # Additional route definitions
├── instance/                     # Instance-specific files (uploads, reports, logs)
├── sessions/                     # Session persistence
├── data/                         # User data storage
├── tests/                        # Comprehensive test suite
└── run.py                        # Application entry point
```

---

This architecture document reflects the current implementation of ChatMRPT v3.0. For specific implementation details, refer to the individual module documentation and code comments. 