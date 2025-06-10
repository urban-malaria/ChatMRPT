# ChatMRPT Architecture Documentation

## 🏗️ System Overview

ChatMRPT is a modern, AI-powered malaria epidemiologist built with a sophisticated modular architecture. The system combines conversational AI with comprehensive data analysis tools to provide intelligent malaria surveillance and control insights.

### Key Architectural Principles
- **Modular Design**: Clean separation of concerns across specialized modules
- **Service-Oriented Architecture**: Core services managed through dependency injection
- **Intelligent Request Processing**: Enhanced natural language to tool execution pipeline with LLM-based classification
- **Session Management**: Persistent conversation and analysis state
- **Extensible Tool System**: 100+ tools organized into 10 specialized categories
- **Enhanced Knowledge System**: Multi-tool response combination with intelligent merging and smooth transitions

## 🧠 Core Architecture Components

### 1. Service Container (`app/core/service_container.py`)
The central dependency injection system that manages all core services:

```python
class ServiceContainer:
    - llm_manager: LLMManager
    - request_interpreter: RequestInterpreter
    - data_manager: DataManager
    - session_manager: SessionManager
    - tool_registry: ToolRegistry
```

**Responsibilities:**
- Centralized service management
- Dependency injection for all components
- Service lifecycle management
- Configuration and initialization

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
- Adaptive language based on user expertise

### 3. Request Interpreter (`app/core/request_interpreter.py`)
The enhanced intelligent brain that converts natural language to tool executions with LLM-powered classification:

```python
class RequestInterpreter:
    - intelligent_classification: LLM-based intent analysis with fallback patterns
    - multi_tool_handling: Support for complex multi-tool requests
    - response_combination: Intelligent merging of multiple tool responses
    - tool_mapping: Natural language to tool conversion
    - parameter_extraction: Context-aware argument parsing
    - execution_planning: Optimal tool selection and sequencing
```

**Enhanced Processing Pipeline:**
1. Intelligent intent classification using LLM with pattern-based fallback
2. Multi-tool support for complex requests (e.g., multiple explain_concept calls)
3. Tool selection based on intent and context
4. Parameter extraction and validation
5. Execution plan generation with parallel processing support
6. Tool orchestration and intelligent response combination
7. Response formatting with smooth transitions and proper structure

## 🛠️ Tool System Architecture

### Tool Categories (100+ Tools)

#### 1. Enhanced Knowledge Tools (`app/tools/knowledge_tools.py`)
Educational and explanatory content with intelligent response combination:
- Enhanced malaria epidemiology explanations with proper structure and length control
- Nigeria-specific malaria information with smooth transitions
- Ward-level analysis guidance with intelligent response merging
- Data interpretation assistance with consistent epidemiologist persona
- Multi-response combination for comprehensive answers (200-600 words optimal)
- Intelligent transitions between different knowledge topics

#### 2. Analysis Tools (`app/tools/analysis_tools.py`)
Statistical analysis and data processing:
- Descriptive statistics
- Correlation analysis
- PCA and clustering
- Time series analysis
- Risk factor identification

#### 3. Visualization Tools (`app/tools/visualization_tools.py`)
Map and chart generation:
- Choropleth maps
- Vulnerability classifications
- Composite risk visualizations
- Interactive dashboards
- Statistical charts

#### 4. Summary Tools (`app/tools/summary_tools.py`)
Report generation and insights:
- Executive summaries
- Ward prioritization reports
- Trend analysis reports
- Comparative analysis
- Recommendations generation

#### 5. Data Tools (`app/tools/data_tools.py`)
Data management and validation:
- File upload and processing
- Data validation and cleaning
- Format conversion
- Ward name standardization
- Data quality assessment

#### 6. Utility Tools (`app/tools/utility_tools.py`)
Helper functions and system operations:
- Session management
- Configuration handling
- Error recovery
- Performance monitoring
- System diagnostics

#### 7. Group Analysis Tools (`app/tools/group_analysis_tools.py`)
Specialized tools for multi-group comparison and segmentation:
- Cross-group statistical comparisons
- Demographic segmentation analysis
- Intervention group effectiveness assessment
- Population subgroup analysis

#### 8. Methodology Tools (`app/tools/methodology_tools.py`)
Research methodology guidance and validation:
- Study design recommendations
- Statistical method selection guidance
- Data collection methodology advice
- Analysis interpretation guidelines

#### 9. Spatial Tools (`app/tools/spatial_tools.py`)
Advanced spatial analysis and geographic modeling:
- Spatial autocorrelation analysis
- Geographic weighted regression
- Spatial clustering identification
- Distance-based analysis

#### 10. Visual Explanation Tools (`app/tools/visual_explanation_tools.py`)
Interactive analysis interpretation and guidance:
- Step-by-step analysis explanations
- Visual interpretation guidance
- Interactive chart and map explanations
- Analysis workflow demonstrations

### Core Services

#### Visual Explanation Service (`app/services/visual_explanation.py`)
Centralized service for generating interactive explanations:
- Dynamic explanation generation
- Context-aware guidance
- Multi-format explanation support
- Integration with all tool categories

## 🌐 Web Architecture

### Route Organization
The Flask application is organized into logical route blueprints:

#### Main Routes (`app/routes/main_routes.py`)
- Index page and core navigation
- Session initialization
- Application status and health checks
- User preference management

#### Upload Routes (`app/routes/upload_routes.py`)
- File upload handling with validation
- Multi-format support (CSV, Excel, Shapefiles)
- Data preview and confirmation
- Error handling and user feedback

#### Analysis Routes (`app/routes/analysis_routes.py`)
- Chat interface for conversational analysis
- Tool execution and orchestration
- Real-time response streaming
- Session persistence and recovery

#### API Routes (`app/routes/api_routes.py`)
- RESTful API endpoints
- Data export and download
- System status and metrics
- Integration capabilities

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
   - Natural language query received
   - Context and session state loaded
   - Input validation and sanitization

2. **Intent Classification**
   - Multi-method classification system
   - Rule-based pattern matching
   - Semantic similarity analysis
   - Context-aware routing

3. **Tool Selection & Mapping**
   - Intent-to-tool mapping
   - Parameter extraction
   - Execution plan generation
   - Resource validation

4. **Tool Execution**
   - Parallel execution where possible
   - Error handling and recovery
   - Progress tracking
   - Result aggregation

5. **Response Generation**
   - Result formatting and presentation
   - Visualization generation
   - Summary and insights
   - Session state updates

## 📊 Data Management Architecture

### Data Storage Strategy
- **Session Data**: Persistent conversation state
- **Uploaded Data**: Temporary processing storage
- **Analysis Results**: Cached computation results
- **Visualization Assets**: Generated maps and charts

### Data Processing Pipeline
1. **Upload & Validation**: File format detection and validation
2. **Standardization**: Ward name normalization and data cleaning
3. **Analysis**: Statistical processing and computation
4. **Visualization**: Map and chart generation
5. **Storage**: Result caching and session persistence

## 🔒 Security & Error Handling

### Security Measures
- Input validation and sanitization
- File upload restrictions and validation
- Session security and isolation
- API rate limiting and protection

### Error Handling Strategy
- Graceful degradation for all components
- Comprehensive exception handling
- User-friendly error messages
- Recovery mechanisms and fallbacks
- Logging and monitoring

## 🚀 Performance & Scalability

### Optimization Features
- **Lazy Loading**: Components loaded on demand
- **Caching**: Results and computations cached
- **Parallel Processing**: Multiple tools executed simultaneously
- **Resource Management**: Efficient memory and CPU usage

### Scalability Considerations
- Modular architecture supports horizontal scaling
- Service-oriented design enables independent scaling
- Stateless tool execution for distributed processing
- Session management supports load balancing

## 🧪 Testing & Quality Assurance

### Testing Strategy
- **Unit Tests**: Individual component testing
- **Integration Tests**: Service interaction testing
- **End-to-End Tests**: Complete workflow testing
- **Performance Tests**: Load and stress testing

### Quality Metrics
- **Code Coverage**: Comprehensive test coverage
- **Performance Benchmarks**: Response time and resource usage
- **Reliability Metrics**: Uptime and error rates
- **User Experience**: Interaction quality and satisfaction

## 🔮 Future Architecture Considerations

### Planned Enhancements (See STATISTICAL_VISUALIZATION_ENHANCEMENT_OPPORTUNITIES.md)
- **Machine Learning Integration**: Random Forest, clustering algorithms, classification models
- **Multi-Panel Dashboards**: Comprehensive overview displays with linked interactions
- **Intelligent Variable Selection**: Auto-detect optimal variables, handle multicollinearity
- **Enhanced Chart Types**: Violin plots, parallel coordinates, bivariate choropleth maps
- **Time Series Analysis**: Temporal analysis, forecasting, seasonal decomposition
- **Advanced Spatial Methods**: Geographically weighted regression, spatial interpolation

### Statistical Enhancement Roadmap
**High Priority Enhancements:**
1. Intelligent variable selection and multicollinearity handling
2. Multi-panel dashboard creation with interactive linking
3. Machine learning model integration (Random Forest, SVM, Neural Networks)
4. Enhanced visualization types (violin plots, parallel coordinates, 3D surfaces)

**Medium Priority Features:**
5. Time series analysis and forecasting capabilities
6. Model diagnostics with automated assumption testing
7. Advanced spatial analysis methods (GWR, spatial interpolation)

**Advanced Capabilities:**
8. Specialized malaria epidemiological tools (ROC analysis, intervention impact)
9. Performance optimization (caching, parallel processing)
10. User experience enhancements (guided workflows, template gallery)

### Extensibility Features
- **Plugin System**: Third-party tool integration
- **Custom Visualizations**: User-defined chart types
- **External Data Sources**: API integrations
- **Multi-language Support**: Internationalization
- **Advanced Analytics**: Machine learning integration with comprehensive model library

## 📋 Configuration & Deployment

### Environment Configuration
```bash
# Development
FLASK_ENV=development
DEBUG=True

# Production
FLASK_ENV=production
DEBUG=False
SECRET_KEY=secure_random_key

# Optional AI Features
OPENAI_API_KEY=your_api_key_here
```

### Deployment Options
- **Local Development**: Flask development server
- **Production**: Gunicorn/uWSGI with Nginx
- **Containerization**: Docker support
- **Cloud Deployment**: Platform-specific configurations

---

This architecture document provides a comprehensive overview of ChatMRPT's design and implementation. For specific implementation details, refer to the individual module documentation and code comments. 