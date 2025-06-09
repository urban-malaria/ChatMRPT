# ChatMRPT Architecture Documentation

## 🏗️ System Overview

ChatMRPT is a modern, AI-powered malaria epidemiologist built with a sophisticated modular architecture. The system combines conversational AI with comprehensive data analysis tools to provide intelligent malaria surveillance and control insights.

### Key Architectural Principles
- **Modular Design**: Clean separation of concerns across specialized modules
- **Service-Oriented Architecture**: Core services managed through dependency injection
- **Intelligent Request Processing**: Natural language to tool execution pipeline
- **Session Management**: Persistent conversation and analysis state
- **Extensible Tool System**: 96+ tools organized into logical categories

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
The intelligent brain that converts natural language to tool executions:

```python
class RequestInterpreter:
    - intent_classification: Multi-method analysis
    - tool_mapping: Natural language to tool conversion
    - parameter_extraction: Context-aware argument parsing
    - execution_planning: Optimal tool selection and sequencing
```

**Processing Pipeline:**
1. Intent classification (knowledge, analysis, visualization, etc.)
2. Tool selection based on intent and context
3. Parameter extraction and validation
4. Execution plan generation
5. Tool orchestration and response formatting

## 🛠️ Tool System Architecture

### Tool Categories (96+ Tools)

#### 1. Knowledge Tools (`app/tools/knowledge_tools.py`)
Educational and explanatory content with ChatMRPT personality:
- Malaria epidemiology explanations
- Nigeria-specific malaria information
- Ward-level analysis guidance
- Data interpretation assistance

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

### Planned Enhancements
- **Microservices Migration**: Break down into smaller services
- **API Gateway**: Centralized API management
- **Message Queue**: Asynchronous processing
- **Database Integration**: Persistent data storage
- **Real-time Updates**: WebSocket integration

### Extensibility Features
- **Plugin System**: Third-party tool integration
- **Custom Visualizations**: User-defined chart types
- **External Data Sources**: API integrations
- **Multi-language Support**: Internationalization
- **Advanced Analytics**: Machine learning integration

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