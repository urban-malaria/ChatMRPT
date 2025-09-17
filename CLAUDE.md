# ChatMRPT Development Guide

## Standard Workflow

Use this workflow when working on a new task:

1. First,  think through the problem, read the codebase for relevant files, and write a plan to tasks/todo.md. Make sure to update the todo.md file as you go through the checklist. Follow the plan in the todo.md file you just wrote.
2. The plan should have a list of todo items, that you can check off as you complete them.
3. Before you begin, check in with me and I will verify the plan.
4. Then, begin working on the todo items, marking them as complete as you go.
5. Finally, add a review section to the todo.md file with a summary of the changes you made and any other relevant information.
6. In the plan I also want to make sure we are using the right software engineering practices and scalable coding, modlular as well and ensure that no file you write is more than 600-800 lines.
7. Put all your thoughts, what you learnt, what worked, what didn't, the decisions made for every task in this tasks/prjoect_notes. This is a folder, so ensure you arrange the notes in order that it is easier to find, create markdown files in this folder and do not exceed a 500 lines for each markdown file. This will help when we want to review stuff. This is a typical project notes. So it should follow standard practice. 
8. ALWAYS update tasks/prjoect_notes. frequently.
9. Always read all lines in context.md and terminal_output.md when attached. Never decide to read the first few lines.
10. When you are working on a new task, if the current file can be modified, please do so. Do not create a new file if the current one can be modified to fit what is needed. However, you can also create a new file and then connect it if the current old original file exceeds 600-800 lines. Please follow these instructions. You should not always be in a hurry to create a new file.
11. Please whenever you are testing something industry-standard unit tests using pytest. That would actually test what whats been actually implemented.
Periodically, make sure to commit when it makes sense to do so.

## Tech Stack
- **Framework**: Flask 2.3.3 (Python web framework) with Flask-Login 0.6.3 authentication
- **Python**: 3.10+ with virtual environment at `chatmrpt_venv_new/`
- **Database**: SQLite (development) / PostgreSQL (production) with psycopg2-binary 2.9.9
- **Geospatial**: GeoPandas >=0.14.3, Shapely >=2.0.0, Fiona >=1.10.0, GDAL 3.6.2
- **AI/ML**: OpenAI >=1.50.0, PyTorch >=1.11.0, Transformers >=4.21.0, Sentence-Transformers >=2.2.0, Scikit-learn >=1.0.0
- **Frontend**: Vanilla JS with modular architecture, Tailwind CSS
- **Deployment**: AWS EC2 Multiple Instances behind ALB
- **Authentication**: Flask-Login with session management
- **Session Management**: Redis 5.0.1 for distributed sessions

## Project Structure
- `app/` - Main application code (Flask app factory pattern)
  - `analysis/` - Malaria risk analysis pipelines (PCA, composite scoring)
    - `pipeline_stages/` - Modular pipeline components (data prep, scoring, utils)
  - `auth/` - Authentication system (models, routes)
  - `config/` - Environment configurations (development, production, testing)
  - `core/` - Core utilities (LLM manager, request interpreter, session state, tool registry/cache/validator)
  - `data/` - Data processing and settlement loading
    - `population_data/` - PBI distribution data for Nigerian states
  - `interaction/` - User interaction tracking system (core, events, storage)
  - `models/` - Data models and legacy compatibility layer
  - `services/` - Service layer with specialized agents
    - `agents/visualizations/` - Visualization agents (composite, PCA)
    - `reports/` - Report generation (modern generator, templates)
    - Earth Engine clients, data extractors, viz services
  - `tools/` - Modular analysis tools (16 specialized tools)
  - `data_analysis_v3/` - Advanced data analysis system with LangGraph integration
    - `core/` - Core analysis components (agent, state manager, TPR workflow handler)
    - `formatters/` - Output formatting utilities
    - `prompts/` - System prompts and templates
    - `tools/` - Analysis-specific tools
    - `utils/` - Utility functions
  - `web/` - Web interface organization
    - `routes/` - Blueprint routes (analysis, core, debug, reports, upload, viz)
    - `admin.py` - Admin dashboard functionality
  - `static/js/modules/` - Modular JavaScript components
    - `chat/` - Chat interface modules (analysis, core, visualization)
    - `data/` - Data management modules
    - `ui/` - UI components (sidebar, etc.)
    - `utils/` - Utility modules
  - `templates/` - Jinja2 HTML templates (including error pages)
- `instance/` - Runtime data (uploads, reports, databases, logs)
- `kano_settlement_data/` - Geospatial settlement footprint data (436MB)
- `aws_files/` - AWS deployment files (keys, IP addresses)
- `tests/` - Unit and integration tests
- `run.py` - Application entry point
- `gunicorn.conf.py` - Production server configuration


## Commands
**IMPORTANT: Always use the virtual environment for all Python commands**

**Note: Virtual environment has been upgraded to `chatmrpt_venv_new/` with enhanced packages including full geospatial support, latest AI/ML libraries, and CUDA compatibility.**

- Activate virtual environment: `source chatmrpt_venv_new/bin/activate` (Linux/WSL) or `chatmrpt_venv_new/Scripts/activate` (Windows)
- `python run.py` - Start development server (http://127.0.0.1:5000) 
- `pip install -r requirements.txt` - Install dependencies
- `chmod +x build.sh && ./build.sh` - Production deployment build
- `gunicorn 'run:app' --bind=0.0.0.0:$PORT` - Production server
- Virtual environment path: `chatmrpt_venv_new/` - ALWAYS activate before running Python commands



## Environment Variables
- `OPENAI_API_KEY` - Required for AI functionality
- `FLASK_ENV` - development/production 
- `SECRET_KEY` - Flask session security
- `DATABASE_URL` - PostgreSQL connection (production only)

## Code Style & Best Practices
- Use Flask app factory pattern with blueprints
- Follow PEP 8 Python style guidelines
- ES6+ JavaScript with async/await patterns
- Import style: `from module import Class` (not `import module`)
- Use type hints in Python functions
- Modular architecture - each tool is self-contained
- Error handling with try/except and proper logging

### Git Commit Guidelines
- **NEVER include Claude signatures** in commit messages (no ">", no "Generated with Claude", no "Co-Authored-By: Claude")
- Write clear, concise commit messages focusing on what changed
- Use conventional commit format when applicable (feat:, fix:, docs:, etc.)
- Keep commit messages professional and human-like

### Scalability & Performance Guidelines
- **Tiered Tool Loading**: Use `app/core/tiered_tool_loader.py` for efficient tool loading
- **Session Isolation**: All user data in `instance/uploads/{session_id}/` for multi-user support
- **Eager Loading**: Preload all essential tools and dependencies at startup for optimal performance
- **Memory Management**: Clear session data after analysis completion
- **Caching Strategy**: Cache expensive operations (geospatial processing, AI responses)
- **Database Optimization**: Use indexes on frequently queried columns
- **File Size Limits**: Enforce upload limits to prevent resource exhaustion

### Anti-Hardcoding Policy
- **NEVER hardcode** geographic locations, state names, or dataset-specific values
- **ALWAYS** use dynamic detection and configuration-based approaches
- **REQUIRED**: Ask for explicit permission before hardcoding ANY values
- **Examples of forbidden hardcoding**:
  ```python
  # L FORBIDDEN - Never hardcode locations
  location = "Kano State"
  
  #  CORRECT - Use dynamic detection
  location = data.get('state_name') or detect_state_from_data(data)
  ```
- **Configuration-driven**: Use `app/config/` for environment-specific settings
- **Data-driven**: Extract values from uploaded data rather than assuming
- **Permission-based**: Always ask before adding hardcoded fallbacks

### Code Quality Standards
- **Defensive Programming**: Validate all inputs and handle edge cases
- **Logging Strategy**: Use structured logging with appropriate levels
- **Error Messages**: Provide clear, actionable error messages to users
- **Code Reviews**: All hardcoded values must be justified and approved
- **Documentation**: Document all configuration options and dynamic behaviors
- **Testing**: Write tests that work with multiple datasets, not just one region

### Multi-Worker Session Management
- **Critical Files**: 
  - `app/core/unified_data_state.py` - No singleton pattern, fresh instances per worker
  - `app/core/analysis_state_handler.py` - No singleton pattern
  - `app/core/request_interpreter.py` - File-based session detection for cross-worker compatibility
- **Worker Configuration**: 6 workers configured in `gunicorn_config.py`
- **Session Persistence**: File-based state checking ensures analysis completion is detected across all workers
- **Redis Support**: ElastiCache Redis available for future session management enhancements

## Core Architecture Patterns
- **Service Container**: `app/services/container.py` manages dependency injection
- **Tool System**: Each analysis tool in `app/tools/` follows standard interface
- **Request Interpreter**: `app/core/request_interpreter.py` handles LLM conversation routing
- **Session Management**: Per-user data isolation in `instance/uploads/{session_id}/`
- **Data Pipeline**: Multi-stage analysis in `app/analysis/pipeline.py`

## File Upload Handling
- CSV/Excel: Max 32MB, stored in `instance/uploads/{session_id}/`
- Shapefiles: Uploaded as ZIP, extracted to `shapefile/` subdirectory  
- Settlement data: Large geospatial files in `kano_settlement_data/`
- Generated files: Maps (.html), analysis results (.csv), reports

## Database Schema
- `instance/interactions.db` - User conversations and analysis history
- `instance/agent_memory.db` - AI learning and patterns
- Session-specific CSVs for analysis results and rankings

## Critical Dependencies
- **GDAL/GEOS**: Geospatial processing (GDAL 3.6.2, available via Fiona >=1.10.0)
- **OpenAI**: Required for conversational AI features (>=1.50.0)
- **GeoPandas**: Shapefile and geospatial data processing (>=0.14.3)
- **PyTorch**: AI/ML processing with CUDA support (>=1.11.0)
- **LangChain**: Conversational AI framework (langchain-core >=0.3.0, langchain-openai >=0.2.0)
- **LangGraph**: Advanced workflow orchestration (>=0.2.0)
- **DuckDB**: SQL execution on DataFrames (>=0.10.0)
- **Redis**: Session management across workers (5.0.1)

## Do Not Touch
- Never edit files in `instance/uploads/` manually
- Do not modify `kano_settlement_data/` structure (436MB dataset)
- Do not commit `.env` files or API keys
- Never change database schema without migration planning
- Preserve existing analysis pipeline stages order
- Do not alter settlement type mappings without testing

## Development Workflow Guidelines
- **Code Reviews**: All changes must be reviewed for hardcoding violations
- **Testing Protocol**: Test with multiple datasets from different regions
- **Documentation Updates**: Update CLAUDE.md when adding new patterns or standards
- **Performance Monitoring**: Profile code for scalability bottlenecks
- **Security Checks**: Validate all user inputs and file uploads
- **Deployment Checklist**: Ensure configurations work across environments

## Testing
- Run integration tests: `python -m pytest tests/`
- Health check endpoint: `/ping` and `/system-health`
- Test data uploads using `app/sample_data/` templates

## Deployment
- **Target**: AWS infrastructure with Auto Scaling Group
- **Production Database**: PostgreSQL with persistent storage
- **Build Process**: `build.sh` creates directories and installs system deps
- **Web Server**: Gunicorn with Flask application (6 workers for 50-60 concurrent users)
- **Static Files**: Served through Flask with caching headers
- **Logs**: Rotating file handler in `instance/app.log`
- **Infrastructure**: Scalable for institutional deployment with large geospatial datasets

### AWS Infrastructure Overview
- **Production** (Active Environment): Multiple instances behind ALB
  - Instance 1: `i-0994615951d0b9563` (Public: 3.21.167.170, Private: 172.31.46.84)
  - Instance 2: `i-0f3b25b72f18a5037` (Public: 18.220.103.20, Private: 172.31.24.195)
  - **CRITICAL**: Must deploy to ALL instances or users will experience inconsistent behavior
- **Redis**: AWS ElastiCache for session management across workers
  - Production Redis: `chatmrpt-redis-staging.1b3pmt.0001.use2.cache.amazonaws.com:6379`
- **Access Points**:
  - **Primary URL (CloudFront HTTPS)**: https://d225ar6c86586s.cloudfront.net
  - Production ALB (HTTP): http://chatmrpt-staging-alb-752380251.us-east-2.elb.amazonaws.com
- **Old Infrastructure (DISABLED)**:
  - ~~Old Instance 1: `i-06d3edfcc85a1f1c7` (172.31.44.52)~~ **[STOPPED]**
  - ~~Old Instance 2: `i-0183aaf795bf8f24e` (172.31.43.200)~~ **[STOPPED]**
  - ~~Old ALB: http://chatmrpt-alb-319454030.us-east-2.elb.amazonaws.com~~ **[DO NOT USE]**
  - ~~Old Redis: `chatmrpt-redis-production.1b3pmt.0001.use2.cache.amazonaws.com:6379`~~ **[DO NOT USE]**

### Deployment Best Practices
**ALWAYS deploy to ALL instances in BOTH environments!**

#### Deploy to Production (2 instances)
```bash
# Production is now the former staging environment
./deployment/deploy_to_production.sh
```
This deploys to both production instances:
- Instance 1: 3.21.167.170
- Instance 2: 18.220.103.20

**Manual deployment to ALL production instances**:
```bash
# Production instances (use public IPs)
for ip in 3.21.167.170 18.220.103.20; do
    scp -i ~/.ssh/chatmrpt-key.pem <files> ec2-user@$ip:/home/ec2-user/ChatMRPT/
    ssh -i ~/.ssh/chatmrpt-key.pem ec2-user@$ip 'sudo systemctl restart chatmrpt'
done
```

� **OLD PRODUCTION INSTANCES ARE DISABLED - DO NOT USE**
- ~~172.31.44.52~~ (STOPPED)
- ~~172.31.43.200~~ (STOPPED)

### SSH Access to AWS Instances

#### Prerequisites
- SSH key file: `aws_files/chatmrpt-key.pem`
- Set proper permissions: `chmod 600 aws_files/chatmrpt-key.pem`

#### Production Server Access (formerly Staging)
```bash
# Copy key to /tmp first
cp aws_files/chatmrpt-key.pem /tmp/chatmrpt-key2.pem
chmod 600 /tmp/chatmrpt-key2.pem

# SSH to production instances
# Instance 1:
ssh -i /tmp/chatmrpt-key2.pem ec2-user@3.21.167.170

# Instance 2:
ssh -i /tmp/chatmrpt-key2.pem ec2-user@18.220.103.20
```

**CRITICAL**: Production has MULTIPLE instances behind an Application Load Balancer (ALB)
- Current ACTIVE instances: 
  - Instance 1: `i-0994615951d0b9563` (IP: 3.21.167.170)
  - Instance 2: `i-0f3b25b72f18a5037` (IP: 18.220.103.20)

**IMPORTANT**: Always deploy to ALL production instances to avoid inconsistent behavior!

#### Old Production (DISABLED - DO NOT USE)
- ~~Instance 1: `i-06d3edfcc85a1f1c7` (172.31.44.52)~~ **[STOPPED]**
- ~~Instance 2: `i-0183aaf795bf8f24e` (172.31.43.200)~~ **[STOPPED]**

1. **AWS Systems Manager Session Manager** (Recommended):
   - Go to AWS Console � EC2 � Instances
   - Find ASG instance (e.g., i-06d3edfcc85a1f1c7)
   - Click "Connect" � "Session Manager"

2. **EC2 Instance Connect**:
   - Go to AWS Console � EC2 � Instances  
   - Select instance � "Connect" � "EC2 Instance Connect"

3. **SSH from Staging** (if within VPC):
   ```bash
   # First SSH to staging
   ssh -i /tmp/chatmrpt-key2.pem ec2-user@18.117.115.217
   
   # Copy key to staging server (if not already there)
   scp -i /tmp/chatmrpt-key2.pem aws_files/chatmrpt-key.pem ec2-user@18.117.115.217:~/.ssh/
   ssh -i /tmp/chatmrpt-key2.pem ec2-user@18.117.115.217 'chmod 600 ~/.ssh/chatmrpt-key.pem'
   
   # SSH to production instances
   # Instance 1:
   ssh -i /tmp/chatmrpt-key2.pem ec2-user@18.117.115.217 'ssh -i ~/.ssh/chatmrpt-key.pem ec2-user@172.31.44.52'
   
   # Instance 2:
   ssh -i /tmp/chatmrpt-key2.pem ec2-user@18.117.115.217 'ssh -i ~/.ssh/chatmrpt-key.pem ec2-user@172.31.43.200'
   ```

#### Common AWS Operations
```bash
# Check service status
sudo systemctl status chatmrpt

# View logs
sudo journalctl -u chatmrpt -f

# Restart service
sudo systemctl restart chatmrpt

# Check worker count
ps aux | grep gunicorn | grep -v grep | wc -l

# Monitor resources
htop
```

#### AWS Backups (Last Updated: 2025-09-17)
- **Current Stable Backups**:
  - `ChatMRPT_stable_survey_20250917_112835.tar.gz` - Latest stable with survey button fix and complete survey module
  - `ChatMRPT_backup_ITN_fixes_20250916_163329.tar.gz` - Instance 1 (ITN distribution fixes)
  - `ChatMRPT_backup_ITN_fixes_20250916_163556.tar.gz` - Instance 2 (ITN distribution fixes)
  - `ChatMRPT_COMPLETE_BACKUP_20250828.tar.gz` - Complete system backup (2.7GB)
- **Latest Stable Features** (as of 2025-09-17):
  - ✅ Survey module fully integrated with cognitive assessment questions
  - ✅ Survey button correctly positioned at end of navigation
  - ✅ CloudFront cache invalidation completed
  - ✅ Both production instances synchronized
  - ✅ All TPR analysis fixes and Word document generation
- **Backup Strategy**: Keep 1-2 most recent stable backups per instance
- **Backup Location**: `/home/ec2-user/` on each instance
- **Backup Command**:
  ```bash
  tar -czf ChatMRPT_backup_$(date +%Y%m%d_%H%M%S).tar.gz ChatMRPT/ \
    --exclude="ChatMRPT/instance/uploads/*" \
    --exclude="ChatMRPT/chatmrpt_venv*" \
    --exclude="ChatMRPT/venv*" \
    --exclude="ChatMRPT/__pycache__" \
    --exclude="*.pyc"
  ```
- **Restore Command**:
  ```bash
  # To restore from backup
  tar -xzf ChatMRPT_stable_survey_20250917_112835.tar.gz
  sudo systemctl restart chatmrpt
  ```

## Malaria Domain Context
- **Primary Use**: Epidemiological risk assessment for malaria intervention targeting
- **Data Types**: Ward-level demographic, environmental, and health indicators
- **Analysis Methods**: Composite scoring, PCA, vulnerability ranking
- **Output**: Interactive maps, risk rankings, intervention recommendations
- **Geographic Focus**: Nigerian states (Kano reference implementation)

## Session Data Flow
1. User uploads CSV (demographic) + shapefile (boundaries)
2. Data validation and cleaning in `app/data/processing.py`
3. Analysis pipeline: normalize � score � rank � visualize
4. Results stored as session-specific files in `instance/uploads/{session_id}/`
5. Interactive maps generated and served via `/serve_viz_file/` route

## Common File Patterns
- Analysis results: `analysis_*.csv` 
- Visualizations: `*_map_*.html`, `*_chart_*.html`
- Raw uploads: `raw_data.csv`, `raw_shapefile.shp`
- Settlement maps: `building_classification_map_*.html`



CloudFront is fully deployed! You can now access ChatMRPT through:
  - CloudFront CDN: https://d225ar6c86586s.cloudfront.net
  - Production ALB: http://chatmrpt-staging-alb-752380251.us-east-2.elb.amazonaws.com
# important-instruction-reminders
Do what has been asked; nothing more, nothing less.
NEVER create files unless they're absolutely necessary for achieving your goal.
ALWAYS prefer editing an existing file to creating a new one.
NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.





For planning use this:
Planning Task — Next Steps

Now that the investigation findings are available, create a clear plan to address the issues.

Instructions:

Review the findings carefully.

For each issue, propose a fix.

Organize the fixes into a logical sequence (what to do first, second, etc.).

Keep the plan simple, actionable, and directly tied to the investigation results.

Deliverable:
A step-by-step plan that we can follow to resolve the issues.