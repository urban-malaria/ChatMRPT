# ChatMRPT Development Guide

## Workflow: Explore → Plan → Code → Commit

1. **Explore**: Read relevant files, understand the problem
2. **Plan**: Write plan to `project/planning/todo.md`, get user approval
3. **Code**: Implement with tests, verify it works
4. **Commit**: Descriptive message, deploy if approved

## Verification (CRITICAL)
- Always provide tests or expected outputs for Claude to verify
- Run `python -m pytest tests/` after changes
- Test locally before deploying to AWS
- Health checks: `/ping` and `/system-health`

## Commands
```bash
# Local development
source venv/bin/activate          # Activate venv
python run.py                     # Start server (http://localhost:5013)
python -m pytest tests/           # Run tests

# AWS deployment (ALWAYS deploy to BOTH instances!)
ssh -i /tmp/chatmrpt-key2.pem ec2-user@3.21.167.170   # Instance 1
ssh -i /tmp/chatmrpt-key2.pem ec2-user@18.220.103.20  # Instance 2
sudo systemctl restart chatmrpt                        # Restart service
sudo journalctl -u chatmrpt -f                        # View logs
```

## Git Rules
- **NO Claude signatures** in commits (no "Co-Authored-By", no "Generated with")
- Use conventional commits: `feat:`, `fix:`, `docs:`, `chore:`
- Commit frequently when changes are stable

## Code Style
- Flask blueprints, PEP 8, type hints
- `from module import Class` (not `import module`)
- Max 600-800 lines per file
- **NEVER hardcode** locations, states, or dataset-specific values

## Key Files
- `app/core/request_interpreter.py` - LLM conversation routing
- `app/services/container.py` - Dependency injection
- `app/tools/` - Analysis tools (standard interface)
- `app/config/redis_config.py` - Session management

## Do Not Touch
- `instance/uploads/` - User data
- `data/geospatial/` - Geospatial datasets
- `.env` files - Never commit

## Project Notes
- Document learnings in `project/planning/project_notes/`
- Keep notes under 500 lines each

## References
- @docs/aws-reference.md - AWS infrastructure details
- @docs/architecture/ARCHITECTURE.md - System architecture
- @local_setup_instructions.txt - Local dev setup
