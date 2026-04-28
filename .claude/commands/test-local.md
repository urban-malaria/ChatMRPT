---
description: Run local tests and verify the application works
---

Run comprehensive local tests before deploying.

## Steps:

### 1. Environment Check
```bash
# Verify virtual environment is active
which python
python --version

# Check .env exists
test -f .env && echo ".env exists" || echo "WARNING: .env missing"
```

### 2. Run Unit Tests
```bash
python -m pytest tests/ -v --tb=short
```

### 3. Run Specific Test Categories
```bash
# TPR tests
python -m pytest tests/tpr/ -v

# ITN tests
python -m pytest tests/itn/ -v

# Integration tests
python -m pytest tests/integration/ -v
```

### 4. Start Local Server (manual verification)
```bash
python run.py
```
- Verify server starts without errors
- Check http://localhost:5013 loads
- Test health endpoint: http://localhost:5013/ping

### 5. Quick Smoke Test
```bash
curl -s http://localhost:5013/ping | grep -q "pong" && echo "Health check passed" || echo "Health check failed"
```

## Arguments:
- `$ARGUMENTS` - Optional: specific test file or pattern to run

## Success Criteria:
- All tests pass
- Server starts without errors
- Health endpoint responds
- No critical warnings in logs
