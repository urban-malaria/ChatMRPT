---
description: Check sync status between local, GitHub, and AWS
---

Compare the state of local, GitHub, and AWS environments.

## Checks to perform:

### 1. Git Commit Status
```bash
# Local
git log --oneline -1

# AWS Instance 1
ssh -i /tmp/chatmrpt-key2.pem ec2-user@3.21.167.170 'cd /home/ec2-user/ChatMRPT && git log --oneline -1'

# AWS Instance 2
ssh -i /tmp/chatmrpt-key2.pem ec2-user@18.220.103.20 'cd /home/ec2-user/ChatMRPT && git log --oneline -1'
```

### 2. Frontend Build Status
Compare built assets between local and AWS:
- Check `app/static/assets/` for React build files
- Compare file sizes and timestamps

### 3. Backend Packages
```bash
# Local
pip freeze | wc -l

# AWS
ssh -i /tmp/chatmrpt-key2.pem ec2-user@3.21.167.170 'source /home/ec2-user/chatmrpt_env/bin/activate && pip freeze | wc -l'
```

### 4. Configuration Differences
Compare `.env` settings (excluding secrets):
- FLASK_ENV
- REDIS_HOST
- Other non-sensitive settings

### 5. Service Status
```bash
# Check both instances
for ip in 3.21.167.170 18.220.103.20; do
    echo "=== $ip ==="
    ssh -i /tmp/chatmrpt-key2.pem ec2-user@$ip 'sudo systemctl status chatmrpt | head -5'
done
```

## Output:
Create a summary table showing:
| Component | Local | AWS Instance 1 | AWS Instance 2 | Status |
|-----------|-------|----------------|----------------|--------|

Report any differences that need attention.
