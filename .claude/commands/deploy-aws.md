---
description: Deploy code to AWS production instances
---

Deploy the current code to AWS production instances.

## Steps:

1. **Pre-flight checks**:
   - Verify local tests pass: `python -m pytest tests/ -x`
   - Check git status is clean or changes are committed
   - Confirm user wants to deploy

2. **Setup SSH key** (if needed):
   ```bash
   cp infrastructure/aws/aws_files/chatmrpt-key.pem /tmp/chatmrpt-key2.pem
   chmod 600 /tmp/chatmrpt-key2.pem
   ```

3. **Deploy to BOTH instances**:
   ```bash
   for ip in 3.21.167.170 18.220.103.20; do
       echo "Deploying to $ip..."
       ssh -i /tmp/chatmrpt-key2.pem ec2-user@$ip 'cd /home/ec2-user/ChatMRPT && git pull origin main && sudo systemctl restart chatmrpt'
   done
   ```

4. **Verify deployment**:
   - Check service status on both instances
   - Test health endpoints
   - Report success/failure

## Arguments:
- `$ARGUMENTS` - Optional: specific branch or commit to deploy

## Important:
- ALWAYS deploy to BOTH instances
- Never deploy untested code
- Check logs if service fails to start
