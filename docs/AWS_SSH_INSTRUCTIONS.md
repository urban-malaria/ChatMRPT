# AWS SSH Instructions

## Connection Details

| Field | Value |
|-------|-------|
| Host | `3.145.108.184` |
| User | `ec2-user` |
| Key File | `/tmp/chatmrpt-key.pem` |
| Project Path | `/home/ec2-user/ChatMRPT` |

## Setup (One-time)

The SSH key needs to be created from the stored content:

```bash
# Key content is stored in: aws_github_ssh_key.txt (GitHub deploy key, not SSH login key)
# The actual SSH login key should be provided by the user or already exist at /tmp/chatmrpt-key.pem
chmod 600 /tmp/chatmrpt-key.pem
```

## Connect to AWS

```bash
ssh -i /tmp/chatmrpt-key.pem -o StrictHostKeyChecking=no ec2-user@3.145.108.184
```

## Common Commands After Connecting

### Navigate to project
```bash
cd /home/ec2-user/ChatMRPT
```

### Check git status
```bash
git status
git branch
```

### Pull latest changes
```bash
git fetch origin
git pull origin main
```

### Switch to unified-agent-v2 branch
```bash
git checkout unified-agent-v2
git pull origin unified-agent-v2
```

### Check application logs
```bash
tail -f logs/chatmrpt.log
```

### Restart application
```bash
sudo systemctl restart chatmrpt
# OR if using pm2:
pm2 restart all
# OR manual:
pkill -f gunicorn && gunicorn --bind 0.0.0.0:5000 app:app &
```

## Troubleshooting

### SSH Connection Timeout
- Instance may be stopped - check AWS Console
- IP may have changed - get new IP from EC2 > Instances

### Permission Denied
- Check key file permissions: `chmod 600 /tmp/chatmrpt-key.pem`
- Verify correct key file is being used

### Host Key Verification Failed
```bash
ssh-keyscan 3.145.108.184 >> ~/.ssh/known_hosts
```
