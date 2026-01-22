# AWS Infrastructure Reference

## Production Environment

### Active Instances (behind ALB)
| Instance | ID | Public IP | Private IP |
|----------|-----|-----------|------------|
| Instance 1 | i-0994615951d0b9563 | 3.21.167.170 | 172.31.46.84 |
| Instance 2 | i-0f3b25b72f18a5037 | 18.220.103.20 | 172.31.24.195 |

### Access Points
- **CloudFront (HTTPS)**: https://d225ar6c86586s.cloudfront.net
- **ALB (HTTP)**: http://chatmrpt-staging-alb-752380251.us-east-2.elb.amazonaws.com

### Redis (ElastiCache)
- **Endpoint**: `chatmrpt-redis-staging.1b3pmt.0001.use2.cache.amazonaws.com:6379`

## SSH Access

```bash
# Copy key to /tmp (required for WSL)
cp infrastructure/aws/aws_files/chatmrpt-key.pem /tmp/chatmrpt-key2.pem
chmod 600 /tmp/chatmrpt-key2.pem

# Instance 1
ssh -i /tmp/chatmrpt-key2.pem ec2-user@3.21.167.170

# Instance 2
ssh -i /tmp/chatmrpt-key2.pem ec2-user@18.220.103.20
```

## Common Operations

```bash
# Service management
sudo systemctl status chatmrpt
sudo systemctl restart chatmrpt
sudo systemctl stop chatmrpt

# View logs
sudo journalctl -u chatmrpt -f
sudo journalctl -u chatmrpt -n 100

# Check workers
ps aux | grep gunicorn | grep -v grep | wc -l

# Disk space
df -h /home/ec2-user
```

## Deployment

**CRITICAL: Always deploy to BOTH instances!**

```bash
# Quick deploy to both instances
for ip in 3.21.167.170 18.220.103.20; do
    ssh -i /tmp/chatmrpt-key2.pem ec2-user@$ip 'cd /home/ec2-user/ChatMRPT && git pull origin main && sudo systemctl restart chatmrpt'
done
```

## Backup System

### S3 Bucket
- **Bucket**: `chatmrpt-backups-20250728`
- **Daily backups**: `s3://chatmrpt-backups-20250728/staging/`
- **Full backups**: `s3://chatmrpt-backups-20250728/production/full_backups/`

### Automated Daily Backups
- **Schedule**: 2:00 AM UTC daily (cron)
- **Script**: `/home/ec2-user/backup-chatmrpt.sh`
- **Retention**: 7 days local, unlimited S3

### Manual Backup
```bash
BACKUP_DATE=$(date +%Y%m%d_%H%M%S)

# Backup database
sqlite3 /home/ec2-user/ChatMRPT/instance/interactions.db \
  ".backup /home/ec2-user/interactions_backup_${BACKUP_DATE}.db"

# Upload to S3
aws s3 cp /home/ec2-user/interactions_backup_${BACKUP_DATE}.db \
  s3://chatmrpt-backups-20250728/production/full_backups/
```

### Restore from Backup
```bash
# Download
aws s3 cp s3://chatmrpt-backups-20250728/production/full_backups/BACKUP_FILE .

# Restore
cp BACKUP_FILE /home/ec2-user/ChatMRPT/instance/interactions.db
sudo systemctl restart chatmrpt
```

## Old Infrastructure (DISABLED)
- ~~Instance 1: i-06d3edfcc85a1f1c7 (172.31.44.52)~~ **[STOPPED]**
- ~~Instance 2: i-0183aaf795bf8f24e (172.31.43.200)~~ **[STOPPED]**
- ~~Old ALB: chatmrpt-alb-319454030~~ **[DO NOT USE]**

## Configuration

### Gunicorn
- Workers: 6
- Config: `/home/ec2-user/ChatMRPT/gunicorn_config.py`
- Logs: `/home/ec2-user/ChatMRPT/instance/logs/`

### Environment
- `.env` on each instance (not in git)
- `FLASK_ENV=production`
- `REDIS_HOST=chatmrpt-redis-staging.1b3pmt.0001.use2.cache.amazonaws.com`
