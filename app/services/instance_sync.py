"""
Instance Sync Module for Data Analysis V3
Syncs uploaded files between instances using rsync
"""

import os
import subprocess
import logging
import json
from typing import Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)

class InstanceSync:
    """
    Sync files between multiple instances.
    This is a workaround for the lack of shared storage.
    """
    
    def __init__(self):
        """Initialize target instance list for cross-instance syncing.

        Priority order:
        1) CHATMRPT_INSTANCE_IPS env var (comma-separated private IPs)
        2) AWS ELB target group discovery if CHATMRPT_TARGET_GROUP_ARN env var is set and awscli available
        3) Safe defaults (current active pair) 172.31.46.84, 172.31.24.195
        """
        # SSH key path
        self.ssh_key = '/home/ec2-user/.ssh/chatmrpt-key.pem'

        # 1) Static env override
        env_ips = os.environ.get('CHATMRPT_INSTANCE_IPS')
        if env_ips:
            self.instances = [ip.strip() for ip in env_ips.split(',') if ip.strip()]
        else:
            # 2) Try AWS ELB discovery
            target_group_arn = os.environ.get('CHATMRPT_TARGET_GROUP_ARN')
            discovered_ips = []
            if target_group_arn:
                try:
                    import subprocess, json
                    # Describe target health to get instance IDs
                    th = subprocess.check_output([
                        'aws', 'elbv2', 'describe-target-health',
                        '--region', os.environ.get('AWS_REGION', 'us-east-2'),
                        '--target-group-arn', target_group_arn
                    ], timeout=5)
                    thj = json.loads(th.decode())
                    instance_ids = [d['Target']['Id'] for d in thj.get('TargetHealthDescriptions', [])
                                    if d.get('TargetHealth', {}).get('State') == 'healthy']
                    # Describe instances to get PrivateIpAddress
                    if instance_ids:
                        di = subprocess.check_output([
                            'aws', 'ec2', 'describe-instances',
                            '--region', os.environ.get('AWS_REGION', 'us-east-2'),
                            '--instance-ids', *instance_ids
                        ], timeout=5)
                        dij = json.loads(di.decode())
                        for res in dij.get('Reservations', []):
                            for inst in res.get('Instances', []):
                                ip = inst.get('PrivateIpAddress')
                                if ip:
                                    discovered_ips.append(ip)
                except Exception:
                    discovered_ips = []

            # 3) Fallback defaults
            if discovered_ips:
                self.instances = discovered_ips
            else:
                # Current active pair (from CLAUDE.md)
                self.instances = ['172.31.46.84', '172.31.24.195']
        
    def get_current_instance_ip(self) -> Optional[str]:
        """Get current instance's internal IP."""
        try:
            import requests
            # Use instance metadata service
            token = requests.put(
                'http://169.254.169.254/latest/api/token',
                headers={'X-aws-ec2-metadata-token-ttl-seconds': '21600'},
                timeout=1
            ).text
            
            ip = requests.get(
                'http://169.254.169.254/latest/meta-data/local-ipv4',
                headers={'X-aws-ec2-metadata-token': token},
                timeout=1
            ).text
            
            return ip
        except:
            return None
    
    def sync_session_to_all_instances(self, session_id: str) -> bool:
        """
        Sync a session's files to all other instances.
        
        Args:
            session_id: Session ID to sync
            
        Returns:
            True if sync successful to all instances
        """
        current_ip = self.get_current_instance_ip()
        if not current_ip:
            logger.warning("Could not determine current instance IP")
            return False
        
        source_dir = f'/home/ec2-user/ChatMRPT/instance/uploads/{session_id}/'
        
        if not os.path.exists(source_dir):
            logger.warning(f"Source directory does not exist: {source_dir}")
            return False
        
        success = True
        
        for instance_ip in self.instances:
            if instance_ip == current_ip:
                continue  # Skip current instance
            
            try:
                # Use rsync to sync the directory
                dest = f'ec2-user@{instance_ip}:/home/ec2-user/ChatMRPT/instance/uploads/{session_id}/'
                
                # Create directory on remote first
                mkdir_cmd = [
                    'ssh',
                    '-i', self.ssh_key,
                    '-o', 'StrictHostKeyChecking=no',
                    '-o', 'ConnectTimeout=5',
                    f'ec2-user@{instance_ip}',
                    f'mkdir -p /home/ec2-user/ChatMRPT/instance/uploads/{session_id}'
                ]
                
                subprocess.run(mkdir_cmd, capture_output=True, timeout=10)
                
                # Rsync the files
                rsync_cmd = [
                    'rsync',
                    '-avz',
                    '--timeout=10',
                    '-e', f'ssh -i {self.ssh_key} -o StrictHostKeyChecking=no',
                    source_dir,
                    dest
                ]
                
                result = subprocess.run(rsync_cmd, capture_output=True, timeout=30)
                
                if result.returncode == 0:
                    logger.info(f"✅ Synced session {session_id} to {instance_ip}")
                else:
                    logger.error(f"❌ Failed to sync to {instance_ip}: {result.stderr.decode()}")
                    success = False
                    
            except subprocess.TimeoutExpired:
                logger.error(f"❌ Timeout syncing to {instance_ip}")
                success = False
            except Exception as e:
                logger.error(f"❌ Error syncing to {instance_ip}: {e}")
                success = False
        
        return success
    
    def check_session_on_any_instance(self, session_id: str) -> Optional[str]:
        """
        Check if session exists on any instance and sync if needed.
        
        Args:
            session_id: Session ID to check
            
        Returns:
            Instance IP where session was found, or None
        """
        # First check locally
        local_dir = f'/home/ec2-user/ChatMRPT/instance/uploads/{session_id}'
        if os.path.exists(local_dir) and os.path.exists(f'{local_dir}/.data_analysis_mode'):
            return 'local'
        
        current_ip = self.get_current_instance_ip()
        
        # Check other instances
        for instance_ip in self.instances:
            if instance_ip == current_ip:
                continue
                
            try:
                # Check if directory exists on remote
                check_cmd = [
                    'ssh',
                    '-i', self.ssh_key,
                    '-o', 'StrictHostKeyChecking=no',
                    '-o', 'ConnectTimeout=5',
                    f'ec2-user@{instance_ip}',
                    f'test -f /home/ec2-user/ChatMRPT/instance/uploads/{session_id}/.data_analysis_mode && echo "EXISTS"'
                ]
                
                result = subprocess.run(check_cmd, capture_output=True, timeout=10)
                
                if b'EXISTS' in result.stdout:
                    logger.info(f"📁 Found session {session_id} on {instance_ip}, syncing...")
                    
                    # Sync from that instance to local
                    source = f'ec2-user@{instance_ip}:/home/ec2-user/ChatMRPT/instance/uploads/{session_id}/'
                    dest = f'/home/ec2-user/ChatMRPT/instance/uploads/'
                    
                    os.makedirs(f'/home/ec2-user/ChatMRPT/instance/uploads/{session_id}', exist_ok=True)
                    
                    rsync_cmd = [
                        'rsync',
                        '-avz',
                        '--timeout=10',
                        '-e', f'ssh -i {self.ssh_key} -o StrictHostKeyChecking=no',
                        source,
                        dest
                    ]
                    
                    sync_result = subprocess.run(rsync_cmd, capture_output=True, timeout=30)
                    
                    if sync_result.returncode == 0:
                        logger.info(f"✅ Synced session {session_id} from {instance_ip}")
                        return instance_ip
                    
            except Exception as e:
                logger.error(f"Error checking {instance_ip}: {e}")
        
        return None

# Singleton instance
_instance_sync = None

def get_instance_sync() -> InstanceSync:
    """Get or create instance sync."""
    global _instance_sync
    if not _instance_sync:
        _instance_sync = InstanceSync()
    return _instance_sync

def sync_session_after_upload(session_id: str):
    """
    Helper function to sync session after upload.
    Called from upload route.
    """
    try:
        sync = get_instance_sync()
        sync.sync_session_to_all_instances(session_id)
    except Exception as e:
        logger.error(f"Failed to sync session: {e}")

def ensure_session_available(session_id: str) -> bool:
    """
    Ensure session data is available locally.
    Syncs from other instances if needed.
    
    Args:
        session_id: Session ID to ensure
        
    Returns:
        True if session is available (locally or after sync)
    """
    try:
        sync = get_instance_sync()
        result = sync.check_session_on_any_instance(session_id)
        return result is not None
    except Exception as e:
        logger.error(f"Failed to ensure session: {e}")
        return False
