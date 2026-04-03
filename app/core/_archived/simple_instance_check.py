"""
Simple instance check for Data Analysis V3
Checks if session exists on current instance or needs to be pulled
"""

import os
import subprocess
import logging

logger = logging.getLogger(__name__)

def check_and_sync_session(session_id: str) -> bool:
    """
    Check if session exists locally, if not try to pull from other instance.
    Simplified version without complex sync logic.
    """
    local_dir = f'/home/ec2-user/ChatMRPT/instance/uploads/{session_id}'
    flag_file = f'{local_dir}/.data_analysis_mode'
    
    # Check if already exists locally
    if os.path.exists(flag_file):
        return True
    
    # Get current hostname to determine which instance we're on
    import socket
    hostname = socket.gethostname()
    
    # Determine the OTHER instance IP
    if '46-84' in hostname or '46.84' in hostname:
        # We're on instance 1, check instance 2
        other_ip = '172.31.24.195'
    else:
        # We're on instance 2, check instance 1
        other_ip = '172.31.46.84'
    
    logger.info(f"🔍 Session {session_id} not found locally, checking {other_ip}")
    
    try:
        # Check if exists on other instance
        check_cmd = f'ssh -i ~/.ssh/chatmrpt-key.pem -o StrictHostKeyChecking=no -o ConnectTimeout=3 ec2-user@{other_ip} "test -d /home/ec2-user/ChatMRPT/instance/uploads/{session_id} && echo EXISTS"'
        
        result = subprocess.run(check_cmd, shell=True, capture_output=True, timeout=5)
        
        if b'EXISTS' in result.stdout:
            logger.info(f"📁 Found on {other_ip}, copying...")
            
            # Create local directory
            os.makedirs(local_dir, exist_ok=True)
            
            # Copy files from other instance
            copy_cmd = f'scp -r -i ~/.ssh/chatmrpt-key.pem -o StrictHostKeyChecking=no ec2-user@{other_ip}:/home/ec2-user/ChatMRPT/instance/uploads/{session_id}/* {local_dir}/'
            
            copy_result = subprocess.run(copy_cmd, shell=True, capture_output=True, timeout=10)
            
            if copy_result.returncode == 0:
                logger.info(f"✅ Copied session {session_id} from {other_ip}")
                return os.path.exists(flag_file)
            else:
                logger.error(f"Failed to copy: {copy_result.stderr.decode()}")
    except Exception as e:
        logger.error(f"Error checking other instance: {e}")
    
    return False