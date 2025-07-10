# ChatMRPT Gunicorn Configuration for AWS Deployment
import os
import multiprocessing

# Server socket
bind = "0.0.0.0:5000"
backlog = 2048

# Worker processes
workers = int(os.environ.get('GUNICORN_WORKERS', multiprocessing.cpu_count() * 2 + 1))
worker_class = "sync"
worker_connections = 1000
max_requests = int(os.environ.get('GUNICORN_MAX_REQUESTS', 1000))
max_requests_jitter = int(os.environ.get('GUNICORN_MAX_REQUESTS_JITTER', 50))
preload_app = True

# Timeout
timeout = int(os.environ.get('GUNICORN_TIMEOUT', 300))  # 5 minutes for long analyses
keepalive = int(os.environ.get('GUNICORN_KEEPALIVE', 2))
graceful_timeout = 30

# Security
limit_request_line = 4094
limit_request_fields = 100
limit_request_field_size = 8190

# Logging
accesslog = "-"
errorlog = "-"
loglevel = os.environ.get('LOG_LEVEL', 'info').lower()
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

# Process naming
proc_name = 'chatmrpt'

# Server mechanics
daemon = False
pidfile = '/tmp/gunicorn.pid'
user = None
group = None
tmp_upload_dir = '/tmp'

# SSL (if needed)
# keyfile = None
# certfile = None

# Environment
raw_env = [
    f'PYTHONPATH={os.environ.get("PYTHONPATH", "/app")}',
    f'FLASK_ENV={os.environ.get("FLASK_ENV", "production")}',
]

# Startup/Shutdown hooks
def on_starting(server):
    """Called just before the master process is initialized."""
    server.log.info("Starting ChatMRPT server...")

def on_reload(server):
    """Called to recycle workers during a reload via SIGHUP."""
    server.log.info("Reloading ChatMRPT server...")

def when_ready(server):
    """Called just after the server is started."""
    server.log.info("ChatMRPT server is ready. Listening on %s", server.address)

def on_exit(server):
    """Called just before exiting."""
    server.log.info("Shutting down ChatMRPT server...")

def worker_int(worker):
    """Called when a worker receives the SIGINT or SIGQUIT signal."""
    worker.log.info("Worker %s received SIGINT/SIGQUIT signal", worker.pid)

def pre_fork(server, worker):
    """Called just before a worker is forked."""
    server.log.info("Worker %s spawned", worker.pid)

def post_fork(server, worker):
    """Called just after a worker has been forked."""
    server.log.info("Worker %s initialized", worker.pid)

def post_worker_init(worker):
    """Called just after a worker has initialized the application."""
    worker.log.info("Worker %s ready to handle requests", worker.pid)

def worker_abort(worker):
    """Called when a worker receives the SIGABRT signal."""
    worker.log.info("Worker %s aborted", worker.pid)