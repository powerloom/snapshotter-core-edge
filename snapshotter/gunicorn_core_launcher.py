import os

from snapshotter.core_api import app
from snapshotter.settings.config import settings
from snapshotter.utils.gunicorn import StandaloneApplication
from snapshotter.utils.gunicorn import StubbedGunicornLogger

WORKERS = int(os.environ.get('GUNICORN_WORKERS', '5'))
JSON_LOGS = True if os.environ.get('JSON_LOGS', '0') == '1' else False

if __name__ == '__main__':
    # In Docker, always bind to 0.0.0.0 to accept connections from all interfaces
    # This ensures nginx and other containers can connect
    bind_host = settings.core_api.host or os.environ.get('CORE_API_HOST', '0.0.0.0')
    bind_port = settings.core_api.port

    options = {
        'bind': f'{bind_host}:{bind_port}',
        'workers': WORKERS,
        'accesslog': '-',
        'errorlog': '-',
        'worker_class': 'uvicorn.workers.UvicornWorker',
        'logger_class': StubbedGunicornLogger,
        'timeout': int(os.environ.get('GUNICORN_TIMEOUT', '30')),  # 120s default for slow IPFS/Redis operations
        'keepalive': int(os.environ.get('GUNICORN_KEEPALIVE', '5')),  # 5s keepalive for nginx connections
        'graceful_timeout': int(os.environ.get('GUNICORN_GRACEFUL_TIMEOUT', '30')),  # Graceful shutdown timeout
    }

    StandaloneApplication(app, options).run()
