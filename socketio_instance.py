from flask import json
from flask_socketio import SocketIO

socketio = SocketIO(cors_allowed_origins="*", json=json, engineio_logger=True, logger=True, async_mode="threading", ping_interval=300, ping_timeout=300)