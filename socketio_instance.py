from flask_socketio import SocketIO

socketio = SocketIO(cors_allowed_origins="*", async_mode="eventlet", ping_interval=300, ping_timeout=300)
