from flask import json
from flask_socketio import SocketIO

socketio = SocketIO(
    cors_allowed_origins="*", 
    json=json, 
    engineio_logger=False,  
    logger=False,           
    async_mode="threading", 
    ping_interval=25,       
    ping_timeout=20,        
    transports=['polling', 'websocket'],
    allow_upgrades=True,
    max_http_buffer_size=1000000,
    always_connect=False,   
    cookie=None,           
    manage_session=False,
    # Try to prevent the write() before start_response error
    http_compression=False, # Disable HTTP compression
    compression=False      # Disable compression
)