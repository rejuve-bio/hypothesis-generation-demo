from flask_jwt_extended import jwt_required, get_jwt_identity, get_jwt
from functools import wraps
from flask import jsonify
from flask import Flask, request, jsonify
from flask_socketio import disconnect
import jwt
from functools import wraps
from dotenv import load_dotenv
import logging
import os
from flask import request

load_dotenv()

JWT_SECRET = os.getenv("JWT_SECRET")

def token_required(f):
    @wraps(f)
    def decorated(self, *args, **kwargs):
        token = request.headers.get('Authorization')
        if not token:
            return jsonify({'message': 'Token is missing!'}), 403
        
        try:
            # Remove 'Bearer' prefix if present
            if 'Bearer' in token:
                token = token.split()[1]
            
            data = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
            current_user_id = data['user_id']
        except Exception as e:
            logging.error(f"Error docodcing token: {e}")
            return {'message': 'Token is invalid!'}, 403
        
        if 'hypothesis_id' in kwargs:
            # For HypothesisResultAPI
            return f(self, kwargs['hypothesis_id'], current_user_id)
        else:
            # For other endpoints
            return f(self, current_user_id, *args, **kwargs)
        
        # return f(self, current_user_id, *args, **kwargs)
    return decorated

def socket_token_required(f):
    @wraps(f)
    def decorated(self, *args, **kwargs):
        logging.info(f"Checking token for {f.__name__}")
        try:
            # Get auth token from connection args
            auth_header = None
            # Try to get from handshake auth
            if hasattr(request, 'args'):
                auth_header = request.args.get('token')
                logging.info(f"Token from args: {auth_header[:10] if auth_header else None}")

            # Try to get from headers
            if not auth_header and hasattr(request, 'headers'):
                auth_header = request.headers.get('Authorization')
                logging.info(f"Token from headers: {auth_header[:10] if auth_header else None}")

            # Try to get from socket environment
            if not auth_header and hasattr(self, 'server'):
                client_sid = args[0] if args else None
                logging.info(f"Client SID: {client_sid}")
                if client_sid:
                    environ = self.server.get_client(client_sid).environ
                    auth_header = environ.get('HTTP_AUTHORIZATION')
                    logging.info(f"Token from environ: {auth_header[:10] if auth_header else None}")
            
            if not auth_header:
                logging.error("No token found in any location")
                disconnect()
                return False
            
            if 'Bearer' in auth_header:
                token = auth_header.split()[1]
            else:
                token = auth_header
            
            try:
                data = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
                current_user_id = data['user_id']
                logging.info(f"Token decoded successfully for user: {current_user_id}")
            except Exception as e:
                logging.error(f"Token decode error: {e}")
                disconnect()
                return False
            
            return f(self, *args, current_user_id=current_user_id, **kwargs)
            
        except Exception as e:
            logging.error(f"Socket auth error in {f.__name__}: {str(e)}")
            disconnect()
            return False
    return decorated