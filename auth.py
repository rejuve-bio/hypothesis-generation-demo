from flask import Flask, request, jsonify
import jwt
from functools import wraps
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from uuid import uuid4
# Load environment variables from .env file
load_dotenv()

# JWT Secret Key
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")

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
            
            data = jwt.decode(token, JWT_SECRET_KEY, algorithms=["HS256"])
            current_user_id = data['user_id']
        except Exception as e:
            return {'message': 'Token is invalid!'}, 403
        
        # Pass current_user_id and maintain other args
        return f(self, current_user_id, *args, **kwargs)
    return decorated