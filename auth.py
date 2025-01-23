from flask_jwt_extended import jwt_required, get_jwt_identity, get_jwt
from functools import wraps
from flask import jsonify
from flask import Flask, request, jsonify
import jwt
from functools import wraps
from dotenv import load_dotenv
import logging
import os
# def token_required(f):
#     @wraps(f)
#     @jwt_required()  # This ensures the token is valid
#     def decorated(self, *args, **kwargs):
#         current_user_id = get_jwt_identity()  # Fetch the user identity from the token
#         token = get_jwt()
#         if not current_user_id:
#             return jsonify({'message': 'Token is invalid!'}), 403

#         # Pass current_user_id to the decorated function
#         return f(self, current_user_id, token, *args, **kwargs)
    
#     return decorated

load_dotenv()

# JWT Secret Key
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
        
        # Pass current_user_id and maintain other args
        return f(self, current_user_id, token, *args, **kwargs)
    return decorated
