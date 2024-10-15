from flask_jwt_extended import jwt_required, get_jwt_identity
from functools import wraps
from flask import jsonify

def token_required(f):
    @wraps(f)
    @jwt_required()  # This ensures the token is valid
    def decorated(self, *args, **kwargs):
        current_user_id = get_jwt_identity()  # Fetch the user identity from the token
        if not current_user_id:
            return jsonify({'message': 'Token is invalid!'}), 403

        # Pass current_user_id to the decorated function
        return f(self, current_user_id, *args, **kwargs)
    
    return decorated
