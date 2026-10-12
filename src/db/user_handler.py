from werkzeug.security import generate_password_hash, check_password_hash
from .base_handler import BaseHandler


class UserHandler(BaseHandler):
    """Handler for user authentication and management operations"""
    
    def __init__(self, uri, db_name):
        super().__init__(uri, db_name)
        self.users_collection = self.db['users']
    
    def create_user(self, email, password):
        """Create a new user"""
        if self.users_collection.find_one({'email': email}):
            return {'message': 'User already exists'}, 400
        
        hashed_password = generate_password_hash(password)
        self.users_collection.insert_one({'email': email, 'password': hashed_password})
        return {'message': 'User created successfully'}, 201

    def verify_user(self, email, password):
        """Verify user credentials"""
        user = self.users_collection.find_one({'email': email})
        if user and check_password_hash(user['password'], password):
            return {'message': 'Logged in successfully', 'user_id': str(user['_id'])}, 200
        return {'message': 'Invalid credentials'}, 401