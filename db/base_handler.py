from pymongo import MongoClient
from loguru import logger


class BaseHandler:
    """Base handler class with common MongoDB operations"""
    
    def __init__(self, uri, db_name):
        self.uri = uri
        self.db_name = db_name
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
    
    def _serialize_object_id(self, doc):
        """Convert ObjectId to string in a document"""
        if doc and '_id' in doc:
            doc['_id'] = str(doc['_id'])
        return doc
    
    def _serialize_object_ids(self, docs):
        """Convert ObjectId to string in a list of documents"""
        for doc in docs:
            self._serialize_object_id(doc)
        return docs