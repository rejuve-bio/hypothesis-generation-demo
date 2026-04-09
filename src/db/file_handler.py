from bson.objectid import ObjectId
from datetime import datetime, timezone
from .base_handler import BaseHandler


class FileHandler(BaseHandler):
    """Handler for file metadata management"""
    
    def __init__(self, uri, db_name):
        super().__init__(uri, db_name)
        self.file_metadata_collection = self.db['file_metadata']
    
    def create_file_metadata(self, user_id, filename, original_filename, file_path, file_type, file_size, md5_hash=None, record_count=None, download_url=None, storage_key=None, source=None):
        """Create file metadata entry"""
        file_data = {
            'user_id': user_id,
            'filename': filename,
            'original_filename': original_filename,
            'file_path': file_path,
            'file_type': file_type,
            'file_size': file_size,
            'upload_date': datetime.now(timezone.utc),
            'md5_hash': md5_hash,
            'record_count': record_count,
            'download_url': download_url,
            'storage_key': storage_key,  # MinIO object key
            'source': source  # 'user_upload', 'gwas_library', etc.
        }
        result = self.file_metadata_collection.insert_one(file_data)
        return str(result.inserted_id)

    def get_file_metadata(self, user_id, file_id=None):
        """Get file metadata"""
        query = {'user_id': user_id}
        if file_id:
            query['_id'] = ObjectId(file_id)
            file_meta = self.file_metadata_collection.find_one(query)
            if file_meta:
                file_meta['_id'] = str(file_meta['_id'])
            return file_meta
        
        files = list(self.file_metadata_collection.find(query))
        for file_meta in files:
            file_meta['_id'] = str(file_meta['_id'])
        return files

    def delete_file_metadata(self, user_id, file_id):
        """Delete file metadata"""
        result = self.file_metadata_collection.delete_one({
            '_id': ObjectId(file_id),
            'user_id': user_id
        })
        return result.deleted_count > 0
    
    def update_file_metadata(self, file_id: str, updates: dict) -> bool:
        """Patch specific fields on an existing file metadata record."""
        updates = {**updates, "updated_at": datetime.now(timezone.utc)}
        result = self.file_metadata_collection.update_one(
            {"_id": ObjectId(file_id)},
            {"$set": updates},
        )
        return result.matched_count > 0

    def find_file_by_md5(self, user_id, md5_hash):
        """
        Find a file by MD5 hash for a specific user
        """
        file_meta = self.file_metadata_collection.find_one({
            'user_id': user_id,
            'md5_hash': md5_hash
        })
        
        if file_meta:
            file_meta['_id'] = str(file_meta['_id'])
        
        return file_meta