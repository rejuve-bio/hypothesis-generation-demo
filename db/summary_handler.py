from bson.objectid import ObjectId
from .base_handler import BaseHandler


class SummaryHandler(BaseHandler):
    """Handler for summary operations and processing status management"""
    
    def __init__(self, uri, db_name):
        super().__init__(uri, db_name)
        self.summary_collection = self.db['summaries']
        self.processing_collection = self.db['processing_status']
    
    # ==================== SUMMARY METHODS ====================
    def create_summary(self, user_id, hypothesis_id, summary_data):
        """Create summary"""
        summary_doc = {
            "user_id": user_id,
            "hypothesis_id": hypothesis_id,
            "summary": summary_data,
        }
        result = self.summary_collection.insert_one(summary_doc)
        
        return {
            "summary_id": str(result.inserted_id),
            "user_id": user_id,
            "hypothesis_id": hypothesis_id,
            "summary": summary_data
        }, 201
    
    def check_summary(self, user_id, hypothesis_id):
        """Check if summary exists"""
        query = {}
        
        if user_id:
            query['user_id'] = user_id
        if hypothesis_id:
            query['hypothesis_id'] = hypothesis_id
        
        summary = self.summary_collection.find_one(query)
        return summary

    def check_global_summary(self, variant_input):
        """Check if global summary exists for variant"""
        query = {"variant": variant_input}
        summary = self.summary_collection.find_one(query)
        if summary:
            summary["_id"] = str(summary["_id"])
        return summary

    def create_global_summary(self, variant_input, summary_data):
        """Create global summary"""
        summary_doc = {
            "variant": variant_input,
            "summary": summary_data,
        }
        result = self.summary_collection.insert_one(summary_doc)
        return {
            "summary_id": str(result.inserted_id),
            "variant": variant_input,
            "summary": summary_data,
        }
    
    def get_summary(self, user_id, summary_id):
        """Get summary by ID"""
        if not user_id or not summary_id:
            print("Missing user_id or summary_id")
            return None
        
        query = {
        "user_id": user_id,
        "_id": ObjectId(summary_id) 
        }
        summary = self.summary_collection.find_one(query)
        if summary:
            summary["_id"] = str(summary["_id"])

        return summary

    # ==================== PROCESSING STATUS METHODS ====================
    def check_processing_status(self, variant_input):
        """Check processing status for variant"""
        return self.processing_collection.find_one({"variant": variant_input})

    def set_processing_status(self, variant_input, status):
        """Set processing status for variant"""
        if status:
            self.processing_collection.update_one(
                {"variant": variant_input},
                {"$set": {"status": "processing"}},
                upsert=True
            )
        else:
            self.processing_collection.delete_one({"variant": variant_input})