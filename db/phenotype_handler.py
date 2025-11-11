from datetime import datetime, timezone
from loguru import logger
from .base_handler import BaseHandler

class PhenotypeHandler(BaseHandler):
    """Handler for phenotype operations"""
    
    def __init__(self, uri, db_name):
        super().__init__(uri, db_name)
        self.phenotype_collection = self.db['phenotypes']
    
    def bulk_create_phenotypes(self, phenotypes_data):
        """Bulk insert phenotypes from a list of dicts with 'id' and 'phenotype_name' keys"""
        try:
            # Get existing phenotype IDs to avoid duplicates
            existing_ids = set()
            existing_phenotypes = self.phenotype_collection.find({}, {'id': 1})
            for phenotype in existing_phenotypes:
                existing_ids.add(phenotype['id'])
            
            # Filter out existing phenotypes
            new_phenotypes = []
            skipped_count = 0
            
            for phenotype in phenotypes_data:
                if phenotype['id'] not in existing_ids:
                    phenotype_doc = {
                        'id': phenotype['id'],
                        'phenotype_name': phenotype['phenotype_name'],
                        'created_at': datetime.now(timezone.utc)
                    }
                    new_phenotypes.append(phenotype_doc)
                else:
                    skipped_count += 1
            
            if new_phenotypes:
                result = self.phenotype_collection.insert_many(new_phenotypes)
                inserted_count = len(result.inserted_ids)
                logger.info(f"Bulk inserted {inserted_count} phenotypes, skipped {skipped_count} existing ones")
                return {'inserted_count': inserted_count, 'skipped_count': skipped_count}
            else:
                logger.info(f"No new phenotypes to insert, skipped {skipped_count} existing ones")
                return {'inserted_count': 0, 'skipped_count': skipped_count}
                
        except Exception as e:
            logger.error(f"Error bulk creating phenotypes: {str(e)}")
            raise

    def get_phenotypes(self, phenotype_id=None, limit=None, skip=0, search_term=None):
        """Get phenotypes with optional filtering and automatic memory protection"""
        try:
            query = {}
            
            if phenotype_id:
                query['id'] = phenotype_id
                phenotype = self.phenotype_collection.find_one(query)
                if phenotype:
                    phenotype['_id'] = str(phenotype['_id'])
                return phenotype
            
            if search_term:
                # Case-insensitive search in either phenotype_name or id
                query['$or'] = [
                    {'phenotype_name': {'$regex': search_term, '$options': 'i'}},
                    {'id': {'$regex': search_term, '$options': 'i'}}
                ]
            
            cursor = self.phenotype_collection.find(query, {'_id': 0})
            
            if skip > 0:
                cursor = cursor.skip(skip)
                
            if limit is None:
                DEFAULT_SAFE_LIMIT = 5000
                logger.warning(f"No limit specified for phenotype query. Applying default safe limit of {DEFAULT_SAFE_LIMIT} to prevent memory issues.")
                cursor = cursor.limit(DEFAULT_SAFE_LIMIT)
            elif limit > 0:
                cursor = cursor.limit(limit)                
            phenotypes = list(cursor)
            return phenotypes
            
        except Exception as e:
            logger.error(f"Error getting phenotypes: {str(e)}")
            raise



    def count_phenotypes(self, search_term=None):
        """Count total phenotypes with optional search filter"""
        try:
            query = {}
            if search_term:
                query['$or'] = [
                    {'phenotype_name': {'$regex': search_term, '$options': 'i'}},
                    {'id': {'$regex': search_term, '$options': 'i'}}
                ]
            return self.phenotype_collection.count_documents(query)
        except Exception as e:
            logger.error(f"Error counting phenotypes: {str(e)}")
            raise
