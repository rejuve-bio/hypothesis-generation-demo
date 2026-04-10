from loguru import logger
from .base_handler import BaseHandler


class HypothesisHandler(BaseHandler):
    """Handler for hypothesis operations"""
    
    def __init__(self, uri, db_name):
        super().__init__(uri, db_name)
        self.hypothesis_collection = self.db['hypotheses']
        self.enrich_collection = self.db['enrich']
    
    def create_hypothesis(self, user_id, data):
        """Create a new hypothesis"""
        data['user_id'] = user_id
        result = self.hypothesis_collection.insert_one(data)
        created_id = data.get('id', str(result.inserted_id))
        return {'message': 'Hypothesis created', 'id': created_id}, 201

    def get_hypotheses(self, user_id=None, hypothesis_id=None):
        """Get hypotheses"""
        query = {}
        
        if user_id:
            query['user_id'] = user_id
        if hypothesis_id:
            query['id'] = hypothesis_id
            hypothesis = self.hypothesis_collection.find_one(query)
            if hypothesis:
                hypothesis["_id"] = str(hypothesis["_id"])
            else:
                logger.info("No document found for the given hypothesis id.")
            return hypothesis

        hypotheses = list(self.hypothesis_collection.find(query))
        for hypothesis in hypotheses:
            hypothesis['_id'] = str(hypothesis['_id'])

        return hypotheses if hypotheses else []

    def get_hypotheses_by_project(self, user_id, project_id):
        """Get all hypotheses for a project"""
        hypotheses = list(self.hypothesis_collection.find({
            'user_id': user_id,
            'project_id': project_id
        }))
        for hypothesis in hypotheses:
            hypothesis['_id'] = str(hypothesis['_id'])
        return hypotheses

    def check_hypothesis(self, user_id=None, enrich_id=None, go_id=None):
        """Check if hypothesis exists"""
        query = {}
        
        if user_id:
            query['user_id'] = user_id
        if enrich_id:
            query['enrich_id'] = enrich_id
        if go_id:
            query['go_id'] = go_id
        
        hypothesis = self.hypothesis_collection.find_one(query)
        return hypothesis is not None

    def get_hypothesis_by_enrich_and_go(self, enrich_id, go_id, user_id=None):
        """Get hypothesis by enrichment and GO term"""
        query = {
            'enrich_id': enrich_id,
            'go_id': go_id,
            'user_id': user_id
        }
        hypothesis = self.hypothesis_collection.find_one(query)
        if hypothesis:
            hypothesis['_id'] = str(hypothesis['_id'])

        return hypothesis

    def get_hypothesis_by_enrich(self, user_id, enrich_id):
        """Get hypothesis by enrichment ID"""
        return self.hypothesis_collection.find_one({
            'user_id': user_id,
            'enrich_id': enrich_id
        })

    def get_hypothesis_by_id(self, hypothesis_id):
        """Get hypothesis by ID without user filtering - used by system services"""
        hypothesis = self.hypothesis_collection.find_one({'id': hypothesis_id})
        if hypothesis:
            hypothesis['_id'] = str(hypothesis['_id'])
        return hypothesis

    def get_hypothesis_by_phenotype_and_variant_in_project(self, user_id, project_id, phenotype, variant):
        """Get hypothesis by phenotype, variant, and project"""
        return self.hypothesis_collection.find_one({
            'user_id': user_id,
            'project_id': project_id,
            'phenotype': phenotype,
            'variant': variant
        })

    def update_hypothesis(self, hypothesis_id, data):
        """Update hypothesis"""
        # Remove _id if present in data to avoid modification errors
        if '_id' in data:
            del data['_id']
        
        result = self.hypothesis_collection.update_one(
            {'id': hypothesis_id},
            {'$set': data}
        )
        
        if result.matched_count > 0:
            return {'message': 'Hypothesis updated successfully'}, 200
        return {'message': 'Hypothesis not found'}, 404

    def delete_hypothesis(self, user_id, hypothesis_id):
        """Delete hypothesis and associated enrichments"""
        hypothesis = self.hypothesis_collection.find_one({'id': hypothesis_id, 'user_id': user_id})
        if not hypothesis:
            return {'message': 'Hypothesis not found or not authorized'}, 404
        
        # Debug: Log all hypothesis fields to understand the data structure
        logger.info(f"Hypothesis fields for {hypothesis_id}: {list(hypothesis.keys())}")
        logger.info(f"Hypothesis enrich_id: {hypothesis.get('enrich_id')}")
        logger.info(f"Hypothesis child_enrich_ids: {hypothesis.get('child_enrich_ids')}")
        
        # Collect enrichment IDs to delete
        enrichment_ids_to_delete = []
        if hypothesis.get('enrich_id'):
            enrichment_ids_to_delete.append(hypothesis['enrich_id'])
        if hypothesis.get('child_enrich_ids'):
            enrichment_ids_to_delete.extend(hypothesis['child_enrich_ids'])
        
        logger.info(f"Looking for enrichments to delete for hypothesis {hypothesis_id}: {enrichment_ids_to_delete}")
        
        # Delete associated enrichments
        enrich_result = None
        if enrichment_ids_to_delete:
            enrich_result = self.enrich_collection.delete_many({
                'user_id': user_id,
                'id': {'$in': enrichment_ids_to_delete}
            })
        else:
            # Create a mock result for consistent logging
            class MockResult:
                deleted_count = 0
            enrich_result = MockResult()
        logger.info(f"Deleted {enrich_result.deleted_count} enrichments for hypothesis {hypothesis_id}")
        
        # Delete the hypothesis
        result = self.hypothesis_collection.delete_one({'id': hypothesis_id, 'user_id': user_id})
        if result.deleted_count > 0:
            return {'message': f'Hypothesis and {enrich_result.deleted_count} associated enrichments deleted'}, 200
        return {'message': 'Hypothesis not found or not authorized'}, 404
    
    def bulk_delete_hypotheses(self, user_id, hypothesis_ids):
        """Delete multiple hypotheses and their associated enrichments by their IDs for a specific user"""
        if not hypothesis_ids or not isinstance(hypothesis_ids, list):
            return {'message': 'Invalid hypothesis_ids format. Expected a non-empty list.'}, 400

        # Get hypotheses to extract enrichment IDs before deletion
        hypotheses = list(self.hypothesis_collection.find({
            'id': {'$in': hypothesis_ids}, 
            'user_id': user_id
        }))
        
        if not hypotheses:
            return {'message': 'No hypotheses found or not authorized'}, 404
        
        # Collect all enrichment IDs to delete
        enrichment_ids_to_delete = []
        for hypothesis in hypotheses:
            # Add main enrichment ID
            if hypothesis.get('enrich_id'):
                enrichment_ids_to_delete.append(hypothesis['enrich_id'])
            # Add child enrichment IDs
            if hypothesis.get('child_enrich_ids'):
                enrichment_ids_to_delete.extend(hypothesis['child_enrich_ids'])
        
        # Delete associated enrichments
        total_enrichments_deleted = 0
        if enrichment_ids_to_delete:
            enrich_result = self.enrich_collection.delete_many({
                'user_id': user_id,
                'id': {'$in': enrichment_ids_to_delete}
            })
            total_enrichments_deleted = enrich_result.deleted_count
            logger.info(f"Deleted {total_enrichments_deleted} enrichments for {len(hypotheses)} hypotheses")

        # Bulk delete hypotheses
        bulk_result = self.hypothesis_collection.delete_many({
            'id': {'$in': hypothesis_ids}, 
            'user_id': user_id
        })

        # Get the actual deleted hypothesis IDs
        deleted_hypothesis_ids = [h['id'] for h in hypotheses if h['id'] in hypothesis_ids]
        failed_ids = list(set(hypothesis_ids) - set(deleted_hypothesis_ids))

        # Check if all were deleted
        if bulk_result.deleted_count == len(hypothesis_ids):
            return {
                'message': f'All {bulk_result.deleted_count} hypotheses and {total_enrichments_deleted} associated enrichments deleted successfully',
                'deleted_count': bulk_result.deleted_count,
                'enrichments_deleted': total_enrichments_deleted,
                'successful': hypothesis_ids,
                'failed': []
            }, 200

        return {
            'message': f"{bulk_result.deleted_count} hypotheses and {total_enrichments_deleted} enrichments deleted successfully, {len(failed_ids)} failed",
            'deleted_count': bulk_result.deleted_count,
            'enrichments_deleted': total_enrichments_deleted,
            'successful': deleted_hypothesis_ids,
            'failed': [{'id': h_id, 'reason': 'Not found or not authorized'} for h_id in failed_ids]
        }, 207 if deleted_hypothesis_ids else 404  # Use 207 for partial success