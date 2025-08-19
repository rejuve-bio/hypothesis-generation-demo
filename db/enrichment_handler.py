from datetime import datetime, timezone
from uuid import uuid4
from loguru import logger
from .base_handler import BaseHandler


class EnrichmentHandler(BaseHandler):
    """Handler for enrichment operations"""
    
    def __init__(self, uri, db_name):
        super().__init__(uri, db_name)
        self.enrich_collection = self.db['enrich']
    
    def create_enrich(self, user_id, project_id, variant, phenotype, causal_gene, go_terms, causal_graph):
        """Create enrichment entry with project references"""
        enrich_data = {
            'id': str(uuid4()),
            'user_id': user_id,
            'project_id': project_id,
            'variant': variant,
            'phenotype': phenotype,
            'causal_gene': causal_gene,
            'GO_terms': go_terms,
            'causal_graph': causal_graph,
            'created_at': datetime.now(timezone.utc)
        }
        result = self.enrich_collection.insert_one(enrich_data)
        return enrich_data['id']

    def get_enrich_by_lead_variant(self, user_id, lead_variant_id, variant, phenotype):
        """Check if enrichment exists for lead variant, variant, and phenotype"""
        return self.enrich_collection.find_one({
            'user_id': user_id,
            'lead_variant_id': lead_variant_id,
            'variant': variant,
            'phenotype': phenotype
        })

    def check_enrich(self, user_id=None, phenotype=None, variant_id=None):
        """Check if enrichment exists for given parameters"""
        query = {}
        
        if user_id:
            query['user_id'] = user_id
        if phenotype:
            query['phenotype'] = phenotype
        if variant_id:
            query['variant'] = variant_id
        
        enrich = self.enrich_collection.find_one(query)
        return enrich is not None

    def get_enrich_by_phenotype_and_variant(self, phenotype, variant_id, user_id=None):
        """Get enrichment by phenotype and variant"""
        query = {
            'phenotype': phenotype,
            'variant': variant_id,
            'user_id': user_id
        }
        
        enrich = self.enrich_collection.find_one(query)
        
        if enrich:
            enrich['_id'] = str(enrich['_id'])
        
        return enrich

    def get_enrich(self, user_id=None, enrich_id=None):
        """Get enrichment data"""
        query = {}
        
        if user_id:
            query['user_id'] = user_id
        if enrich_id:
            query['id'] = enrich_id
            enrich = self.enrich_collection.find_one(query)  
            if enrich:
                enrich['_id'] = str(enrich['_id'])
            else:
                logger.info("No document found for the given enrich_id.")
            return enrich

        enriches = list(self.enrich_collection.find(query))
        for enrich in enriches:
            enrich['_id'] = str(enrich['_id'])

        return enriches if enriches else []

    def delete_enrich(self, user_id, enrich_id):
        """Delete enrichment entry"""
        result = self.enrich_collection.delete_one({'id': enrich_id, 'user_id': user_id})
        if result.deleted_count > 0:
            return {'message': 'Enrich deleted'}, 200
        return {'message': 'Enrich not found or not authorized'}, 404