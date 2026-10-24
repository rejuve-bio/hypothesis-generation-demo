from datetime import datetime, timezone
from uuid import uuid4
from loguru import logger
from .base_handler import BaseHandler


class EnrichmentHandler(BaseHandler):
    """Handler for enrichment operations"""
    
    def __init__(self, uri, db_name):
        super().__init__(uri, db_name)
        self.enrich_collection = self.db['enrich']
    
    def create_enrich(
        self,
        user_id,
        project_id,
        variant,
        phenotype,
        causal_gene,
        go_terms,
        causal_graph,
        status=None,
        skip_reason=None,
    ):
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
        if status is not None:
            enrich_data['status'] = status
        if skip_reason is not None:
            enrich_data['skip_reason'] = skip_reason
        self.enrich_collection.insert_one(enrich_data)
        return enrich_data['id']

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

    def ensure_enrich_copy_for_user(self, source_enrich: dict, user_id: str, project_id: str) -> str:
        """Return an enrichment id owned by user_id for the forked project."""
        existing = self.get_enrich_by_phenotype_and_variant(
            source_enrich["phenotype"],
            source_enrich["variant"],
            user_id,
        )
        if existing and existing.get("project_id") == project_id:
            return existing["id"]

        new_doc = {key: value for key, value in source_enrich.items() if key != "_id"}
        new_doc.update(
            {
                "id": str(uuid4()),
                "user_id": user_id,
                "project_id": project_id,
                "created_at": datetime.now(timezone.utc),
            }
        )
        self.enrich_collection.insert_one(new_doc)
        logger.info(
            f"Copied enrichment {source_enrich['id']} to user {user_id} "
            f"as {new_doc['id']} in project {project_id}"
        )
        return new_doc["id"]

    def delete_enrich(self, user_id, enrich_id):
        """Delete enrichment entry"""
        result = self.enrich_collection.delete_one({'id': enrich_id, 'user_id': user_id})
        if result.deleted_count > 0:
            return {'message': 'Enrich deleted'}, 200
        return {'message': 'Enrich not found or not authorized'}, 404
