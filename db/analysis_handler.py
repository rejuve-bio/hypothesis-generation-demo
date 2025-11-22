from bson.objectid import ObjectId
from datetime import datetime, timezone
from loguru import logger
from .base_handler import BaseHandler


class AnalysisHandler(BaseHandler):
    """Handler for analysis results and credible sets operations"""
    
    def __init__(self, uri, db_name):
        super().__init__(uri, db_name)
        self.analysis_results_collection = self.db['analysis_results']
        self.credible_sets_collection = self.db['credible_sets']
    
    def create_analysis_result(self, project_id, population, gene_types_identified, result_path=None):
        """Create analysis result entry"""
        analysis_data = {
            'project_id': project_id,
            'population': population,
            'analysis_date': datetime.now(timezone.utc),
            'status': 'completed',
            'gene_types_identified': gene_types_identified,
            'result_path': result_path
        }
        result = self.analysis_results_collection.insert_one(analysis_data)
        return str(result.inserted_id)

    def get_analysis_results(self, project_id, analysis_id=None):
        """Get analysis results for a project"""
        query = {'project_id': project_id}
        if analysis_id:
            query['_id'] = ObjectId(analysis_id)
            analysis = self.analysis_results_collection.find_one(query)
            if analysis:
                analysis['_id'] = str(analysis['_id'])
            return analysis
        
        results = list(self.analysis_results_collection.find(query))
        for analysis in results:
            analysis['_id'] = str(analysis['_id'])
        return results

    def update_analysis_result(self, analysis_id, data):
        """Update analysis result"""
        result = self.analysis_results_collection.update_one(
            {'_id': ObjectId(analysis_id)},
            {'$set': data}
        )
        return result.matched_count > 0

    def save_analysis_results(self, user_id, project_id, results_data):
        """Save analysis results to database"""
        try:
            # Create analysis result entry
            analysis_data = {
                'user_id': user_id,
                'project_id': project_id,
                'analysis_date': datetime.now(timezone.utc),
                'status': 'completed',
                'results_data': results_data
            }
            result = self.analysis_results_collection.insert_one(analysis_data)
            logger.info(f"Saved analysis results for project {project_id}: {len(results_data)} variants")
            return str(result.inserted_id)
        except Exception as e:
            logger.error(f"Error saving analysis results: {str(e)}")
            raise

    def get_lead_variant_credible_sets(self, user_id, project_id, lead_variant_id=None):
        """Get credible sets organized by lead variant"""
        try:
            query = {
                'user_id': user_id,
                'project_id': project_id,
                'type': 'lead_variant_credible_sets'
            }
            
            if lead_variant_id:
                query['lead_variant_id'] = lead_variant_id
                result = self.credible_sets_collection.find_one(query)
                if result:
                    result['_id'] = str(result['_id'])
                return result
            else:
                results = list(self.credible_sets_collection.find(query))
                for result in results:
                    result['_id'] = str(result['_id'])
                return results
        except Exception as e:
            logger.error(f"Error getting lead variant credible sets: {str(e)}")
            raise

    def save_credible_set(self, user_id, project_id, credible_set_data):
        """Save a single credible set with its own lead variant"""
        try:
            # Extract lead variant info from the credible set data
            variants_data = credible_set_data.get('variants', {}).get('data', {})
            if not variants_data or not variants_data.get('variant'):
                raise ValueError("No variant data found in credible set")
            
            # Find lead variant (highest posterior probability)
            variants = variants_data['variant']
            posterior_probs = variants_data['posterior_prob']
            max_idx = posterior_probs.index(max(posterior_probs))
            lead_variant_id = variants[max_idx]
            
            # Create lead variant info
            lead_variant = {
                'id': lead_variant_id,
                'rs_id': variants_data.get('rs_id', [None] * len(variants))[max_idx],
                'beta': variants_data['beta'][max_idx],
                'chromosome': str(variants_data['chromosome'][max_idx]),
                'log_pvalue': variants_data['log_pvalue'][max_idx],
                'position': variants_data['position'][max_idx],
                'ref_allele': variants_data['ref_allele'][max_idx],
                'minor_allele': variants_data['minor_allele'][max_idx],
                'ref_allele_freq': variants_data['ref_allele_freq'][max_idx],
                'posterior_prob': variants_data['posterior_prob'][max_idx]
            }
            
            # Create credible set document
            credible_set_doc = {
                'user_id': user_id,
                'project_id': project_id,
                'lead_variant_id': lead_variant_id,
                'coverage': credible_set_data.get('coverage'),
                'variants_count': len(variants),
                'completed_at': credible_set_data.get('completed_at'),
                'lead_variant': lead_variant,
                'variants_data': credible_set_data.get('variants'),
                'metadata': credible_set_data.get('metadata', {}),
                'created_at': datetime.now(timezone.utc),
                'type': 'credible_set'
            }
            
            result = self.credible_sets_collection.insert_one(credible_set_doc)
            logger.info(f"Saved credible set with lead variant {lead_variant_id} in project {project_id}")
            return str(result.inserted_id)
        except Exception as e:
            logger.error(f"Error saving credible set: {str(e)}")
            raise

    def get_credible_sets_for_project(self, user_id, project_id):
        """Get all credible sets for a project"""
        try:
            query = {
                'user_id': user_id,
                'project_id': project_id,
                'type': 'credible_set'
            }
            
            results = list(self.credible_sets_collection.find(query))
            credible_sets = []
            
            for result in results:
                credible_set = {
                    '_id': str(result['_id']),
                    'coverage': result.get('coverage'),
                    'variants_count': result.get('variants_count'),
                    'completed_at': result.get('completed_at'),
                    'lead_variant': result.get('lead_variant')
                }
                credible_sets.append(credible_set)
            
            return credible_sets
        except Exception as e:
            logger.error(f"Error getting credible sets for project: {str(e)}")
            raise
    
    def get_credible_set_by_lead_variant(self, user_id, project_id, lead_variant_id):
        """Get credible set data by lead variant ID"""
        try:
            query = {
                'user_id': user_id,
                'project_id': project_id,
                'lead_variant_id': lead_variant_id,
                'type': 'credible_set'
            }
            
            result = self.credible_sets_collection.find_one(query)
            if result:
                result['_id'] = str(result['_id'])
                return result
            return None
        except Exception as e:
            logger.error(f"Error getting credible set by lead variant: {str(e)}")
            raise

    def get_credible_set_by_id(self, user_id, project_id, credible_set_id):
        """Get credible set data by credible set ID"""
        try:
            query = {
                '_id': ObjectId(credible_set_id),
                'user_id': user_id,
                'project_id': project_id,
                'type': 'credible_set'
            }
            
            result = self.credible_sets_collection.find_one(query)
            if result:
                result['_id'] = str(result['_id'])
                return result
            return None
        except Exception as e:
            logger.error(f"Error getting credible set by ID: {str(e)}")
            raise
    def update_analysis_state(self, project_id, user_id, update_data):
        """Update analysis state by merging new data with existing state"""
        # Load existing state
        existing_state = self.load_analysis_state(user_id, project_id)
        if existing_state is None:
            existing_state = {}
        
        # Merge update data
        existing_state.update(update_data)
        
        # Save updated state
        self.save_analysis_state(user_id, project_id, existing_state)
        logger.info(f"Updated analysis state for project {project_id} with keys: {list(update_data.keys())}")
        return existing_state