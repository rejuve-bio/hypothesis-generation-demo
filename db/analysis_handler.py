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

    def save_lead_variant_credible_sets(self, user_id, project_id, lead_variant_id, lead_variant_data):
        """Save credible sets for a specific lead variant incrementally"""
        try:
            # Create entry organized by lead variant
            credible_set_data = {
                'user_id': user_id,
                'project_id': project_id,
                'lead_variant_id': lead_variant_id,
                'data': lead_variant_data,
                'created_at': datetime.now(timezone.utc),
                'type': 'lead_variant_credible_sets'
            }
            
            # Upsert - update if exists, create if doesn't
            result = self.credible_sets_collection.update_one(
                {
                    'user_id': user_id,
                    'project_id': project_id,
                    'lead_variant_id': lead_variant_id,
                    'type': 'lead_variant_credible_sets'
                },
                {'$set': credible_set_data},
                upsert=True
            )
            
            logger.info(f"Saved/updated credible sets for lead variant {lead_variant_id} in project {project_id}")
            return str(result.upserted_id) if result.upserted_id else "updated"
        except Exception as e:
            logger.error(f"Error saving lead variant credible sets: {str(e)}")
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