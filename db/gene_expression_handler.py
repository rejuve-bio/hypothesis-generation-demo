from bson.objectid import ObjectId
from datetime import datetime, timezone
from uuid import uuid4
from loguru import logger
import pandas as pd

from .base_handler import BaseHandler


class GeneExpressionHandler(BaseHandler):
    """Handler for gene_expression results"""
    
    def __init__(self, uri, db_name):
        super().__init__(uri, db_name)
        self.gene_expression_runs_collection = self.db['gene_expression_runs']
        self.ldsc_results_collection = self.db['ldsc_results']
        self.coexpression_results_collection = self.db['coexpression_results']
        self.pathway_results_collection = self.db['pathway_results']
        self.tissue_mappings_collection = self.db['tissue_mappings']
    
    def create_gene_expression_run(self, gwas_file, gene_of_interest, project_id, user_id):
        """Create a new gene expression analysis run"""
        run_data = {
            'id': str(uuid4()),
            'gwas_file': gwas_file,
            'gene_of_interest': gene_of_interest,
            'project_id': project_id,
            'user_id': user_id,
            'status': 'running',
            'created_at': datetime.now(timezone.utc),
            'updated_at': datetime.now(timezone.utc)
        }
        result = self.gene_expression_runs_collection.insert_one(run_data)
        logger.info(f"Created gene expression run {run_data['id']} for gene {gene_of_interest}")
        return run_data['id']


    def update_gene_expression_run_status(self, analysis_run_id, status):
        """Update gene expression analysis run status"""
        result = self.gene_expression_runs_collection.update_one(
            {'id': analysis_run_id},
            {'$set': {
                'status': status,
                'updated_at': datetime.now(timezone.utc)
            }}
        )
        logger.info(f"Updated gene expression run {analysis_run_id} status to {status}")
        return result.matched_count > 0


    def save_ldsc_results(self, analysis_run_id, ldsc_results_data):
        """Save LDSC results to database"""
        try:
            results_docs = []
            for idx, result in enumerate(ldsc_results_data):
                doc = {
                    'id': str(uuid4()),
                    'analysis_run_id': analysis_run_id,
                    'tissue_name': result.get('Name', ''),
                    'coefficient': result.get('Coefficient'),
                    'coefficient_se': result.get('Coefficient_std_error'),
                    'p_value': result.get('Coefficient_P_value'),
                    'rank_order': idx + 1,
                    'created_at': datetime.now(timezone.utc)
                }
                results_docs.append(doc)
            
            if results_docs:
                self.ldsc_results_collection.insert_many(results_docs)
                logger.info(f"Saved {len(results_docs)} LDSC results for analysis {analysis_run_id}")
            return len(results_docs)
        except Exception as e:
            logger.error(f"Error saving LDSC results: {str(e)}")
            raise


    def save_tissue_mappings(self, analysis_run_id, tissue_mapping_results):
        """Save tissue mapping results"""
        try:
            mapping_docs = []
            for gtex_tissue, mapping_data in tissue_mapping_results.items():
                doc = {
                    'id': str(uuid4()),
                    'analysis_run_id': analysis_run_id,
                    'gtex_tissue_name': mapping_data.get('gtex_tissue_name', ''),
                    'gtex_uberon_id': mapping_data.get('gtex_uberon_id', ''),
                    'cellxgene_parent_uberon_id': mapping_data.get('cellxgene_parent_uberon_id', ''),
                    'cellxgene_descendant_uberon_id': mapping_data.get('cellxgene_descendant_uberon_id', ''),
                    'cellxgene_parent_ontology_name': mapping_data.get('cellxgene_parent_ontology_name', ''),
                    'cellxgene_descendant_ontology_name': mapping_data.get('cellxgene_descendant_ontology_name', ''),
                    'match_type': mapping_data.get('match_type', ''),
                    'mapping_notes': mapping_data.get('notes', ''),
                    'created_at': datetime.now(timezone.utc)
                }
                mapping_docs.append(doc)
            
            if mapping_docs:
                self.tissue_mappings_collection.insert_many(mapping_docs)
                logger.info(f"Saved {len(mapping_docs)} tissue mappings for analysis {analysis_run_id}")
            return len(mapping_docs)
        except Exception as e:
            logger.error(f"Error saving tissue mappings: {str(e)}")
            raise


    def save_coexpression_results(self, analysis_run_id, hgnc_converted_results):
        """Save co-expression results"""
        try:
            results_docs = []
            for tissue_name, results in hgnc_converted_results.items():
                # Save positive correlations
                for rank, (gene_symbol, correlation) in enumerate(results.get('top_positive_hgnc', [])):
                    doc = {
                        'id': str(uuid4()),
                        'analysis_run_id': analysis_run_id,
                        'tissue_name': tissue_name,
                        'gene_symbol': gene_symbol,
                        'correlation': correlation,
                        'correlation_type': 'positive',
                        'rank_order': rank + 1,
                        'created_at': datetime.now(timezone.utc)
                    }
                    results_docs.append(doc)
                
                # Save negative correlations
                for rank, (gene_symbol, correlation) in enumerate(results.get('top_negative_hgnc', [])):
                    doc = {
                        'id': str(uuid4()),
                        'analysis_run_id': analysis_run_id,
                        'tissue_name': tissue_name,
                        'gene_symbol': gene_symbol,
                        'correlation': correlation,
                        'correlation_type': 'negative',
                        'rank_order': rank + 1,
                        'created_at': datetime.now(timezone.utc)
                    }
                    results_docs.append(doc)
            
            if results_docs:
                self.coexpression_results_collection.insert_many(results_docs)
                logger.info(f"Saved {len(results_docs)} co-expression results for analysis {analysis_run_id}")
            return len(results_docs)
        except Exception as e:
            logger.error(f"Error saving co-expression results: {str(e)}")
            raise


    def save_pathway_results(self, analysis_run_id, pathway_results):
        """Save pathway enrichment results"""
        try:
            results_docs = []
            for tissue_name, df in pathway_results.items():
                if df is not None and not df.empty:
                    for _, row in df.iterrows():
                        doc = {
                            'id': str(uuid4()),
                            'analysis_run_id': analysis_run_id,
                            'tissue_name': tissue_name,
                            'pathway_term': row.get('Term', ''),
                            'pathway_id': row.get('ID', ''),
                            'adjusted_p_value': row.get('Adjusted P-value'),
                            'odds_ratio': row.get('Odds Ratio'),
                            'overlap_count': row.get('Overlap'),
                            'pathway_size': row.get('Gene_set_size'),
                            'created_at': datetime.now(timezone.utc)
                        }
                        results_docs.append(doc)
            
            if results_docs:
                self.pathway_results_collection.insert_many(results_docs)
                logger.info(f"Saved {len(results_docs)} pathway results for analysis {analysis_run_id}")
            return len(results_docs)
        except Exception as e:
            logger.error(f"Error saving pathway results: {str(e)}")
            raise


    def get_gene_expression_results(self, project_id=None, user_id=None, gene_of_interest=None):
        """Get gene expression analysis results"""
        query = {}
        if project_id:
            query['project_id'] = project_id
        if user_id:
            query['user_id'] = user_id
        if gene_of_interest:
            query['gene_of_interest'] = gene_of_interest
        
        runs = list(self.gene_expression_runs_collection.find(query).sort('created_at', -1))
        
        results = []
        for run in runs:
            run['_id'] = str(run['_id'])
            analysis_run_id = run['id']
            
            # Get LDSC results
            ldsc_results = list(self.ldsc_results_collection.find(
                {'analysis_run_id': analysis_run_id}
            ).sort('rank_order', 1))
            
            # Get co-expression results
            coexp_results = list(self.coexpression_results_collection.find(
                {'analysis_run_id': analysis_run_id}
            ).sort([('tissue_name', 1), ('correlation_type', 1), ('rank_order', 1)]))
            
            # Get pathway results
            pathway_results = list(self.pathway_results_collection.find(
                {'analysis_run_id': analysis_run_id}
            ).sort([('tissue_name', 1), ('adjusted_p_value', 1)]))
            
            # Clean up _id fields
            for result_list in [ldsc_results, coexp_results, pathway_results]:
                for result in result_list:
                    result['_id'] = str(result['_id'])
            
            results.append({
                'run_info': run,
                'ldsc_results': ldsc_results,
                'coexpression_results': coexp_results,
                'pathway_results': pathway_results
            })
        
        return results


    def check_gene_expression_status(self, project_id, user_id):
        """Check gene expression analysis status for a project"""
        latest_run = self.gene_expression_runs_collection.find_one(
            {'project_id': project_id, 'user_id': user_id},
            sort=[('created_at', -1)]
        )
        
        if not latest_run:
            return {
                'status': 'not_started',
                'has_data': False,
                'analysis_count': 0
            }
        
        # Count results
        analysis_run_id = latest_run['id']
        ldsc_count = self.ldsc_results_collection.count_documents({'analysis_run_id': analysis_run_id})
        coexp_count = self.coexpression_results_collection.count_documents({'analysis_run_id': analysis_run_id})
        pathway_count = self.pathway_results_collection.count_documents({'analysis_run_id': analysis_run_id})
        
        return {
            'status': latest_run['status'],
            'has_data': latest_run['status'] == 'completed',
            'gene_of_interest': latest_run['gene_of_interest'],
            'created_at': latest_run['created_at'],
            'ldsc_count': ldsc_count,
            'coexpression_count': coexp_count,
            'pathway_count': pathway_count
        }


    def get_coexpressed_genes_for_enrichment(self, gene_of_interest, project_id=None, min_correlation=0.5):
        """Get co-expressed genes for enrichment analysis"""
        # Find latest completed run
        run_query = {'gene_of_interest': gene_of_interest, 'status': 'completed'}
        if project_id:
            run_query['project_id'] = project_id
        
        latest_run = self.gene_expression_runs_collection.find_one(
            run_query, sort=[('created_at', -1)]
        )
        
        if not latest_run:
            return []
        
        # Get co-expressed genes
        coexp_query = {
            'analysis_run_id': latest_run['id'],
            'correlation_type': 'positive',
            'correlation': {'$gte': min_correlation}
        }
        
        coexp_results = list(self.coexpression_results_collection.find(
            coexp_query
        ).sort('correlation', -1))
        
        # Return unique gene symbols
        seen_genes = set()
        unique_genes = []
        for result in coexp_results:
            gene_symbol = result['gene_symbol']
            if gene_symbol not in seen_genes:
                unique_genes.append(gene_symbol)
                seen_genes.add(gene_symbol)
        
        return unique_genes

    # ==================== TISSUE SELECTION METHODS ====================
    
    def save_tissue_selection(self, user_id, project_id, variant_id, tissue_name):
        try:
            selection_data = {
                'id': str(uuid4()),
                'user_id': user_id,
                'project_id': project_id,
                'variant_id': variant_id,
                'tissue_name': tissue_name,
                'created_at': datetime.now(timezone.utc)
            }
            
            # Use upsert to replace any existing selection for this variant
            result = self.db['tissue_selections'].replace_one(
                {
                    'user_id': user_id,
                    'project_id': project_id,
                    'variant_id': variant_id
                },
                selection_data,
                upsert=True
            )
            
            logger.info(f"Saved tissue selection: {tissue_name} for variant {variant_id}")
            return selection_data['id']
            
        except Exception as e:
            logger.error(f"Error saving tissue selection: {str(e)}")
            raise
    
    def get_tissue_selection(self, user_id, project_id, variant_id):
        """Get user's tissue selection for a specific variant"""
        try:
            selection = self.db['tissue_selections'].find_one({
                'user_id': user_id,
                'project_id': project_id,
                'variant_id': variant_id
            })
            
            if selection:
                selection['_id'] = str(selection['_id'])
                
            return selection
            
        except Exception as e:
            logger.error(f"Error getting tissue selection: {str(e)}")
            return None
    
    def get_tissue_mapping(self, user_id, project_id, tissue_name):
        """Get tissue mapping with UBERON ID for a specific tissue from latest analysis"""
        try:
            # Get the latest LDSC analysis for this project
            latest_run = self.gene_expression_runs_collection.find_one(
                {
                    'user_id': user_id,
                    'project_id': project_id,
                    'gene_of_interest': 'project_analysis',
                    'status': {'$in': ['ldsc_tissue_completed', 'completed']}
                },
                sort=[('created_at', -1)]
            )
            
            if not latest_run:
                logger.warning(f"No completed LDSC analysis found for project {project_id}")
                return None
            
            # Get the tissue mapping for this tissue name
            mapping = self.tissue_mappings_collection.find_one({
                'analysis_run_id': latest_run['id'],
                'gtex_tissue_name': tissue_name
            })
            
            if mapping:
                mapping['_id'] = str(mapping['_id'])
                logger.info(f"Found tissue mapping for '{tissue_name}': {mapping.get('cellxgene_parent_uberon_id')} or {mapping.get('cellxgene_descendant_uberon_id')}")
            else:
                logger.warning(f"No tissue mapping found for '{tissue_name}' in analysis {latest_run['id']}")
                
            return mapping
            
        except Exception as e:
            logger.error(f"Error getting tissue mapping: {str(e)}")
            return None
    
    def get_ldsc_results_for_project(self, user_id, project_id, limit=10, format='summary'):
        try:
            # Get the latest LDSC analysis for this project
            latest_run = self.gene_expression_runs_collection.find_one(
                {
                    'user_id': user_id,
                    'project_id': project_id,
                    'gene_of_interest': 'project_analysis',
                    'status': {'$in': ['ldsc_tissue_completed', 'completed']}
                },
                sort=[('created_at', -1)]
            )
            
            if not latest_run:
                return None if format == 'summary' else []
            
            # Get top LDSC results sorted by p-value (most significant first)
            ldsc_results = list(self.ldsc_results_collection.find(
                {'analysis_run_id': latest_run['id']}
            ).sort('p_value', 1).limit(limit))
            

            if not ldsc_results and format != 'selection':
                return {
                    'status': latest_run.get('status', 'ldsc_tissue_completed'),
                    'tissues': [],
                    'total_tissues_analyzed': 0,
                    'significant_tissues_count': 0,
                    'completed_at': latest_run.get('updated_at') or latest_run.get('created_at')
                }
            if not ldsc_results and format == 'selection':
                return []
            
            if format == 'selection':
                # Format for tissue selection UI
                tissues = []
                for result in ldsc_results:
                    tissue_data = {
                        'tissue_name': result['tissue_name'],
                        'coefficient': result.get('coefficient'),
                        'p_value': result.get('p_value'),
                        'rank_order': result.get('rank_order'),
                        'is_significant': result.get('p_value', 1) < 0.05
                    }
                    tissues.append(tissue_data)
                return tissues
            
            else:  # format == 'summary'
                # Get total counts
                total_tissues = self.ldsc_results_collection.count_documents(
                    {'analysis_run_id': latest_run['id']}
                )
                
                significant_count = self.ldsc_results_collection.count_documents({
                    'analysis_run_id': latest_run['id'],
                    'p_value': {'$lt': 0.05}
                })
                
                # Format for project API response
                tissues = [
                    {
                        'name': r['tissue_name'],
                        'coefficient': r.get('coefficient'),
                        'coefficient_std_error': r.get('coefficient_se'),
                        'p_value': r.get('p_value')
                    }
                    for r in ldsc_results
                ]
                
                return {
                    'status': 'completed',
                    'tissues': tissues,
                    'total_tissues_analyzed': total_tissues,
                    'significant_tissues_count': significant_count,
                    'completed_at': latest_run.get('updated_at') or latest_run.get('created_at')
                }
            
        except Exception as e:
            logger.error(f"Error getting LDSC results: {str(e)}")
            return None if format == 'summary' else []