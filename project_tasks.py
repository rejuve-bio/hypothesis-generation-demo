import os
from prefect import task
from loguru import logger


@task(cache_policy=None)
def save_analysis_state_task(db, user_id, project_id, state_data):
    """Save analysis state to file system"""
    try:
        db.save_analysis_state(user_id, project_id, state_data)
        logger.info(f"Saved analysis state for project {project_id}")
        return True
    except Exception as e:
        logger.info(f"Error saving analysis state: {str(e)}")
        raise


@task(cache_policy=None)
def load_analysis_state_task(db, user_id, project_id):
    """Load analysis state from file system"""
    try:
        state = db.load_analysis_state(user_id, project_id)
        if state:
            logger.info(f"Loaded analysis state for project {project_id}")
        else:
            logger.info(f"No analysis state found for project {project_id}")
        return state
    except Exception as e:
        logger.info(f"Error loading analysis state: {str(e)}")
        raise


@task(cache_policy=None)
def create_analysis_result_task(db, project_id, population, gene_types_identified):
    """Create analysis result entry in database"""
    try:
        analysis_id = db.create_analysis_result(project_id, population, gene_types_identified)
        logger.info(f"Created analysis result {analysis_id} for project {project_id}")
        return analysis_id
    except Exception as e:
        logger.info(f"Error creating analysis result: {str(e)}")
        raise


@task(cache_policy=None)
def create_credible_sets_task(db, analysis_id, credible_sets_data):
    """Create credible sets entries in database"""
    try:
        credible_set_ids = {}
        for gene_type, data in credible_sets_data.items():
            credible_set_id = db.create_credible_set(analysis_id, gene_type, data)
            credible_set_ids[gene_type] = credible_set_id
            logger.info(f"Created credible set {credible_set_id} for gene type {gene_type}")
        
        return credible_set_ids
    except Exception as e:
        logger.info(f"Error creating credible sets: {str(e)}")
        raise


@task(cache_policy=None)
def check_existing_credible_sets(db, analysis_id, requested_gene_types):
    """Check which credible sets already exist and which need to be computed"""
    try:
        existing_sets = {}
        missing_gene_types = []
        
        for gene_type in requested_gene_types:
            if db.check_credible_set_exists(analysis_id, gene_type):
                credible_set = db.get_credible_sets(analysis_id, gene_type=gene_type)
                if credible_set:
                    existing_sets[gene_type] = credible_set[0]  # Get first result
                    logger.info(f"Found existing credible set for {gene_type}")
            else:
                missing_gene_types.append(gene_type)
                logger.info(f"Need to compute credible set for {gene_type}")
        
        return existing_sets, missing_gene_types
    except Exception as e:
        logger.info(f"Error checking existing credible sets: {str(e)}")
        raise


@task(cache_policy=None)
def get_project_analysis_path_task(db, user_id, project_id):
    """Get the analysis path for a project"""
    try:
        analysis_path = db.get_project_analysis_path(user_id, project_id)
        
        # Create directory structure if it doesn't exist
        os.makedirs(analysis_path, exist_ok=True)
        os.makedirs(os.path.join(analysis_path, "preprocessed_data"), exist_ok=True)
        os.makedirs(os.path.join(analysis_path, "plink_binary"), exist_ok=True)
        os.makedirs(os.path.join(analysis_path, "cojo"), exist_ok=True)
        os.makedirs(os.path.join(analysis_path, "expanded_regions"), exist_ok=True)
        os.makedirs(os.path.join(analysis_path, "ld"), exist_ok=True)
        
        logger.info(f"Project analysis path: {analysis_path}")
        return analysis_path
    except Exception as e:
        logger.info(f"Error getting project analysis path: {str(e)}")
        raise