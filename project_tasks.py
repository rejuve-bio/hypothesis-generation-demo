import os
import pandas as pd
from prefect import task
from loguru import logger
from datetime import datetime


@task(cache_policy=None)
def save_analysis_state_task(projects_handler, user_id, project_id, state_data):
    """Save analysis state to file system"""
    try:
        projects_handler.save_analysis_state(user_id, project_id, state_data)
        logger.info(f"Saved analysis state for project {project_id}")
        return True
    except Exception as e:
        logger.info(f"Error saving analysis state: {str(e)}")
        raise


@task(cache_policy=None)
def load_analysis_state_task(projects_handler, user_id, project_id):
    """Load analysis state from file system"""
    try:
        state = projects_handler.load_analysis_state(user_id, project_id)
        if state:
            logger.info(f"Loaded analysis state for project {project_id}")
        else:
            logger.info(f"No analysis state found for project {project_id}")
        return state
    except Exception as e:
        logger.info(f"Error loading analysis state: {str(e)}")
        raise


@task(cache_policy=None)
def create_analysis_result_task(analysis_handler, user_id, project_id, combined_results, output_dir):
    """Create and save analysis results"""
    try:
        # Save to output directory
        results_file = os.path.join(output_dir, "analysis_results.csv")
        combined_results.to_csv(results_file, index=False)
        
        # Save to database
        analysis_handler.save_analysis_results(user_id, project_id, combined_results.to_dict('records'))
        
        logger.info(f"Analysis results saved: {results_file}")
        return results_file
    except Exception as e:
        logger.error(f"Error saving analysis results: {str(e)}")
        raise


@task(cache_policy=None)
def save_lead_variant_credible_sets_task(analysis_handler, user_id, project_id, lead_variant_id, credible_sets_data, metadata):
    """Save credible sets for a single lead variant incrementally"""
    try:
        # Organize data by lead variant
        lead_variant_data = {
            "lead_variant_id": lead_variant_id,
            "credible_sets": credible_sets_data,
            "metadata": metadata,
            "saved_at": datetime.now().isoformat()
        }
        
        # Save to database immediately
        analysis_handler.save_lead_variant_credible_sets(user_id, project_id, lead_variant_id, lead_variant_data)
        
        logger.info(f"Saved credible sets for lead variant {lead_variant_id}: {len(credible_sets_data)} sets")
        return True
    except Exception as e:
        logger.error(f"Error saving credible sets for lead variant {lead_variant_id}: {str(e)}")
        raise


@task(cache_policy=None)
def get_project_analysis_path_task(projects_handler, user_id, project_id):
    """Get the analysis path for a project"""
    try:
        analysis_path = projects_handler.get_project_analysis_path(user_id, project_id)
        
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

