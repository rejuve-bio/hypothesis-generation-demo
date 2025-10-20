import os
import pandas as pd
from prefect import task
from loguru import logger
from datetime import datetime, timezone
import gzip
import re


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


def count_gwas_records(file_path):
    """Count the number of records in a GWAS file"""
    try:
        
        if not os.path.exists(file_path):
            logger.warning(f"GWAS file not found: {file_path}")
            return 0
        
        count = 0
        
        # Handle gzipped files
        if file_path.endswith('.gz') or file_path.endswith('.bgz'):
            with gzip.open(file_path, 'rt') as f:
                # Skip header
                next(f, None)
                for line in f:
                    if line.strip():  # Skip empty lines
                        count += 1
        else:
            with open(file_path, 'r') as f:
                # Skip header
                next(f, None)
                for line in f:
                    if line.strip():  # Skip empty lines
                        count += 1
        
        return count
    except Exception as e:
        logger.warning(f"Error counting GWAS records in {file_path}: {str(e)}")
        return 0


def get_project_with_full_data(projects_handler, analysis_handler, hypotheses_handler, enrichment_handler, user_id, project_id):
    """Get comprehensive project data including state, hypotheses, and credible sets"""
    try:
        # Get basic project info
        project = projects_handler.get_projects(user_id, project_id)
        if not project:
            return {"error": "Project not found"}, 404
        
        # Get credible sets with simplified metadata
        credible_sets_data = []
        total_credible_sets_count = 0
        total_variants_count = 0
        analysis_parameters = project.get("analysis_parameters", {})
        
        try:
            credible_sets_data = analysis_handler.get_credible_sets_for_project(user_id, project_id)
            if credible_sets_data:
                total_credible_sets_count = len(credible_sets_data)
                total_variants_count = sum(cs.get("variants_count", 0) for cs in credible_sets_data)
            else:
                credible_sets_data = []
        except Exception as cs_e:
            logger.warning(f"Could not load credible sets for project {project_id}: {cs_e}")
            credible_sets_data = []
        
        # Get analysis state
        analysis_state = projects_handler.load_analysis_state(user_id, project_id)
        if not analysis_state:
            # Infer status from whether results exist
            if credible_sets_data and len(credible_sets_data) > 0:
                analysis_state = {"status": "Completed"}
            else:
                analysis_state = {"status": "not_started"}
        
        # Get hypotheses for this project
        project_hypotheses = []
        try:
            all_hypotheses = hypotheses_handler.get_hypotheses(user_id)
            if isinstance(all_hypotheses, list):
                project_hypotheses = []
                for h in all_hypotheses:
                    if h.get('project_id') == project_id:
                        # Extract probability from hypothesis graph OR enrichment data
                        probability = None
                        
                        # First try to get from hypothesis graph (if hypothesis is fully generated)
                        if h.get("graph") and isinstance(h["graph"], dict):
                            probability = h["graph"].get("probability")
                        
                        # If no probability in hypothesis, try to get from enrichment data
                        if probability is None and h.get("enrich_id"):
                            try:
                                enrich_data = enrichment_handler.get_enrich(user_id, h["enrich_id"])
                                if enrich_data and enrich_data.get("causal_graph"):
                                    causal_graph = enrich_data["causal_graph"]
                                    if isinstance(causal_graph, dict) and causal_graph.get("graph"):
                                        graph = causal_graph["graph"]
                                        if isinstance(graph, dict):
                                            probability = graph.get('prob', {}).get('value') if isinstance(graph.get('prob'), dict) else None
                            except Exception as e:
                                logger.warning(f"Could not get enrichment data for hypothesis {h['id']}: {e}")
                        
                        hypothesis_data = {
                            "id": h["id"], 
                            "variant": h.get("variant") or h.get("variant_id"),
                            "status": h.get("status", "pending"),
                            "causal_gene": h.get("causal_gene"),
                            "created_at": h.get("created_at"),
                            "probability": probability  # Add confidence/probability score
                        }
                        
                        project_hypotheses.append(hypothesis_data)
        except Exception as hyp_e:
            logger.warning(f"Could not load hypotheses for project {project_id}: {hyp_e}")
            project_hypotheses = []
        
        # Build comprehensive response
        response = {
            "id": project["id"],
            "name": project["name"],
            "phenotype": project.get("phenotype", ""),
            "gwas_file_id": project["gwas_file_id"],
            "created_at": project.get("created_at"),
            
            # Summary counts at top level
            "total_credible_sets_count": total_credible_sets_count,
            "total_variants_count": total_variants_count,
            
            # Analysis state and parameters  
            "analysis_state": analysis_state,
            "analysis_parameters": analysis_parameters,
            
            # Credible sets data
            "credible_sets": credible_sets_data,
            
            # Hypotheses information
            "hypotheses": project_hypotheses
        }        
        return response, 200
        
    except Exception as e:
        logger.error(f"Error getting comprehensive project data for {project_id}: {str(e)}")
        return {"error": f"Error retrieving project data: {str(e)}"}, 500


def get_phenotype_name(phenotype_id):
    """Get phenotype name from known UK Biobank field IDs"""
    known_phenotypes = {
        "21001": "Body Mass Index (BMI)",
        "50": "Standing Height",
        "23104": "Body Fat Percentage", 
        "21002": "Weight",
        "23105": "Basal Metabolic Rate",
        "23106": "Impedance",
        "23107": "Arm Fat-free Mass",
        "23108": "Arm Fat Mass",
        "23109": "Arm Predicted Mass",
        "48": "Waist Circumference",
        "49": "Hip Circumference"
    }
    return known_phenotypes.get(phenotype_id, f"Phenotype {phenotype_id}")


def extract_gwas_file_metadata(file_path):
    """Extract metadata from a GWAS file by examining its content"""
    filename = os.path.basename(file_path)
    
    try:
        # Extract phenotype ID from filename
        phenotype_id = None
        match = re.match(r'^(\d+)_', filename)
        if match:
            phenotype_id = match.group(1)
        
        # Determine file type and how to open it
        if file_path.endswith('.bgz'):
            with gzip.open(file_path, 'rt') as f:
                header = f.readline().strip().split('\t')
                lines = [f.readline().strip() for _ in range(3)]
        elif file_path.endswith('.gz'):
            with gzip.open(file_path, 'rt') as f:
                header = f.readline().strip().split('\t')
                lines = [f.readline().strip() for _ in range(3)]
        else:
            with open(file_path, 'r') as f:
                header = f.readline().strip().split('\t')
                lines = [f.readline().strip() for _ in range(3)]
        
        # Extract sample size from n_complete_samples column if available
        sample_size = "Unknown"
        if 'n_complete_samples' in header:
            try:
                n_idx = header.index('n_complete_samples')
                for line in lines:
                    if line:
                        fields = line.split('\t')
                        if len(fields) > n_idx and fields[n_idx]:
                            sample_size = f"~{int(float(fields[n_idx])):,}"
                            break
            except (ValueError, IndexError):
                pass
        
        # Extract population from filename patterns
        population = "Unknown"
        if "both_sexes" in filename:
            population = "Mixed"
        elif "male" in filename:
            population = "Male"
        elif "female" in filename:
            population = "Female"
        elif re.search(r'(eur|european)', filename.lower()):
            population = "EUR"
        elif re.search(r'(afr|african)', filename.lower()):
            population = "AFR"
        elif re.search(r'(eas|east_asian)', filename.lower()):
            population = "EAS"
        elif re.search(r'(sas|south_asian)', filename.lower()):
            population = "SAS"
        elif re.search(r'(amr|american)', filename.lower()):
            population = "AMR"
        else:
            # Default to EUR for UK Biobank files
            if phenotype_id and phenotype_id.startswith(('2', '50', '23')):
                population = "EUR"
        
        # Determine genome build
        genome_build = "GRCh37"
        if "hg38" in filename.lower() or "grch38" in filename.lower():
            genome_build = "GRCh38"
        
        # Get phenotype name from known mappings or use ID
        phenotype_name = get_phenotype_name(phenotype_id) if phenotype_id else "Unknown"
        
        return {
            "phenotype_id": phenotype_id,
            "phenotype_name": phenotype_name,
            "population": population,
            "sample_size": sample_size,
            "genome_build": genome_build,
            "header_columns": header,
            "file_size": os.path.getsize(file_path)
        }
        
    except Exception as e:
        logger.warning(f"Could not extract metadata from {filename}: {str(e)}")
        return {
            "phenotype_id": phenotype_id,
            "phenotype_name": f"Phenotype {phenotype_id}" if phenotype_id else "Unknown",
            "population": "Unknown",
            "sample_size": "Unknown",
            "genome_build": "Unknown",
            "header_columns": [],
            "file_size": os.path.getsize(file_path) if os.path.exists(file_path) else 0
        }

