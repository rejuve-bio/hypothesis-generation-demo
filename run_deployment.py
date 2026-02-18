from prefect.deployments import run_deployment

def invoke_enrichment_deployment(current_user_id, phenotype, variant, hypothesis_id, project_id, seed):
    """
    Invoke enrichment deployment with minimal parameters.
    Objects are initialized inside the flow for deployment compatibility.
    """
    run_deployment(
        name="enrichment-flow/enrichment-flow-deployment",
        parameters={
            "current_user_id": current_user_id, 
            "phenotype": phenotype, 
            "variant": variant,
            "hypothesis_id": hypothesis_id,
            "project_id": project_id,
            "seed": seed
        },
        timeout=0
    )

def invoke_analysis_pipeline_deployment(
    user_id, project_id, gwas_file_path, ref_genome, population, 
    batch_size, max_workers, maf_threshold, seed, window, L, 
    coverage, min_abs_corr, sample_size
):

    run_deployment(
        name="analysis-pipeline-flow/analysis-pipeline-deployment", 
        parameters={
            "user_id": user_id,
            "project_id": project_id,
            "gwas_file_path": gwas_file_path,
            "ref_genome": ref_genome,
            "population": population,
            "batch_size": batch_size,
            "max_workers": max_workers,
            "maf_threshold": maf_threshold,
            "seed": seed,
            "window": window,
            "L": L,
            "coverage": coverage,
            "min_abs_corr": min_abs_corr,
            "sample_size": sample_size
        },
        timeout=0 
    )

def invoke_child_batch_deployment(current_user_id, child_enrich_ids, parent_hypothesis_id):

    run_deployment(
        name="child-enrichment-batch-flow/child-batch-deployment",
        parameters={
            "current_user_id": current_user_id,
            "child_enrich_ids": child_enrich_ids,
            "parent_hypothesis_id": parent_hypothesis_id
        },
        timeout=0  
    )

def invoke_hypothesis_deployment(current_user_id, hypothesis_id, enrich_id, go_id):

    run_deployment(
        name="hypothesis-flow/hypothesis-generation-deployment",
        parameters={
            "current_user_id": current_user_id,
            "hypothesis_id": hypothesis_id,
            "enrich_id": enrich_id,
            "go_id": go_id
        },
        timeout=0  # Fire-and-forget
    )