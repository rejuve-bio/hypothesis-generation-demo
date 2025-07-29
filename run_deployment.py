from prefect.deployments import run_deployment

def invoke_enrichment_deployment(current_user_id, phenotype, variant, hypothesis_id, project_id):
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
            "project_id": project_id
        },
        timeout=0
    )