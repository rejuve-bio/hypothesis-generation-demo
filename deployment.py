from prefect import flow
from loguru import logger
from tasks import annotate_variant



@flow(log_prints=True)
def annotate_variant_flow(db, variant, current_user_id, hypothesis_id):
    try:
        annotate_variant.submit(db, variant, current_user_id, hypothesis_id)
    except Exception as e:
        logger.error(f"Error in annotation flow: {e}")

if __name__ == "__main__":
    annotate_variant_flow.serve(
        name="annotate-variant-deployment",
        tags=["background"],
    )