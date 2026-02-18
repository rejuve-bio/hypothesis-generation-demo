from loguru import logger

def dask_setup(worker):
    """
    Dask worker setup function called when each worker starts.
    """
    if hasattr(worker, "deps") and worker.deps is not None:
        logger.info("[DASK PRELOAD] Worker deps already been set up")
        return

    # every worker needs to intialize db instances since we cant serialize them
    logger.info("[DASK PRELOAD] =========================================")
    logger.info("[DASK PRELOAD] Worker starting, setting up dependencies...")
    logger.info(f"[DASK PRELOAD] Worker ID: {getattr(worker, 'id', 'unknown')}")
    
    try:
        from config import create_dependencies, Config
        from status_tracker import status_tracker
        
        config = Config.from_env()
        logger.info("[DASK PRELOAD] Config created, creating dependencies...")
        deps = create_dependencies(config)
        logger.info("[DASK PRELOAD] Dependencies created successfully")
        
        # Initialize StatusTracker singleton for this worker
        status_tracker.initialize(deps['tasks'])
        
        worker.deps = deps
        logger.info("[DASK PRELOAD] Worker dependencies set up successfully!")
        logger.info("[DASK PRELOAD] =========================================")
    except Exception as e:
        worker.deps = None
        worker.deps_error = str(e)
        logger.exception(f"[DASK PRELOAD] Failed to initialize worker dependencies: {e}")
        logger.error(f"[DASK PRELOAD] Worker will fail to start due to dependency initialization error")
        raise