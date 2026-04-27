from __future__ import annotations

import argparse
import asyncio
from contextlib import asynccontextmanager

import socketio as python_socketio
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from src.config import Config, create_dependencies
from src.logging_config import setup_logging
from src.socketio_instance import sio
from src.services.socketio_relay import relay_subscribe_forever
from src.services.status_tracker import StatusTracker
from src.api import router
from src.api.dependencies import init_deps



def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FastAPI + Socket.IO Server")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--host", type=str, default="0.0.0.0")
    parser.add_argument("--embedding-model", type=str, default="w601sxs/b1ade-embed-kd")
    parser.add_argument("--swipl-host", type=str, default="localhost")
    parser.add_argument("--swipl-port", type=int, default=4242)
    parser.add_argument("--ensembl-hgnc-map", type=str, required=True)
    parser.add_argument("--hgnc-ensembl-map", type=str, required=True)
    parser.add_argument("--go-map", type=str, required=True)
    return parser.parse_args()


def create_app_with_config() -> python_socketio.ASGIApp:
    load_dotenv()
    config = Config.from_env()
    if not all([config.ensembl_hgnc_map, config.hgnc_ensembl_map, config.go_map]):
        raise ValueError(
            "Missing required configuration: ENSEMBL_HGNC_MAP, HGNC_ENSEMBL_MAP, GO_MAP"
        )
    return create_app(config)


def create_app(config: Config) -> python_socketio.ASGIApp:
    """Build and return the combined ASGI application."""
    load_dotenv()

    setup_logging(log_level="INFO")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        deps = create_dependencies(config)
        status_tracker = StatusTracker()
        status_tracker.initialize(deps["tasks"], redis_url=deps["redis_url"])
        init_deps(deps)


        relay_task: asyncio.Task[None] | None = None
        redis_url = deps.get("redis_url")
        if redis_url:
            relay_task = asyncio.create_task(relay_subscribe_forever(redis_url, sio))
            logger.info("Socket.IO Redis relay subscriber started")
        else:
            logger.warning("REDIS_URL not set - live Socket.IO relay from workers is disabled")

        logger.info("Application dependencies initialized")
        try:
            yield
        finally:
            if relay_task is not None:
                relay_task.cancel()
                try:
                    await relay_task
                except asyncio.CancelledError:
                    pass
        logger.info("Application shutting down")

    fastapi_app = FastAPI(
        title="Hypothesis Generation API",
        version="0.1.0",
        lifespan=lifespan,
    )

    fastapi_app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    fastapi_app.include_router(router)

    combined_app = python_socketio.ASGIApp(sio, fastapi_app)
    return combined_app


def main() -> None:
    args = parse_arguments()
    config = Config.from_args(args)

    if not all([config.ensembl_hgnc_map, config.hgnc_ensembl_map, config.go_map]):
        raise ValueError(
            "Missing required configuration: ensembl_hgnc_map, hgnc_ensembl_map, go_map"
        )

    setup_logging(log_level="INFO")
    logger.info(f"Starting FastAPI + Socket.IO on {config.host}:{config.port}")

    app = create_app(config)

    uvicorn.run(
        app,
        host=config.host,
        port=config.port,
        workers=1,
        log_level="info",
        log_config=None,
    )


if __name__ == "__main__":
    main()
