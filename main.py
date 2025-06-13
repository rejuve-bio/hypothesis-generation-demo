import argparse
from flask import Flask
from flask_restful import Api
from loguru import logger

from config import Config, create_dependencies
from logging_config import setup_logging
from api import (
    EnrichAPI, 
    HypothesisAPI, 
    ChatAPI, 
    init_socket_handlers
)
from dotenv import load_dotenv
import os
from flask_cors import CORS
from flask_jwt_extended import JWTManager
from socketio_instance import socketio
from status_tracker import StatusTracker

def parse_flask_arguments():
    """Parse arguments specific to Flask application"""
    parser = argparse.ArgumentParser(description="Flask Application Server")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--host", type=str, default="0.0.0.0")
    
    # Application configuration
    parser.add_argument("--embedding-model", type=str, default="w601sxs/b1ade-embed-kd")
    
    # Prolog arguments
    parser.add_argument("--swipl-host", type=str, default="localhost")
    parser.add_argument("--swipl-port", type=int, default=4242)
    
    # Data file arguments
    parser.add_argument("--ensembl-hgnc-map", type=str, required=True)
    parser.add_argument("--hgnc-ensembl-map", type=str, required=True)
    parser.add_argument("--go-map", type=str, required=True)
    
    return parser.parse_args()

def setup_api(config):
    """Setup Flask application with centralized configuration"""
    load_dotenv()
    
    app = Flask(__name__)

    # JWT Configuration
    app.config['JWT_SECRET_KEY'] = os.getenv("JWT_SECRET_KEY")
    app.config['JWT_TOKEN_LOCATION'] = ['headers']
    app.config['JWT_HEADER_NAME'] = 'Authorization'
    app.config['JWT_HEADER_TYPE'] = 'Bearer'

    # Initialize JWTManager
    jwt = JWTManager(app)
    CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True, allow_headers=["Content-Type", "Authorization"])
    api = Api(app)

    # Initialize SocketIO with the app
    socketio.init_app(app)

    # Create dependencies using centralized config
    deps = create_dependencies(config)
    
    # Initialize status tracker
    status_tracker = StatusTracker()
    status_tracker.initialize(deps['db'])
    try:
        hf_token = os.environ["HF_TOKEN"]
    except KeyError:
        hf_token = None
    # semantic_search = SemanticSearch(args.embedding_model, hf_token=hf_token)


    # Setup API endpoints with dependencies
    api.add_resource(EnrichAPI, "/enrich", 
        resource_class_kwargs={
            "enrichr": deps['enrichr'], 
            "llm": deps['llm'], 
            "prolog_query": deps['prolog_query'], 
            "db": deps['db']
        }
    )
    api.add_resource(HypothesisAPI, "/hypothesis", 
        resource_class_kwargs={
            "enrichr": deps['enrichr'], 
            "prolog_query": deps['prolog_query'], 
            "llm": deps['llm'], 
            "db": deps['db']
        }
    )
    api.add_resource(ChatAPI, "/chat", 
        resource_class_kwargs={"llm": deps['llm']}
    )

    # Initialize socket handlers 
    socket_namespace = init_socket_handlers(deps['db'])
    logger.info(f"Socket namespace initialized: {socket_namespace}")
    print(f"Socket namespace initialized: {socket_namespace}")
    
    return app, socketio


def main():
    """Main Flask application entry point"""
    # Parse Flask-specific arguments
    args = parse_flask_arguments()
    
    # Create configuration from arguments
    config = Config.from_args(args)
    
    
    setup_logging(log_level='INFO')  
    # Validate configuration
    if not all([config.ensembl_hgnc_map, config.hgnc_ensembl_map, config.go_map]):
        raise ValueError("Missing required configuration: ensembl_hgnc_map, hgnc_ensembl_map, go_map")
    
    logger.info("🚀 Starting Flask application...")
    logger.info(f"   - Host: {config.host}:{config.port}")

    
    # Setup application with configuration
    app, socketio = setup_api(config)

    # Start the application
    socketio.run(
        app, 
        host=config.host, 
        port=config.port, 
        debug=False,  # Disable debug mode to avoid WSGI issues
        use_reloader=False,
        allow_unsafe_werkzeug=True,
        log_output=True
    )
    

if __name__ == "__main__":
    main()
