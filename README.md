# hypothesis-generation

## Overview

This project is designed to generate hypotheses for the causal relationship between genetic variants and phenotypes using various bioinformatics tools and machine learning models. The system integrates data from multiple sources, performs enrichment analysis, and uses a large language model (LLM) to summarize and generate hypotheses.

### Prerequisites

- Python 3.8+
- MongoDB
- SWI-Prolog
- Docker and Docker Compose

### Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/rejuve-bio/hypothesis-generation-demo.git
    cd hypothesis-generation
    ```

2. Compile the Prolog files (this step assumes you have access to the Prolog files):
    ```sh
    python compile_kb.py --config-path config/kb.yaml --compile-script pl/compile.pl --path-prefix /mnt/d2_nfs/wondwossen/prolog_out_v2 --hook-script pl/hook.pl
    ```

    To compile the files under `prolog_out_v3`, change the `--path-prefix` argument as follows:
    ```sh
    python compile_kb.py --config-path config/kb.yaml --compile-script pl/compile.pl --path-prefix /mnt/d2_nfs/wondwossen/prolog_out_v3 --hook-script pl/hook.pl
    ```

3. Set up environment variables:
    Create a `.env` file in the root directory and add the following variables:
    ```env
    MONGODB_URI=<your_mongodb_uri>
    DB_NAME=<your_db_name>
    OPENAI_API_KEY=<your_openai_api_key>
    ANTHROPIC_API_KEY=<your_anthropic_api_key>
    HF_TOKEN=<your_huggingface_token>
    JWT_SECRET_KEY=<your_jwt_secret_key>
    ```

4. Run the Docker containers:
    ```sh
    sudo docker-compose up --build
    ```

5. Once the containers are running, the Flask app, Prolog service, and MongoDB service will be accessible on ports 5000, 4242, and 27017, respectively.
