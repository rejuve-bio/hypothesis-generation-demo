from loguru import logger
import requests


class PrologServiceError(RuntimeError):
    """The Prolog HTTP service could not be reached or returned an unusable
    response (connection failure, non-2xx status, or invalid JSON) -- as
    opposed to a reachable service cleanly reporting "no match"."""

    def __init__(self, message: str) -> None:
        super().__init__(message)

    def as_detail(self) -> dict[str, str]:
        return {"error_type": "prolog_service_unavailable", "message": str(self)}


class PrologNoEvidenceError(ValueError):
    """The Prolog service was reachable and responded normally, but found no
    causal-gene evidence for the given variant."""

    def __init__(self, message: str, *, variant: str | None = None) -> None:
        super().__init__(message)
        self.variant = variant

    def as_detail(self) -> dict[str, str]:
        detail = {"error_type": "prolog_no_evidence", "message": str(self)}
        if self.variant:
            detail["variant"] = self.variant
        return detail


class PrologQuery:

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.server = f"http://{self.host}:{self.port}"

    def _get(self, path: str, params: dict, *, action: str):
        """GET against the Prolog server, converting connection-level
        failures (server down, DNS, timeout) into PrologServiceError so
        callers only ever have to deal with one "service unavailable"
        exception type regardless of what went wrong at the transport
        layer."""
        try:
            return requests.get(f"{self.server}{path}", params=params)
        except requests.exceptions.RequestException as exc:
            logger.error(f"Prolog server unreachable for {action}: {exc}")
            raise PrologServiceError(
                f"{action} failed. Prolog server is unreachable: {exc}"
            ) from exc

    def get_candidate_genes(self, variant_id):
        """
        Given a SNP, get candidate genes that are proximal to it.
        """
        logger.info(f"Getting candidate genes for variant {variant_id}")
        payload = {"rsid": variant_id}
        res = self._get(
            "/api/hypgen/candidate_genes", payload, action="get_candidate_genes"
        )
        if not res.ok:
            logger.error(f"Prolog server error for variant {variant_id}: {res.status_code} - {res.text}")
            raise PrologServiceError(f"get_candidate_genes failed. Prolog server response: {res.text}")

        try:
            result = res.json()
            genes = [g.upper() for g in result["candidate_genes"]]
            return genes
        except requests.exceptions.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response from Prolog server for variant {variant_id} with response text: {res.text}")
            raise PrologServiceError(f"get_candidate_genes failed. Invalid JSON response from Prolog server. Response: {res.text[:500]}") from e

    def get_relevant_gene_proof(self, variant_id, seed, samples):
        payload = {"rsid": variant_id, "seed": seed,  "samples": samples}
        res = self._get("/api/hypgen", payload, action="get_relevant_gene_proof")
        if not res.ok:
            logger.error(f"Prolog server error for variant {variant_id}: {res.status_code} - {res.text}")
            raise PrologServiceError(f"get_relevant_gene_proof failed. Prolog server response: {res.text}")

        try:
            result = res.json()
            logger.info(f"Found {len(result) if isinstance(result, list) else 'unknown'} gene proof items")
            return result
        except requests.exceptions.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response for variant {variant_id} with response text: {res.text}")
            raise PrologServiceError(f"get_relevant_gene_proof failed. Invalid JSON response from Prolog server. Response: {res.text[:500]}") from e

    def execute_query(self, query):
        logger.info(f"Executing Prolog query: {query}")
        payload = {"query": query}

        res = self._get("/api/query", payload, action="execute_query")
        if not res.ok:
            logger.error(f"Prolog server error for query '{query}': {res.status_code} - {res.text}")
            raise PrologServiceError(f"execute_query failed. Prolog server response: {res.text}")

        try:
            result = res.json()

            # Check if the response contains an error from Prolog
            if isinstance(result, dict) and 'error' in result:
                logger.error(f"Prolog query error for '{query}': {result['error']}")
                raise PrologServiceError(f"Prolog query failed: {result['error']}")

            return result
        except requests.exceptions.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response for query '{query}' with response text: {res.text}")
            raise PrologServiceError(f"Invalid JSON from Prolog server. Response: {res.text[:500]}") from e
    
    def get_gene_ids(self, gene_names):

        logger.info(f"Getting gene IDs for genes: {gene_names}")
        
        # Query each gene name individually
        gene_ids = []
        for gene_name in gene_names:
            query = f"gene_id('{gene_name}', X)"
            try:
                result = self.execute_query(query)
                if result and len(result) > 0:
                    gene_ids.append(result[0])
                else:
                    logger.warning(f"No gene ID found for gene name: {gene_name}")
                    gene_ids.append(gene_name)  
            except Exception as e:
                logger.error(f"Error getting gene ID for {gene_name}: {e}")
                gene_ids.append(gene_name)
        
        return gene_ids