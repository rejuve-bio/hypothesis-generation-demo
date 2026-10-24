from copy import deepcopy
from unittest.mock import MagicMock

from src.flows import enrichment as enrichment_flow_module
from src.flows import hypothesis as hypothesis_flow_module
from src.tasks import enrichment as enrichment_tasks
from src.tasks import hypothesis as hypothesis_tasks


class InMemoryEnrichment:
    def __init__(self):
        self.documents = {}

    def check_enrich(self, user_id, phenotype, variant):
        return any(
            doc["user_id"] == user_id
            and doc["phenotype"] == phenotype
            and doc["variant"] == variant
            for doc in self.documents.values()
        )

    def get_enrich_by_phenotype_and_variant(self, phenotype, variant, user_id=None):
        return next(
            (
                deepcopy(doc)
                for doc in self.documents.values()
                if doc["phenotype"] == phenotype
                and doc["variant"] == variant
                and (user_id is None or doc["user_id"] == user_id)
            ),
            None,
        )

    def create_enrich(
        self, user_id, project_id, variant, phenotype, causal_gene, go_terms, causal_graph
    ):
        enrich_id = f"enrich-{len(self.documents) + 1}"
        self.documents[enrich_id] = {
            "id": enrich_id,
            "user_id": user_id,
            "project_id": project_id,
            "variant": variant,
            "phenotype": phenotype,
            "causal_gene": causal_gene,
            "GO_terms": deepcopy(go_terms),
            "causal_graph": deepcopy(causal_graph),
        }
        return enrich_id

    def get_enrich(self, user_id=None, enrich_id=None):
        doc = self.documents.get(enrich_id)
        if doc and (user_id is None or doc["user_id"] == user_id):
            return deepcopy(doc)
        return None


class InMemoryHypotheses:
    def __init__(self):
        self.documents = {
            "hyp-1": {
                "id": "hyp-1", "user_id": "user-1", "project_id": "project-1",
                "variant": "rs16940186", "phenotype": "Ulcerative colitis",
                "status": "pending",
            }
        }

    def update_hypothesis(self, hypothesis_id, patch):
        self.documents[hypothesis_id].update(deepcopy(patch))
        return {"message": "updated"}, 200

    def get_hypotheses(self, user_id=None, hypothesis_id=None):
        if hypothesis_id:
            doc = self.documents.get(hypothesis_id)
            return deepcopy(doc) if doc and doc["user_id"] == user_id else None
        return [deepcopy(doc) for doc in self.documents.values() if doc["user_id"] == user_id]

    def check_hypothesis(self, user_id, enrich_id, go_id):
        return any(
            doc.get("user_id") == user_id
            and doc.get("enrich_id") == enrich_id
            and doc.get("go_id") == go_id
            for doc in self.documents.values()
        )

    def get_hypothesis_by_enrich_and_go(self, enrich_id, go_id, user_id=None):
        return next(
            (
                deepcopy(doc)
                for doc in self.documents.values()
                if doc.get("user_id") == user_id
                and doc.get("enrich_id") == enrich_id
                and doc.get("go_id") == go_id
            ),
            None,
        )


def test_variant_to_persisted_hypothesis_component_e2e(
    monkeypatch, immediate_task_factory, sample_graph
):
    enrichment = InMemoryEnrichment()
    hypotheses = InMemoryHypotheses()
    prolog = MagicMock()
    prolog.get_candidate_genes.return_value = ["ENSG00000140968"]
    prolog.get_relevant_gene_proof.return_value = {"response": [deepcopy(sample_graph)]}
    prolog.get_gene_ids.return_value = ["ENSG00000115415", "ENSG00000125347"]

    def execute(query):
        if query.startswith("maplist(variant_id"):
            return ["chr16_85990001_C_T"]
        if query.startswith("maplist(gene_name"):
            return ["IRF8"]
        if query.startswith("term_name"):
            return ["EFO_0000729"]
        raise AssertionError(f"Unexpected query: {query}")

    prolog.execute_query.side_effect = execute
    enrichr = MagicMock()
    enrichr.to_symbol.side_effect = lambda value: {
        "ENSG00000140968": "IRF8", "STAT1": "STAT1", "IRF1": "IRF1"
    }.get(str(value).strip("'\""), str(value).strip("'\"").upper())
    enrichr.to_ensembl_id.side_effect = lambda value: {
        "IRF8": "ensg00000140968"
    }.get(str(value).upper())
    enrichr.is_ensembl_id.side_effect = lambda value: str(value).upper().startswith("ENSG")
    enrichr.annotate_graph_gene_names.side_effect = deepcopy
    enrichr.run.return_value = [{"Term": "Inflammatory response"}]
    llm = MagicMock()
    llm.get_relevant_go.return_value = [
        {"id": "GO:0006954", "name": "inflammatory response", "genes": ["STAT1", "IRF1"]}
    ]
    llm.summarize_graph.return_value = "IRF8 may alter inflammatory signaling."
    gene_expression = MagicMock()
    gene_expression.get_tissue_selection.return_value = None
    gene_expression.get_ldsc_results_for_project.return_value = []
    deps = {
        "enrichment": enrichment,
        "hypotheses": hypotheses,
        "prolog_query": prolog,
        "enrichr": enrichr,
        "llm": llm,
        "gene_expression": gene_expression,
        "tasks": MagicMock(),
        "redis_url": "redis://unused",
    }

    monkeypatch.setattr(enrichment_flow_module.Config, "from_env", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr(hypothesis_flow_module.Config, "from_env", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr(enrichment_flow_module, "create_dependencies", lambda _config: deps)
    monkeypatch.setattr(hypothesis_flow_module, "create_dependencies", lambda _config: deps)
    monkeypatch.setattr(enrichment_tasks, "get_deps", lambda: deps)
    monkeypatch.setattr(hypothesis_tasks, "get_deps", lambda: deps)
    monkeypatch.setattr(enrichment_tasks, "emit_task_update", MagicMock())
    monkeypatch.setattr(hypothesis_tasks, "emit_task_update", MagicMock())
    monkeypatch.setattr(enrichment_flow_module, "emit_task_update", MagicMock())
    monkeypatch.setattr("src.services.status_tracker.StatusTracker", MagicMock())
    monkeypatch.setattr(enrichment_tasks.status_tracker, "get_history", lambda *_: [])
    monkeypatch.setattr(hypothesis_tasks.status_tracker, "get_history", lambda *_: [])

    for name in (
        "check_enrich", "get_candidate_genes", "get_relevant_gene_proof",
        "retry_get_relevant_gene_proof", "create_enrich_data",
    ):
        task = getattr(enrichment_tasks, name)
        monkeypatch.setattr(enrichment_flow_module, name, immediate_task_factory(task.fn))
    monkeypatch.setattr(
        enrichment_flow_module,
        "get_coexpression_matrix_for_tissue",
        immediate_task_factory(lambda *_args, **_kwargs: None),
    )
    for name in (
        "check_hypothesis", "get_enrich", "get_gene_ids", "execute_gene_query",
        "execute_variant_query", "execute_phenotype_query", "summarize_graph",
        "create_hypothesis",
    ):
        task = getattr(hypothesis_tasks, name)
        monkeypatch.setattr(hypothesis_flow_module, name, immediate_task_factory(task.fn))

    enrich_response = enrichment_flow_module.enrichment_flow.fn(
        "user-1", "Ulcerative colitis", "rs16940186", "hyp-1", "project-1", 11
    )
    assert enrich_response == ({"id": "enrich-1"}, 200)

    hypothesis_response, status = hypothesis_flow_module.hypothesis_flow.fn(
        "user-1", "hyp-1", "enrich-1", "GO:0006954"
    )
    assert status == 201
    assert hypothesis_response["summary"] == "IRF8 may alter inflammatory signaling."
    persisted = hypotheses.documents["hyp-1"]
    assert persisted["status"] == "Completed"
    assert persisted["variant"] == "rs16940186"
    assert persisted["graph"] == hypothesis_response["graph"]
    assert any(node["id"] == "EFO_0000729" for node in persisted["graph"]["nodes"])
