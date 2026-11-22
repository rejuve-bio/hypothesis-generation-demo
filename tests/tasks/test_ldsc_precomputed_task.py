"""Tests for run_combined_ldsc_tissue_analysis using precomputed LDSC results
(src/services/ldsc_precomputed.py) instead of running LDSC."""
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import src.services.ldsc_precomputed as pre
import src.tasks.gene_expression as ge

class FakeStorage:
    def __init__(self):
        self.objects = {}
        self.uploads = []

    def exists(self, key):
        return key in self.objects

    def upload_file(self, path, key):
        with open(path) as fh:
            self.objects[key] = fh.read()
        self.uploads.append(key)
        return True

    def upload_string(self, data, key):
        self.objects[key] = data
        self.uploads.append(key)
        return True

    def download_string(self, key):
        return self.objects.get(key)

    def download_file(self, key, path):
        if key not in self.objects:
            return False
        with open(path, "w") as fh:
            fh.write(self.objects[key])
        return True


RESULTS = "Name\tCoefficient\tCoefficient_std_error\tCoefficient_P_value\nT_cell\t1e-8\t2e-9\t1e-6\n"


@pytest.fixture
def results_file(tmp_path):
    path = tmp_path / "results.txt"
    path.write_text(RESULTS)
    return str(path)


@pytest.fixture
def ldsc_task(monkeypatch, tmp_path):
    """run_combined_ldsc_tissue_analysis with LDSC, mapping and DB mocked out."""
    gene_expression = MagicMock()
    gene_expression.create_gene_expression_run.return_value = "run-1"
    storage = FakeStorage()
    monkeypatch.setattr(ge, "get_deps", lambda: {"gene_expression": gene_expression, "storage": storage})
    monkeypatch.setattr(ge.Config, "from_env", staticmethod(lambda: object()))
    monkeypatch.setattr(ge, "reference_fingerprint", lambda config: "fp")

    def fake_run_ldsc(ldsc_dir, gwas_file, output_prefix):
        with open(f"{output_prefix}.cell_type_results.txt", "w") as fh:
            fh.write(RESULTS)
        return True

    run_ldsc = MagicMock(side_effect=fake_run_ldsc)
    monkeypatch.setattr(ge, "run_ldsc_analysis", run_ldsc)
    monkeypatch.setattr(ge, "setup_ldsc_environment", lambda d: "/opt/ldsc")
    # Call the real ranking function directly: invoking a Prefect task outside a
    # flow starts a background API server, whose threads break fork-based tests.
    monkeypatch.setattr(ge, "process_ldsc_results", ge.process_ldsc_results.fn)
    monkeypatch.setattr(ge, "map_tissues_to_cellxgene", lambda tissues: {
        t: {"cell_type": t, "census_skip_coexpression": False} for t in tissues
    })

    def run(**kwargs):
        return ge.run_combined_ldsc_tissue_analysis.fn(
            "harmonized.tsv.gz", str(tmp_path), "project-1", "user-1", **kwargs
        )

    return SimpleNamespace(run=run, run_ldsc=run_ldsc, storage=storage, db=gene_expression)


def test_task_uses_precomputed_results(ldsc_task, results_file):
    pre.publish_precomputed(ldsc_task.storage, "FINNGEN_R12_X", 1000, "fp", results_file)

    result = ldsc_task.run(opentargets_study_id="FINNGEN_R12_X", sample_size=1000)

    ldsc_task.run_ldsc.assert_not_called()
    assert result["ldsc_source"] == "precomputed"
    saved = ldsc_task.db.save_ldsc_results.call_args.args[1]
    assert [r["Name"] for r in saved] == ["T_cell"]
    ldsc_task.db.update_gene_expression_run_status.assert_called_with("run-1", "ldsc_tissue_completed")


def test_task_runs_ldsc_on_miss(ldsc_task):
    result = ldsc_task.run(opentargets_study_id="FINNGEN_R12_X", sample_size=1000)

    ldsc_task.run_ldsc.assert_called_once()
    assert result["ldsc_source"] == "computed"


def test_task_runs_ldsc_without_study_id(ldsc_task, results_file):
    pre.publish_precomputed(ldsc_task.storage, "FINNGEN_R12_X", 1000, "fp", results_file)

    result = ldsc_task.run()

    ldsc_task.run_ldsc.assert_called_once()
    assert result["ldsc_source"] == "computed"
