"""Sample size (N) handling in harmonize_sumstats_with_nextflow's post-process."""

import gzip
import shutil
import subprocess
from pathlib import Path

import pandas as pd
import pytest

import src.db.credible_sets_handler as credible_sets_handler
from src.tasks import analysis

SCRIPT_DIR = Path(__file__).resolve().parents[2] / "scripts" / "1000Genomes_phase3"

pytestmark = pytest.mark.skipif(
    shutil.which("bgzip") is None, reason="the harmonize post-process needs bgzip"
)

SSF_HEADER = ["chromosome", "base_pair_location", "effect_allele", "other_allele",
              "beta", "standard_error", "p_value", "effect_allele_frequency", "rsid"]
SSF_ROW = ["1", "100", "A", "G", "0.1", "0.01", "1e-9", "0.2", "rs1"]


class _KnownStudy:
    def is_study_known_to_opentargets(self, study_id):
        return True

    def close(self):
        pass


class _Config:
    def get_harmonizer_ref_dir(self, ref_genome):
        return "/unused"


@pytest.fixture
def run_harmonize(monkeypatch, tmp_path):
    """Run the OpenTargets path, which skips Nextflow but keeps the N post-process."""
    monkeypatch.setattr(analysis, "get_deps", lambda: {"storage": None})
    monkeypatch.setattr(analysis.Config, "from_env", staticmethod(lambda: _Config()))
    monkeypatch.setattr(credible_sets_handler, "CredibleSetsHandler", _KnownStudy)

    def run(src, sample_size=10000):
        out = analysis.harmonize_sumstats_with_nextflow.fn(
            str(src), str(tmp_path / "out"), sample_size=sample_size,
            script_dir=str(SCRIPT_DIR), code_repo="/unused",
            opentargets_study_id="GCST000001",
        )
        return pd.read_csv(out, sep="\t", compression="gzip")

    return run


def _write_bgzipped(path, header, rows):
    text = "\n".join("\t".join(r) for r in [header, *rows]) + "\n"
    raw = path.with_suffix("")
    raw.write_text(text)
    subprocess.run(["bgzip", "-f", str(raw)], check=True)
    return path


def test_lowercase_n_is_used_as_n(tmp_path, run_harmonize):
    src = _write_bgzipped(tmp_path / "study.tsv.gz", [*SSF_HEADER, "n"], [[*SSF_ROW, "4321"]])

    df = run_harmonize(src)

    assert "n" not in df.columns
    assert df["N"].tolist() == [4321]


def test_uppercase_n_is_kept(tmp_path, run_harmonize):
    src = _write_bgzipped(tmp_path / "study.tsv.gz", [*SSF_HEADER, "N"], [[*SSF_ROW, "4321"]])

    df = run_harmonize(src)

    assert df["N"].tolist() == [4321]


def test_fallback_sample_size_without_an_n_column(tmp_path, run_harmonize):
    src = _write_bgzipped(tmp_path / "study.tsv.gz", SSF_HEADER, [SSF_ROW])

    df = run_harmonize(src, sample_size=10000)

    assert df["N"].tolist() == [10000]


def test_sample_size_survives_the_column_remap(tmp_path, run_harmonize):
    header = ["CHR", "BP", "SNP", "A1", "A2", "FRQ", "BETA", "SE", "P", "OBS_CT"]
    row = ["1", "100", "rs1", "A", "G", "0.2", "0.1", "0.01", "1e-9", "4321"]
    src = _write_bgzipped(tmp_path / "plink.tsv.gz", header, [row])

    df = run_harmonize(src)

    assert df["N"].tolist() == [4321]
