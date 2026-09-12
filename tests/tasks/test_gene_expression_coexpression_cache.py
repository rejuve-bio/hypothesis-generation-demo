"""Tests for the per-tissue co-expression matrix cache in
src/tasks/gene_expression.py (get_coexpression_matrix_for_tissue and its
supporting _get_or_build_tissue_context / disk-cache helpers).

Builds a small synthetic CellxGene Census double (mocked, but with the same
call surface the real code uses against tiledbsoma/cellxgene_census) so we
can verify, without any network access:

  1. The correlation *math* still matches the original (pre-cache)
     algorithm — sign/magnitude for a synthetic positively- and
     negatively-correlated gene.
  2. The raw-count matrix is downloaded from "Census" exactly ONCE per
     tissue no matter how many genes are queried against it, including
     under concurrent access from multiple threads (the race-condition fix).
  3. A disk cache is written, and a *fresh* process (simulated by clearing
     the in-process cache) reuses it instead of re-downloading.
  4. Cache entries are correctly scoped per tissue.
"""
import concurrent.futures
import threading

import numpy as np
import pyarrow as pa
import pytest

import src.tasks.gene_expression as ge
from src.catlas_census_mapping import ResolvedCensusCellFilter


# ---------------------------------------------------------------------------
# Synthetic dataset: 200 cells, 6 genes.
# GENE_A: our gene of interest.
# GENE_B: engineered to correlate strongly positively with GENE_A.
# GENE_C: engineered to correlate strongly negatively with GENE_A.
# GENE_D/E/F: noise, uncorrelated.
# ---------------------------------------------------------------------------
N_CELLS = 200
GENE_IDS = ["GENE_A", "GENE_B", "GENE_C", "GENE_D", "GENE_E", "GENE_F"]


def _make_synthetic_counts(seed=42):
    rng = np.random.default_rng(seed)
    cell_sums_true = rng.integers(800, 1200, size=N_CELLS).astype(np.float32)

    gene_a = rng.integers(0, 50, size=N_CELLS).astype(np.float32)
    gene_b = np.clip(gene_a * 1.8 + rng.normal(0, 2, size=N_CELLS), 0, None)
    gene_c = np.clip((60 - gene_a) + rng.normal(0, 2, size=N_CELLS), 0, None)
    gene_d = rng.integers(0, 50, size=N_CELLS).astype(np.float32)
    gene_e = rng.integers(0, 50, size=N_CELLS).astype(np.float32)
    gene_f = rng.integers(0, 50, size=N_CELLS).astype(np.float32)

    counts = {
        "GENE_A": gene_a, "GENE_B": gene_b, "GENE_C": gene_c,
        "GENE_D": gene_d, "GENE_E": gene_e, "GENE_F": gene_f,
    }
    return counts, cell_sums_true


class FakeArrowResult:
    def __init__(self, df):
        self._df = df

    def concat(self):
        return self

    def to_pandas(self):
        return self._df.copy()


class FakeAxisQuery:
    def obs_joinids(self):
        return pa.array(np.arange(N_CELLS, dtype=np.int64))


class FakeTable:
    def __init__(self, dim0, dim1, data):
        self._d = {
            "soma_dim_0": pa.array(np.asarray(dim0, dtype=np.int64)),
            "soma_dim_1": pa.array(np.asarray(dim1, dtype=np.int64)),
            "soma_data": pa.array(np.asarray(data, dtype=np.float32)),
        }

    def __getitem__(self, key):
        return self._d[key]


class FakeCensusDouble:
    """Bundles the synthetic dataset + call counters + a fake Census
    experiment object graph, all scoped to one test (no shared globals)."""

    def __init__(self, seed=42, download_delay=0.0):
        self.counts, self.cell_sums_true = _make_synthetic_counts(seed)
        self.gene_joinids = {g: i + 100 for i, g in enumerate(GENE_IDS)}
        self.download_log = {"big_matrix_reads": 0, "open_soma_calls": 0, "single_gene_reads": 0}
        self.force_batch_failure = False
        self.download_delay = download_delay  # artificial delay to widen race windows in concurrency tests
        self._lock = threading.Lock()

    def open_soma(self, census_version):
        return _FakeCensusCtx(self)


class _FakeCensusCtx:
    def __init__(self, double: FakeCensusDouble):
        self._double = double

    def __enter__(self):
        with self._double._lock:
            self._double.download_log["open_soma_calls"] += 1
        return {"census_data": {"homo_sapiens": _FakeExperiment(self._double)}}

    def __exit__(self, *a):
        return False


class _FakeXRaw:
    def __init__(self, double: FakeCensusDouble):
        self._double = double

    def read(self, coords):
        double = self._double
        row_joinids, col_joinids = coords
        row_joinids = list(row_joinids)
        col_joinids = list(col_joinids)
        gene_by_joinid = {v: k for k, v in double.gene_joinids.items()}

        if double.download_delay:
            import time
            time.sleep(double.download_delay)

        with double._lock:
            if len(col_joinids) > 1:
                double.download_log["big_matrix_reads"] += 1
            else:
                double.download_log["single_gene_reads"] += 1

        dim0, dim1, data = [], [], []
        for cj in col_joinids:
            gname = gene_by_joinid[cj]
            counts = double.counts[gname]
            for rj in row_joinids:
                v = counts[rj]
                if v != 0:
                    dim0.append(rj)
                    dim1.append(cj)
                    data.append(v)

        class _Iter:
            def tables(self_inner):
                yield FakeTable(dim0, dim1, data)

        return _Iter()


class _FakeVar:
    def __init__(self, double: FakeCensusDouble):
        self._double = double

    def read(self, column_names, value_filter=None):
        import pandas as pd
        rows = [
            {
                "soma_joinid": self._double.gene_joinids[g],
                "feature_id": g,
                "feature_name": g,
                "n_measured_obs": N_CELLS,
            }
            for g in GENE_IDS
        ]
        df = pd.DataFrame(rows)
        if value_filter:
            import re as _re
            m = _re.match(r"feature_id == '(.*)'", value_filter)
            if m:
                df = df[df["feature_id"] == m.group(1)]
        return FakeArrowResult(df)


class _FakeObs:
    def __init__(self, double: FakeCensusDouble):
        self._double = double

    def read(self, coords, column_names):
        import pandas as pd
        joinids = coords[0]
        df = pd.DataFrame({
            "soma_joinid": joinids,
            "n_measured_vars": [self._double.cell_sums_true[j] for j in joinids],
        })
        return FakeArrowResult(df)


class _FakeMS:
    def __init__(self, double: FakeCensusDouble):
        self.var = _FakeVar(double)
        self.X = {"raw": _FakeXRaw(double)}

    def __getitem__(self, key):
        assert key == "RNA"
        return self


class _FakeExperiment:
    def __init__(self, double: FakeCensusDouble):
        self.ms = _FakeMS(double)
        self.obs = _FakeObs(double)

    def axis_query(self, measurement_name, obs_query):
        return FakeAxisQuery()


def _fake_resolve_ldsc_for_census(cell_type, **kwargs):
    return ResolvedCensusCellFilter(
        ldsc_name=cell_type,
        skip_coexpression=False,
        cell_type_labels=["fake_label"],
        cl_ids=["CL:0000000"],
        source="test",
    )


def _fake_axis_query_for_resolved(experiment, resolved):
    return FakeAxisQuery()


@pytest.fixture(autouse=True)
def _clear_tissue_caches():
    """Every test starts and ends with clean in-process caches, so tests
    don't leak state into each other."""
    with ge._TISSUE_CONTEXT_LOCK:
        ge._TISSUE_CONTEXT_CACHE.clear()
    with ge._TISSUE_LOCKS_GUARD:
        ge._TISSUE_LOCKS.clear()
    yield
    with ge._TISSUE_CONTEXT_LOCK:
        ge._TISSUE_CONTEXT_CACHE.clear()
    with ge._TISSUE_LOCKS_GUARD:
        ge._TISSUE_LOCKS.clear()


@pytest.fixture
def census_double(monkeypatch, tmp_path):
    """Patches cellxgene_census.open_soma, resolve_ldsc_for_census,
    _census_obs_axis_query_for_resolved and Config.from_env to use a fresh
    FakeCensusDouble + a tmp_path-backed disk cache, for one test."""
    double = FakeCensusDouble()

    fake_config = type("FakeConfig", (), {})()
    fake_config.census_cache_dir = str(tmp_path)
    fake_config.census_cache_max_bytes = 0  # no eviction unless a test opts in
    fake_config.data_dir = str(tmp_path)
    fake_config.repo_root = "."
    fake_config.catlas_celltype_cl_mapping_json = "x"
    fake_config.catlas_abc_aliases_tsv = "y"

    monkeypatch.setattr(ge.cellxgene_census, "open_soma", double.open_soma)
    monkeypatch.setattr(ge, "resolve_ldsc_for_census", _fake_resolve_ldsc_for_census)
    monkeypatch.setattr(ge, "_census_obs_axis_query_for_resolved", _fake_axis_query_for_resolved)
    monkeypatch.setattr(ge.Config, "from_env", classmethod(lambda cls: fake_config))

    return double, fake_config


def test_correlation_math_matches_original(census_double):
    """Correctness: positively/negatively correlated synthetic genes are
    identified with the right sign and a strong magnitude, matching the
    pre-cache algorithm's behavior."""
    double, _ = census_double
    top_pos, top_neg, all_genes = ge.get_coexpression_matrix_for_tissue.fn("GENE_A", "TissueT1", k=5)

    assert sorted(all_genes) == sorted(GENE_IDS)

    pos_by_gene = dict(top_pos)
    neg_by_gene = dict(top_neg)
    assert "GENE_B" in pos_by_gene
    assert "GENE_C" in neg_by_gene
    assert pos_by_gene["GENE_B"] > 0.8
    assert neg_by_gene["GENE_C"] < -0.7


def test_matrix_downloaded_once_per_tissue_across_multiple_genes(census_double):
    """The actual bug being fixed: querying several different genes against
    the same tissue must only download the matrix once."""
    double, _ = census_double

    ge.get_coexpression_matrix_for_tissue.fn("GENE_A", "TissueT1", k=5)
    assert double.download_log["big_matrix_reads"] == 1
    assert double.download_log["open_soma_calls"] == 1

    ge.get_coexpression_matrix_for_tissue.fn("GENE_D", "TissueT1", k=5)
    ge.get_coexpression_matrix_for_tissue.fn("GENE_E", "TissueT1", k=5)

    assert double.download_log["big_matrix_reads"] == 1
    assert double.download_log["open_soma_calls"] == 1


def test_disk_cache_reused_after_clearing_in_process_cache(census_double):
    """A fresh process (simulated by clearing the in-process cache) must
    reuse the on-disk cache instead of re-downloading."""
    double, fake_config = census_double

    ge.get_coexpression_matrix_for_tissue.fn("GENE_A", "TissueT1", k=5)
    assert double.download_log["big_matrix_reads"] == 1

    cached_files = list(__import__("pathlib").Path(fake_config.census_cache_dir).iterdir())
    assert any(f.name.endswith(".matrix.npz") for f in cached_files)
    assert any(f.name.endswith(".meta.json") for f in cached_files)

    with ge._TISSUE_CONTEXT_LOCK:
        ge._TISSUE_CONTEXT_CACHE.clear()

    ge.get_coexpression_matrix_for_tissue.fn("GENE_F", "TissueT1", k=5)
    assert double.download_log["big_matrix_reads"] == 1  # no re-download
    assert double.download_log["open_soma_calls"] == 1


def test_different_tissue_triggers_its_own_download(census_double):
    """Cache entries are scoped per tissue — a different tissue must not
    reuse another tissue's cached matrix."""
    double, _ = census_double

    ge.get_coexpression_matrix_for_tissue.fn("GENE_A", "TissueT1", k=5)
    ge.get_coexpression_matrix_for_tissue.fn("GENE_A", "TissueT2", k=5)

    assert double.download_log["big_matrix_reads"] == 2
    assert double.download_log["open_soma_calls"] == 2


def test_concurrent_calls_for_same_cold_tissue_download_exactly_once(monkeypatch, tmp_path):
    """Race-condition fix: several threads racing to query different genes
    against the same COLD tissue at once must still only download the
    matrix once (per-tissue lock around the whole load/build/save path)."""
    double = FakeCensusDouble(download_delay=0.05)  # widen the race window

    fake_config = type("FakeConfig", (), {})()
    fake_config.census_cache_dir = str(tmp_path)
    fake_config.census_cache_max_bytes = 0
    fake_config.data_dir = str(tmp_path)
    fake_config.repo_root = "."
    fake_config.catlas_celltype_cl_mapping_json = "x"
    fake_config.catlas_abc_aliases_tsv = "y"

    monkeypatch.setattr(ge.cellxgene_census, "open_soma", double.open_soma)
    monkeypatch.setattr(ge, "resolve_ldsc_for_census", _fake_resolve_ldsc_for_census)
    monkeypatch.setattr(ge, "_census_obs_axis_query_for_resolved", _fake_axis_query_for_resolved)
    monkeypatch.setattr(ge.Config, "from_env", classmethod(lambda cls: fake_config))

    genes = ["GENE_A", "GENE_D", "GENE_E", "GENE_F", "GENE_A"]
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(genes)) as executor:
        futures = [
            executor.submit(ge.get_coexpression_matrix_for_tissue.fn, gene, "TissueConcurrent", 5)
            for gene in genes
        ]
        for f in futures:
            f.result()

    assert double.download_log["big_matrix_reads"] == 1, (
        "expected exactly one matrix download across concurrent callers for the same tissue, "
        f"got {double.download_log['big_matrix_reads']}"
    )
    assert double.download_log["open_soma_calls"] == 1


def test_atomic_save_leaves_no_tmp_files_behind(census_double):
    """After a successful save, no .tmp-<uuid> artifacts should remain —
    only the final matrix/meta/obs/cellsums files."""
    _double, fake_config = census_double
    ge.get_coexpression_matrix_for_tissue.fn("GENE_A", "TissueT1", k=5)

    from pathlib import Path
    files = list(Path(fake_config.census_cache_dir).iterdir())
    assert files, "expected cache files to have been written"
    assert not any(".tmp-" in f.name for f in files), f"leftover tmp files: {files}"


def test_cache_eviction_removes_oldest_tissue_when_over_size_cap(monkeypatch, tmp_path):
    """When the cache directory exceeds census_cache_max_bytes, the
    least-recently-written tissue's cache entry should be evicted."""
    double = FakeCensusDouble()

    fake_config = type("FakeConfig", (), {})()
    fake_config.census_cache_dir = str(tmp_path)
    fake_config.data_dir = str(tmp_path)
    fake_config.repo_root = "."
    fake_config.catlas_celltype_cl_mapping_json = "x"
    fake_config.catlas_abc_aliases_tsv = "y"
    # No cap while writing the first tissue, so it isn't evicted the moment
    # it's written — we want it present as the "old" entry to be evicted
    # once the SECOND tissue pushes the cache over the (now-set) cap below.
    fake_config.census_cache_max_bytes = 0

    monkeypatch.setattr(ge.cellxgene_census, "open_soma", double.open_soma)
    monkeypatch.setattr(ge, "resolve_ldsc_for_census", _fake_resolve_ldsc_for_census)
    monkeypatch.setattr(ge, "_census_obs_axis_query_for_resolved", _fake_axis_query_for_resolved)
    monkeypatch.setattr(ge.Config, "from_env", classmethod(lambda cls: fake_config))

    ge.get_coexpression_matrix_for_tissue.fn("GENE_A", "TissueOld", k=5)

    from pathlib import Path
    one_tissue_size = sum(f.stat().st_size for f in Path(fake_config.census_cache_dir).iterdir())
    # Cap sized to fit exactly one tissue's cache entry but not two, so
    # writing the second tissue forces the first (older) one out.
    fake_config.census_cache_max_bytes = int(one_tissue_size * 1.5)
    with ge._TISSUE_CONTEXT_LOCK:
        ge._TISSUE_CONTEXT_CACHE.clear()
    ge.get_coexpression_matrix_for_tissue.fn("GENE_A", "TissueNew", k=5)

    from pathlib import Path
    remaining = list(Path(fake_config.census_cache_dir).glob("*.meta.json"))
    remaining_keys = {f.name[: -len(".meta.json")] for f in remaining}

    assert not any("TissueOld" in k for k in remaining_keys), (
        f"expected the older tissue's cache entry to be evicted, remaining: {remaining_keys}"
    )
    assert any("TissueNew" in k for k in remaining_keys), (
        f"expected the newer tissue's cache entry to survive, remaining: {remaining_keys}"
    )
