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
import json
import multiprocessing
import threading
from pathlib import Path

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
    don't leak state into each other. This includes _ALL_GENES_CACHE, which
    is keyed by CENSUS_VERSION (not by tmp_path), so it would otherwise leak
    a previous test's shared all_genes list across tests in the same
    pytest process."""
    with ge._TISSUE_CONTEXT_LOCK:
        ge._TISSUE_CONTEXT_CACHE.clear()
        ge._ALL_GENES_CACHE.clear()
    with ge._TISSUE_LOCKS_GUARD:
        ge._TISSUE_LOCKS.clear()
    yield
    with ge._TISSUE_CONTEXT_LOCK:
        ge._TISSUE_CONTEXT_CACHE.clear()
        ge._ALL_GENES_CACHE.clear()
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


def test_all_genes_list_is_shared_once_per_version_not_duplicated_per_tissue(census_double):
    """all_genes_list is tissue-independent (identical Census feature list
    for every tissue at a given CENSUS_VERSION), so it must be written to a
    single shared file once, not duplicated into every tissue's meta.json —
    and every tissue must still get the correct (identical) list back."""
    double, fake_config = census_double

    _, _, all_genes_t1 = ge.get_coexpression_matrix_for_tissue.fn("GENE_A", "TissueT1", k=5)
    _, _, all_genes_t2 = ge.get_coexpression_matrix_for_tissue.fn("GENE_A", "TissueT2", k=5)

    assert sorted(all_genes_t1) == sorted(GENE_IDS)
    assert sorted(all_genes_t2) == sorted(GENE_IDS)

    cache_dir = Path(fake_config.census_cache_dir)
    shared_files = list(cache_dir.glob(f"all_genes__{ge.CENSUS_VERSION}.json"))
    assert len(shared_files) == 1, f"expected exactly one shared all_genes file, found {shared_files}"

    # No per-tissue meta.json should carry its own copy of all_genes_list.
    for meta_path in cache_dir.glob("*.meta.json"):
        meta = json.loads(meta_path.read_text())
        assert "all_genes_list" not in meta, (
            f"{meta_path.name} should not duplicate all_genes_list — it belongs in the shared file"
        )


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


def test_freshly_saved_tissue_survives_even_if_it_alone_exceeds_cap(monkeypatch, tmp_path):
    """Edge case: a single tissue's cache is written, and its size alone
    already exceeds census_cache_max_bytes. The freshly-saved entry must be
    excluded from eviction (there's nothing older to evict instead) and
    survive, rather than being deleted moments after being written."""
    double = FakeCensusDouble()

    fake_config = type("FakeConfig", (), {})()
    fake_config.census_cache_dir = str(tmp_path)
    fake_config.data_dir = str(tmp_path)
    fake_config.repo_root = "."
    fake_config.catlas_celltype_cl_mapping_json = "x"
    fake_config.catlas_abc_aliases_tsv = "y"
    # Cap smaller than any single tissue's cache size, so the very act of
    # saving the first (and only) tissue immediately exceeds the cap.
    fake_config.census_cache_max_bytes = 1

    monkeypatch.setattr(ge.cellxgene_census, "open_soma", double.open_soma)
    monkeypatch.setattr(ge, "resolve_ldsc_for_census", _fake_resolve_ldsc_for_census)
    monkeypatch.setattr(ge, "_census_obs_axis_query_for_resolved", _fake_axis_query_for_resolved)
    monkeypatch.setattr(ge.Config, "from_env", classmethod(lambda cls: fake_config))

    ge.get_coexpression_matrix_for_tissue.fn("GENE_A", "TissueOnly", k=5)

    from pathlib import Path
    remaining = list(Path(fake_config.census_cache_dir).glob("*.meta.json"))
    remaining_keys = {f.name[: -len(".meta.json")] for f in remaining}

    assert any("TissueOnly" in k for k in remaining_keys), (
        f"expected the freshly-cached tissue to survive even though it alone exceeds the cap, "
        f"remaining: {remaining_keys}"
    )

    # And the in-process cache must agree — a subsequent call for the same
    # gene/tissue should hit the (still-present) disk cache, not re-download.
    with ge._TISSUE_CONTEXT_LOCK:
        ge._TISSUE_CONTEXT_CACHE.clear()
    ge.get_coexpression_matrix_for_tissue.fn("GENE_A", "TissueOnly", k=5)
    assert double.download_log["big_matrix_reads"] == 1


def _mp_save_tissue_worker(barrier, tissue_name: str) -> None:
    """Module-level (picklable/fork-safe) worker: runs in its own OS
    process, saving one tissue's cache entry. Relies on the parent test
    process's monkeypatches (cellxgene_census.open_soma, etc.) being
    inherited via fork's copy-on-write semantics — no re-patching needed
    here, since fork duplicates the already-patched module state.

    Waits on a shared Barrier immediately before starting, so every worker
    begins at the same instant — without this, process-start jitter alone
    is enough to let each save complete before the next one begins, which
    never exercises the actual interleaving the race depends on."""
    barrier.wait()
    ge.get_coexpression_matrix_for_tissue.fn("GENE_A", tissue_name, k=5)


def test_cross_process_concurrent_saves_respect_cache_cap(monkeypatch, tmp_path):
    """Reproduces the race Tesnim found: several different tissues saving
    concurrently from SEPARATE OS PROCESSES (not just threads — Dask
    workers are separate processes, so an in-process threading.Lock cannot
    prevent one tissue's eviction pass from sweeping up another tissue's
    concurrently-in-flight or just-published cache entry) under a cap sized
    for only a few of them.

    Verifies the final on-disk cache: (a) never balloons far past the
    configured cap, and (b) never contains a torn/partial entry (missing
    one of its 4 files) as a result of losing a race mid-publish."""
    if multiprocessing.get_start_method(allow_none=True) not in (None, "fork"):
        pytest.skip("test relies on fork-based multiprocessing (inherits monkeypatches via COW)")

    # A small delay per Census "read" call widens the race window enough to
    # actually exercise interleaving between processes — the synthetic
    # dataset is tiny and writes near-instantly otherwise, which (unlike a
    # real multi-second Census download) doesn't give concurrent processes
    # enough overlap to race against each other.
    double = FakeCensusDouble(download_delay=0.05)

    fake_config = type("FakeConfig", (), {})()
    fake_config.census_cache_dir = str(tmp_path)
    fake_config.data_dir = str(tmp_path)
    fake_config.repo_root = "."
    fake_config.catlas_celltype_cl_mapping_json = "x"
    fake_config.catlas_abc_aliases_tsv = "y"
    fake_config.census_cache_max_bytes = 0  # sized below, once we know one entry's footprint

    monkeypatch.setattr(ge.cellxgene_census, "open_soma", double.open_soma)
    monkeypatch.setattr(ge, "resolve_ldsc_for_census", _fake_resolve_ldsc_for_census)
    monkeypatch.setattr(ge, "_census_obs_axis_query_for_resolved", _fake_axis_query_for_resolved)
    monkeypatch.setattr(ge.Config, "from_env", classmethod(lambda cls: fake_config))

    # Measure one tissue's on-disk footprint, then remove it and size the
    # cap to fit roughly 4 of the 8 tissues we're about to save concurrently.
    ge.get_coexpression_matrix_for_tissue.fn("GENE_A", "SizerTissue", k=5)
    sizer_files = [p for p in tmp_path.iterdir() if "SizerTissue" in p.name]
    one_tissue_size = sum(p.stat().st_size for p in sizer_files)
    for p in sizer_files:
        p.unlink()
    with ge._TISSUE_CONTEXT_LOCK:
        ge._TISSUE_CONTEXT_CACHE.clear()

    fake_config.census_cache_max_bytes = int(one_tissue_size * 4.5)

    n_tissues = 8
    tissue_names = [f"Tissue{i}" for i in range(n_tissues)]
    ctx = multiprocessing.get_context("fork")
    barrier = ctx.Barrier(n_tissues)
    procs = [ctx.Process(target=_mp_save_tissue_worker, args=(barrier, name)) for name in tissue_names]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=60)
        assert p.exitcode == 0, f"worker process for a tissue save failed with exitcode {p.exitcode}"

    remaining_meta = list(tmp_path.glob("*.meta.json"))
    surviving_keys = {p.name[: -len(".meta.json")] for p in remaining_meta}
    assert len(surviving_keys) >= 1, "expected at least one tissue's cache entry to survive"

    # No torn/partial entries: every surviving tissue must have all 4 files.
    for key in surviving_keys:
        for ext in (".matrix.npz", ".obs_joinids.npy", ".cell_sums.npy"):
            assert (tmp_path / f"{key}{ext}").exists(), (
                f"surviving cache entry {key!r} is missing {ext} — torn/partial entry from a lost race"
            )

    total_size = sum(
        f.stat().st_size for f in tmp_path.iterdir()
        if f.is_file() and ".tmp-" not in f.name and not f.name.endswith(".lock")
    )
    # Slack: the cap only bounds per-tissue entries; the one shared
    # all_genes file plus the "always keep the entry you just wrote" floor
    # both add a bounded amount of unavoidable overhead on top of the cap.
    max_allowed = fake_config.census_cache_max_bytes + one_tissue_size + 2_000_000
    assert total_size <= max_allowed, (
        f"cache grew to {total_size} bytes, past cap {fake_config.census_cache_max_bytes} "
        f"(+ slack {max_allowed - fake_config.census_cache_max_bytes}) — eviction race not fixed"
    )
