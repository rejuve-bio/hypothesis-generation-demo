"""Tests for the precomputed cell-type LDSC store (src/services/ldsc_precomputed.py)."""
import json
import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import src.services.ldsc_precomputed as pre


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
def config(tmp_path):
    cts = tmp_path / "cell_types.cts"
    cts.write_text("T_cell data/ldscores/T_cell.\n")
    return SimpleNamespace(
        ldsc_cts_file=str(cts),
        ldsc_w_ld_prefix="data/OSF/weights/weights.hm3_noMHC.",
        ldsc_w_hm3_snplist="data/ldsc/data/w_hm3.snplist",
        get_ldsc_ref_ld_chr=lambda: "baseline.,ccre.",
        resolve_ldsc_path=lambda p: p,
    )


@pytest.fixture
def results_file(tmp_path):
    path = tmp_path / "results.txt"
    path.write_text(RESULTS)
    return str(path)


def test_fingerprint_is_stable_and_tracks_references(config, monkeypatch):
    fp = pre.reference_fingerprint(config)
    assert fp == pre.reference_fingerprint(config)

    with open(config.ldsc_cts_file, "a") as fh:
        fh.write("B_cell data/ldscores/B_cell.\n")
    fp_cts = pre.reference_fingerprint(config)
    assert fp_cts != fp

    monkeypatch.setattr(pre, "LDSC_PRECOMPUTE_VERSION", pre.LDSC_PRECOMPUTE_VERSION + 1)
    assert pre.reference_fingerprint(config) != fp_cts


def test_publish_uploads_results_before_manifest(results_file):
    storage = FakeStorage()
    assert pre.publish_precomputed(storage, "FINNGEN_R12_X", 1000, "fp", results_file, "25.09")

    assert storage.uploads == [
        "ldsc-precomputed/fp/FINNGEN_R12_X/cell_type_results.txt",
        "ldsc-precomputed/fp/FINNGEN_R12_X/manifest.json",
    ]
    manifest = json.loads(storage.objects[storage.uploads[1]])
    assert manifest["n"] == 1000
    assert manifest["row_count"] == 1
    assert manifest["ot_release"] == "25.09"
    assert pre.has_precomputed(storage, "FINNGEN_R12_X", "fp")


def test_fetch_hit(results_file, tmp_path):
    storage = FakeStorage()
    pre.publish_precomputed(storage, "S", 1000, "fp", results_file)
    dest = tmp_path / "out" / "x.cell_type_results.txt"
    os.makedirs(dest.parent)

    assert pre.fetch_precomputed(storage, "S", 1000, "fp", str(dest))
    assert dest.read_text() == RESULTS


def test_fetch_miss_without_manifest(tmp_path):
    assert not pre.fetch_precomputed(FakeStorage(), "S", 1000, "fp", str(tmp_path / "x"))


def test_fetch_miss_on_other_fingerprint(results_file, tmp_path):
    storage = FakeStorage()
    pre.publish_precomputed(storage, "S", 1000, "fp", results_file)
    assert not pre.fetch_precomputed(storage, "S", 1000, "other", str(tmp_path / "x"))


def test_fetch_miss_on_sample_size_mismatch(results_file, tmp_path):
    storage = FakeStorage()
    pre.publish_precomputed(storage, "S", 1000, "fp", results_file)
    dest = tmp_path / "x"

    assert not pre.fetch_precomputed(storage, "S", 2000, "fp", str(dest))
    assert not dest.exists()


def test_fetch_miss_on_storage_error(tmp_path):
    storage = MagicMock()
    storage.exists.side_effect = RuntimeError("minio down")
    assert not pre.fetch_precomputed(storage, "S", 1000, "fp", str(tmp_path / "x"))
