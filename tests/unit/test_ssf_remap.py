"""Tests for the GWAS-SSF column remap in scripts/1000Genomes_phase3/ssf_remap.sh."""

import gzip
import shutil
import subprocess
from pathlib import Path

import pytest

REMAP_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "1000Genomes_phase3" / "ssf_remap.sh"

pytestmark = pytest.mark.skipif(
    shutil.which("bgzip") is None, reason="ssf_remap.sh needs bgzip"
)

PLINK_HEADER = ["CHR", "BP", "SNP", "A1", "A2", "FRQ", "BETA", "SE", "P"]


def _plink_rows(n):
    return [
        [str(i % 22 + 1), str(1000 + i), f"rs{i}", "A", "G", "0.2", "0.1", "0.01", "1e-9"]
        for i in range(1, n + 1)
    ]


def _write(path, header, rows, sep="\t", compress=False):
    text = "\n".join(sep.join(r) for r in [header, *rows]) + "\n"
    if compress:
        with gzip.open(path, "wt") as f:
            f.write(text)
    else:
        path.write_text(text)


def _run_remap(src):
    return subprocess.run(
        ["bash", "-c", f"source {REMAP_SCRIPT} && to_gwas_ssf"],
        env={"PATH": "/usr/bin:/bin:/usr/local/bin", "SUMSTATS": str(src),
             "SUMSTATS_DIR": str(src.parent), "BUILD": "GRCh37"},
        capture_output=True, text=True,
    )


def _read_ssf(path):
    with gzip.open(path, "rt") as f:
        lines = f.read().splitlines()
    header = lines[0].split("\t")
    return [dict(zip(header, line.split("\t"))) for line in lines[1:]]


@pytest.mark.parametrize("name, compress", [
    ("study.tsv", False),
    ("study.tsv.gz", True),
])
def test_remap_keeps_every_row(tmp_path, name, compress):
    # Large enough that the input would be truncated mid-read if the output
    # were written straight over an uncompressed <stem>.tsv input.
    src = tmp_path / name
    _write(src, PLINK_HEADER, _plink_rows(20000), compress=compress)

    result = _run_remap(src)

    assert result.returncode == 0, result.stderr
    out = Path(result.stdout.strip().splitlines()[-1])
    assert out == tmp_path / "study.tsv.gz"
    assert len(_read_ssf(out)) == 20000
    assert not list(tmp_path.glob("*.remap.*"))


def test_remap_keeps_columns_aligned_when_a_cell_is_empty(tmp_path):
    src = tmp_path / "study.tsv"
    _write(src, PLINK_HEADER, [["1", "100", "rs1", "C", "G", "", "0.0282", "0.0083", "0.0004"]])

    result = _run_remap(src)

    assert result.returncode == 0, result.stderr
    (row,) = _read_ssf(tmp_path / "study.tsv.gz")
    assert row["beta"] == "0.0282"
    assert row["standard_error"] == "0.0083"
    assert row["p_value"] == "0.0004"
    assert row["effect_allele_frequency"] == "NA"


def test_remap_keeps_finngen_columns_aligned_when_rsids_is_empty(tmp_path):
    header = ["#chrom", "pos", "ref", "alt", "rsids", "nearest_genes", "pval", "mlogp",
              "beta", "sebeta", "af_alt"]
    src = tmp_path / "finngen.tsv.gz"
    _write(src, header, [["1", "14872", "T", "A", "", "WASH7P", "0.538644", "0.2687",
                          "0.000635", "0.00046", "0.0011"]], compress=True)

    result = _run_remap(src)

    assert result.returncode == 0, result.stderr
    (row,) = _read_ssf(tmp_path / "finngen.tsv.gz")
    assert row["p_value"] == "0.538644"
    assert row["beta"] == "0.000635"
    assert row["standard_error"] == "0.00046"
    assert row["rsid"] == "NA"


def test_remap_reads_space_separated_files(tmp_path):
    src = tmp_path / "study.tsv"
    _write(src, PLINK_HEADER, _plink_rows(3), sep=" ")

    result = _run_remap(src)

    assert result.returncode == 0, result.stderr
    rows = _read_ssf(tmp_path / "study.tsv.gz")
    assert [r["p_value"] for r in rows] == ["1e-9"] * 3


def test_remap_names_found_columns_for_an_unknown_layout(tmp_path):
    src = tmp_path / "study.tsv"
    _write(src, ["foo", "bar"], [["1", "2"]])

    result = _run_remap(src)

    assert result.returncode != 0
    assert "unrecognised column layout" in result.stderr
    assert "Columns found: foo, bar" in result.stderr


def test_remap_reports_when_no_variants_are_left(tmp_path):
    src = tmp_path / "study.tsv"
    _write(src, PLINK_HEADER, [["23", "5", "rs1", "A", "G", "0.2", "0.1", "0.01", "1e-9"]])

    result = _run_remap(src)

    assert result.returncode != 0
    assert "no variants left after converting to GWAS-SSF" in result.stderr
    assert "1 data rows, 1 of them on X, Y or MT" in result.stderr


def test_remap_reports_an_empty_file(tmp_path):
    src = tmp_path / "study.tsv"
    src.write_text("")

    result = _run_remap(src)

    assert result.returncode != 0
    assert "the input file is empty" in result.stderr


@pytest.mark.parametrize("n_col", ["N", "n", "OBS_CT", "NMISS"])
def test_remap_carries_plink_sample_size_as_n(tmp_path, n_col):
    src = tmp_path / "study.tsv"
    _write(src, [*PLINK_HEADER, n_col], [[*r, "4321"] for r in _plink_rows(2)])

    result = _run_remap(src)

    assert result.returncode == 0, result.stderr
    rows = _read_ssf(tmp_path / "study.tsv.gz")
    assert [r["N"] for r in rows] == ["4321", "4321"]


def test_remap_carries_neale_sample_size_as_n(tmp_path):
    header = ["variant", "minor_AF", "n_complete_samples", "beta", "se", "pval"]
    src = tmp_path / "neale.tsv"
    _write(src, header, [["1:100:A:G", "0.2", "361194", "0.01", "0.002", "1e-6"]])

    result = _run_remap(src)

    assert result.returncode == 0, result.stderr
    (row,) = _read_ssf(tmp_path / "neale.tsv.gz")
    assert row["N"] == "361194"


def test_remap_adds_no_n_column_without_a_sample_size(tmp_path):
    src = tmp_path / "study.tsv"
    _write(src, PLINK_HEADER, _plink_rows(1))

    result = _run_remap(src)

    assert result.returncode == 0, result.stderr
    (row,) = _read_ssf(tmp_path / "study.tsv.gz")
    assert "N" not in row
