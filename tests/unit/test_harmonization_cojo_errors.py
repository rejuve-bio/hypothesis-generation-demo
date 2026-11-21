import pickle
import subprocess
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from src.tasks import analysis as tasks_module
from src.tasks.analysis import (
    CojoError,
    HarmonizationError,
    describe_cojo_exclusions,
    describe_harmonizer_failure,
    prepare_cojo_input,
    run_cojo_per_chromosome,
)


def _nextflow_failure(process, exit_status, command_error):
    """Nextflow's failure block as it appears in the harmonizer output (twice)."""
    block = (
        f"ERROR ~ Error executing process > '{process}'\n"
        "\n"
        "Caused by:\n"
        f"  Process `{process}` terminated with an error exit status ({exit_status})\n"
        "\n"
        "Command executed:\n"
        "\n"
        "  map_to_build_nf.py -f sumstats.tsv.gz -from_build 38 -to_build 38\n"
        "\n"
        "Command exit status:\n"
        f"  {exit_status}\n"
        "\n"
        "Command output:\n"
        "  (empty)\n"
        "\n"
        "Command error:\n"
        f"{command_error}"
        "\n"
        "Work dir:\n"
        "  /app/data/temp/1/upload_x/work/72/ce8c05\n"
        "\n"
        " -- Check '.nextflow.log' file for details\n"
    )
    progress = (
        "N E X T F L O W  ~  version 23.10.1\n"
        "executor >  local (1)\n"
        "[72/ce8c05] process > NFCORE_GWASCATALOGHARM:GWASCATALOGHARM:major_direction:map_to_build [100%] 1 of 1, failed: 1\n"
    )
    return progress + block + progress + block


DUCKDB_TRACEBACK = (
    "  Traceback (most recent call last):\n"
    '    File "/app/gwas-sumstats-harmoniser/bin/map_to_build_nf.py", line 243, in <module>\n'
    "      main()\n"
    '    File "/app/gwas-sumstats-harmoniser/bin/map_to_build_nf.py", line 77, in merge_ss_vcf\n'
    "      ssdf = con.execute(query).df()\n"
    '  duckdb.duckdb.BinderException: Binder Error: Column "chromosome" in EXCLUDE list not found in FROM clause\n'
)


def test_nextflow_failure_names_the_step_and_the_last_traceback_line():
    stdout = _nextflow_failure(
        "NFCORE_GWASCATALOGHARM:GWASCATALOGHARM:major_direction:map_to_build (21001_raw_chr16_small)",
        1, DUCKDB_TRACEBACK,
    )

    message, step = describe_harmonizer_failure(stdout, "")

    assert step == "map_to_build"
    assert message == (
        "Harmonization failed while mapping variants to the GRCh38 reference build (map_to_build): "
        'duckdb.duckdb.BinderException: Binder Error: Column "chromosome" in EXCLUDE list '
        "not found in FROM clause"
    )


def test_nextflow_failure_from_harmonization_assertion():
    stdout = _nextflow_failure(
        "NFCORE_GWASCATALOGHARM:GWASCATALOGHARM:main_harm:harmonization (study_chr1)",
        1,
        "  Traceback (most recent call last):\n"
        '    File "/app/gwas-sumstats-harmoniser/bin/main_pysam.py", line 120, in <module>\n'
        "      assert 0 <= eaf <= 1\n"
        "  AssertionError\n",
    )

    assert describe_harmonizer_failure(stdout, "") == (
        "Harmonization failed while harmonizing alleles against the reference panel "
        "(harmonization): AssertionError",
        "harmonization",
    )


def test_nextflow_failure_from_strand_count_sort():
    stdout = _nextflow_failure(
        "NFCORE_GWASCATALOGHARM:GWASCATALOGHARM:major_direction:ten_percent_counts (study)",
        2, "  sort: invalid number at field start: invalid count at start of ',n'\n",
    )

    message, step = describe_harmonizer_failure(stdout, "")

    assert step == "ten_percent_counts"
    assert message == (
        "Harmonization failed while checking strand orientation on a sample of variants "
        "(ten_percent_counts): sort: invalid number at field start: invalid count at start of ',n'"
    )


def test_killed_nextflow_step_is_reported_as_out_of_memory():
    stdout = _nextflow_failure(
        "NFCORE_GWASCATALOGHARM:GWASCATALOGHARM:major_direction:map_to_build (study_hg38)",
        137,
        "  .command.sh: line 7: 514997 Killed                  map_to_build_nf.py -f study.tsv.gz -to_build 38\n",
    )

    message, step = describe_harmonizer_failure(stdout, "")

    assert step == "map_to_build"
    assert message.endswith(
        "(map_to_build): the step was stopped by the system (exit status 137), "
        "most likely because it ran out of memory"
    )


@pytest.mark.parametrize("stderr, expected", [
    (
        "ERROR: unrecognised column layout. Columns found: SNP, CHR, BP, A1\n",
        "Harmonization failed: unrecognised column layout. Columns found: SNP, CHR, BP, A1",
    ),
    ("ERROR: the input file is empty.\n", "Harmonization failed: the input file is empty."),
    (
        "Converting...\nError: no variants to harmonize: /data/x.tsv.gz\n",
        "Harmonization failed: no variants to harmonize: /data/x.tsv.gz",
    ),
])
def test_script_error_line_is_used(stderr, expected):
    assert describe_harmonizer_failure("Starting harmonizer\n", stderr) == (expected, None)


def test_indented_error_text_is_not_mistaken_for_a_script_error():
    stdout = "  Binder Error: something\nN E X T F L O W  ~  version 23\nlast real line\n"

    message, _ = describe_harmonizer_failure(stdout, "")

    assert message == "Harmonization failed: Binder Error: something | last real line"


def test_fallback_uses_last_meaningful_lines_and_strips_colours():
    stdout = "\x1b[32mstep one\x1b[0m\nstep two\nstep three\nstep four\nexecutor >  local (3)\n"

    message, step = describe_harmonizer_failure(stdout, "")

    assert (message, step) == ("Harmonization failed: step two | step three | step four", None)


def test_nextflow_error_outside_any_process_is_reported():
    stdout = (
        "N E X T F L O W  ~  version 23.10.1\n"
        "Nextflow 26.04.1 is available - Please consider updating your version to it\n"
        "ERROR ~ Unknown method invocation `tokenize` on Integer type\n"
        "\n"
        " -- Check '.nextflow.log' file for details\n"
    )

    assert describe_harmonizer_failure(stdout, "") == (
        "Harmonization failed: the Nextflow pipeline stopped: "
        "Unknown method invocation `tokenize` on Integer type",
        None,
    )


def test_no_output_still_gives_a_reason():
    assert describe_harmonizer_failure("", "") == (
        "Harmonization failed without any output from the harmonizer.", None,
    )


def test_long_reason_is_truncated():
    message, _ = describe_harmonizer_failure("", "ERROR: " + "x" * 1000)

    assert len(message) < 350
    assert message.endswith("…")


def test_harmonization_error_survives_pickling():
    error = HarmonizationError("Harmonization failed: x", step="qc", exit_code=1)

    restored = pickle.loads(pickle.dumps(error))

    assert restored.as_detail() == {
        "error_type": "harmonization_failed",
        "message": "Harmonization failed: x",
        "step": "qc",
        "exit_code": 1,
    }


def test_harmonizer_non_zero_exit_raises_harmonization_error(monkeypatch, tmp_path):
    script_dir = tmp_path / "scripts"
    script_dir.mkdir()
    (script_dir / "6_harmoniser.sh").write_text("")
    gwas = tmp_path / "upload" / "study.tsv"
    gwas.parent.mkdir()
    gwas.write_text("")
    monkeypatch.setattr(tasks_module, "get_deps", lambda: {"storage": None})
    monkeypatch.setattr(tasks_module.Config, "from_env", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr(tasks_module.subprocess, "run", MagicMock(return_value=SimpleNamespace(
        returncode=1, stdout="Preparing\n", stderr="ERROR: the input file is empty.\n",
    )))

    with pytest.raises(HarmonizationError) as exc:
        tasks_module.harmonize_sumstats_with_nextflow.fn(
            str(gwas), str(tmp_path / "out"), ref_dir="/ref", code_repo="/repo",
            script_dir=str(script_dir),
        )

    assert str(exc.value) == "Harmonization failed: the input file is empty."
    assert exc.value.exit_code == 1


# === COJO ===

def _significant(rows):
    columns = ["effect_allele", "other_allele", "effect_allele_frequency",
               "beta", "standard_error", "p_value", "N"]
    return pd.DataFrame(
        [r[1:] for r in rows], columns=columns,
        index=pd.Index([r[0] for r in rows], name="variant_id"),
    )


ROWS = [
    ("1:100:A:G", "G", "A", 0.2, 0.1, 0.01, 1e-9, 10000),
    ("1:200:C:G", "G", "C", np.nan, -0.06, 0.007, 7e-20, 10000),
    ("1:300:T:C", "C", "T", np.nan, 0.05, np.nan, 1e-10, 10000),
    ("2:400:A:T", "T", "A", 0.4, np.inf, 0.01, 1e-12, 10000),
    ("2:500:G:A", "A", "G", 0.3, 0.2, 0.02, 1e-15, 10000),
]


def test_prepare_cojo_input_drops_rows_gcta_cannot_read():
    cojo_df, exclusions = prepare_cojo_input(_significant(ROWS))

    assert list(cojo_df.columns) == ["SNP", "A1", "A2", "FRQ", "BETA", "SE", "P", "N"]
    assert list(cojo_df["SNP"]) == ["1:100:A:G", "2:500:G:A"]
    assert exclusions == {
        "input_variants": 5,
        "excluded_variants": 3,
        "missing_frequency": 2,
        "missing_beta": 1,
        "missing_standard_error": 1,
        "missing_p_value": 0,
        "missing_sample_size": 0,
    }
    assert describe_cojo_exclusions(exclusions) == (
        "3 of 5 significant variants were excluded from COJO because of missing values "
        "(2 with no allele frequency, 1 with no effect size (beta), 1 with no standard error)."
    )


def test_single_exclusion_reason_reads_naturally():
    exclusions = {"input_variants": 3058, "excluded_variants": 87, "missing_frequency": 87}

    assert describe_cojo_exclusions(exclusions) == (
        "87 of 3,058 significant variants had no allele frequency and were excluded from COJO."
    )
    assert describe_cojo_exclusions({"input_variants": 5, "excluded_variants": 0}) is None


def test_cojo_input_file_has_eight_fields_on_every_line():
    cojo_df, _ = prepare_cojo_input(_significant(ROWS))

    lines = cojo_df.to_csv(sep=" ", index=False).splitlines()

    assert all(len(line.split(" ")) == 8 and "  " not in line for line in lines)


@pytest.fixture
def fake_gcta(monkeypatch, tmp_path):
    """Run run_cojo_per_chromosome with gcta64 replaced by *behaviour*(chrom, prefix)."""
    plink_dir = tmp_path / "plink"
    (plink_dir / "EUR").mkdir(parents=True)
    for chrom in range(1, 23):
        (plink_dir / "EUR" / f"EUR.chr{chrom}.bed").write_text("")
    work = tmp_path / "work"
    work.mkdir()
    config = MagicMock(**{"get_plink_file_pattern.side_effect": lambda population, chrom: f"EUR.chr{chrom}"})
    monkeypatch.setattr(tasks_module.Config, "from_env", MagicMock(return_value=config))
    monkeypatch.setattr(tasks_module, "get_shared_temp_dir", lambda prefix: str(work))
    record = MagicMock()
    monkeypatch.setattr(tasks_module, "record_analysis_event", record)
    calls = []

    def run(behaviour, significant_df):
        def fake_run(cmd, **_kwargs):
            chrom = int(cmd[cmd.index("--chr") + 1])
            prefix = cmd[cmd.index("--out") + 1]
            calls.append(chrom)
            return behaviour(chrom, prefix)

        monkeypatch.setattr(tasks_module.subprocess, "run", fake_run)
        return run_cojo_per_chromosome.fn(
            "user-1", "project-1", significant_df, str(plink_dir), str(tmp_path / "out"),
        )

    run.calls = calls
    run.events = record
    return run


GCTA_BAD_LINE = (
    "Reading GWAS summary-level statistics from [cojo_input.txt] ...\n"
    "Error: in line:\n"
    '"1:55021673:C:G G C  -0.0639 0.0067 7.003e-20 10000"\n'
    "An error occurs, please check the options or data\n"
)


def test_all_chromosomes_failing_reports_gcta_error(fake_gcta):
    def gcta(chrom, prefix):
        with open(f"{prefix}.log", "w") as log:
            log.write(GCTA_BAD_LINE)
        return SimpleNamespace(returncode=1, stdout=GCTA_BAD_LINE, stderr="")

    with pytest.raises(CojoError) as exc:
        fake_gcta(gcta, _significant(ROWS))

    assert sorted(fake_gcta.calls) == [1, 2]
    assert str(exc.value) == (
        "COJO failed on all 2 chromosomes with significant variants. GCTA reported on "
        'chromosome 1: Error: in line: "1:55021673:C:G G C  -0.0639 0.0067 7.003e-20 10000" '
        "3 of 5 significant variants were excluded from COJO because of missing values "
        "(2 with no allele frequency, 1 with no effect size (beta), 1 with no standard error)."
    )
    detail = exc.value.as_detail()
    assert detail["error_type"] == "cojo_failed"
    assert detail["failed_chromosomes"] == [1, 2]
    assert detail["excluded_variants"]["missing_frequency"] == 2
    assert pickle.loads(pickle.dumps(exc.value)).as_detail() == detail


def test_gcta_error_is_read_from_its_log_when_stdout_is_empty(fake_gcta):
    def gcta(chrom, prefix):
        with open(f"{prefix}.log", "w") as log:
            log.write("Error: none of the SNPs in the GWAS summary data can be found in the genotype data.\n")
        return SimpleNamespace(returncode=1, stdout="", stderr="")

    with pytest.raises(CojoError) as exc:
        fake_gcta(gcta, _significant(ROWS[:1]))

    assert exc.value.gcta_error == (
        "Error: none of the SNPs in the GWAS summary data can be found in the genotype data."
    )


def test_no_signals_without_errors_is_not_called_a_failure_of_gcta(fake_gcta):
    with pytest.raises(CojoError) as exc:
        fake_gcta(lambda chrom, prefix: SimpleNamespace(returncode=0, stdout="", stderr=""),
                  _significant(ROWS[:1]))

    assert str(exc.value) == "COJO found no independent signals on the 1 chromosome with significant variants."
    assert exc.value.gcta_error is None


def test_no_usable_rows_raises_before_running_gcta(fake_gcta):
    with pytest.raises(CojoError) as exc:
        fake_gcta(MagicMock(), _significant(ROWS[1:3]))

    assert fake_gcta.calls == []
    assert str(exc.value) == (
        "No significant variants could be used for COJO. "
        "2 of 2 significant variants were excluded from COJO because of missing values "
        "(2 with no allele frequency, 1 with no standard error)."
    )


def test_successful_cojo_returns_exclusions_and_partial_failures(fake_gcta):
    def gcta(chrom, prefix):
        if chrom == 2:
            return SimpleNamespace(returncode=1, stdout="Error: bad thing\n", stderr="")
        pd.DataFrame({"Chr": [1], "SNP": ["1:100:A:G"], "bp": [100], "p": [1e-9]}).to_csv(
            f"{prefix}.jma.cojo", sep="\t", index=False,
        )
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    results, path, report = fake_gcta(gcta, _significant(ROWS))

    assert list(results.index) == ["1:100:A:G"]
    assert path.endswith("combined_cojo_results.txt")
    assert report["excluded_variants"]["excluded_variants"] == 3
    assert report["failed_chromosomes"] == {"2": "Error: bad thing"}
    events = [(c.args[3], c.kwargs.get("level", "info"), c.kwargs.get("details")) for c in fake_gcta.events.call_args_list]
    assert events == [
        (describe_cojo_exclusions(report["excluded_variants"]), "warning", report["excluded_variants"]),
        ("COJO failed on chr 2; its signals are not included", "warning",
         {"chromosome": 2, "gcta_error": "Error: bad thing"}),
        ("1 independent signals on chr 1", "info", {"independent_signals": 1, "chromosomes": [1]}),
    ]
    assert report["message"] == (
        "3 of 5 significant variants were excluded from COJO because of missing values "
        "(2 with no allele frequency, 1 with no effect size (beta), 1 with no standard error). "
        "COJO failed on chromosome 2, so no signals from it were fine-mapped "
        "(chromosome 2: Error: bad thing)."
    )


def test_gcta_timeout_is_reported(fake_gcta):
    def gcta(chrom, prefix):
        raise subprocess.TimeoutExpired("gcta64", 1800)

    with pytest.raises(CojoError, match="GCTA did not finish within 30 minutes on chromosome 1"):
        fake_gcta(gcta, _significant(ROWS[:1]))
