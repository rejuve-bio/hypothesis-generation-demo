import marimo

__generated_with = "0.7.0"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    return (mo,)


@app.cell
def __(mo):
    mo.md(r"""
    # deltaSVM Pipeline
    End-to-end pipeline for scoring SNP allelic effects on TF binding using deltaSVM.
    """)
    return


@app.cell
def __(mo):
    mo.md(r"""## 0. Environment Setup""")
    return


@app.cell
def __():
    import subprocess


    def run_bash(script, label="step"):
        """Run a bash script, echo its output, and surface a non-zero exit."""
        proc = subprocess.run(["bash", "-c", script], capture_output=True, text=True)
        if proc.stdout:
            print(proc.stdout, end="")
        if proc.stderr:
            print(proc.stderr, end="")
        if proc.returncode != 0:
            print(f"\n!! {label} FAILED with exit code {proc.returncode}")
        return proc
    return run_bash, subprocess


@app.cell
def __(run_bash):
    result = run_bash("""
set -euo pipefail
BASE_DIR="/mnt/hdd_1/rediet/deltaSVM"
mkdir -p $BASE_DIR/scripts

if [ ! -f "$BASE_DIR/scripts/gkmpredict" ]; then
    rm -rf /tmp/lsgkm
    git clone https://github.com/Dongwon-Lee/lsgkm.git /tmp/lsgkm
    cd /tmp/lsgkm/src && make
    cp /tmp/lsgkm/bin/gkmpredict $BASE_DIR/scripts/
    cp /tmp/lsgkm/bin/gkmtrain $BASE_DIR/scripts/
fi

if [ ! -f "$BASE_DIR/scripts/deltasvm_subset_multi" ]; then
    g++ -O3 -o $BASE_DIR/scripts/deltasvm_subset_multi $BASE_DIR/scripts/deltasvm_subset_multi.cpp
fi

mkdir -p $BASE_DIR/resources/hs38
mkdir -p $BASE_DIR/gkmsvm_models
mkdir -p $BASE_DIR/snp_batches

echo "Setup complete. Make sure the following are in place:"
echo "  - resources/hs38/hs38.fa"
echo "  - resources/models.weights.txt"
echo "  - resources/thresholds.pbs.tsv"
echo "  - resources/thresholds.obs.tsv"
echo "  - gkmsvm_models/*.model.txt"
echo "  - snp_batches/chrN.tsv"
    """, label="section 0 environment setup")
    return result,


@app.cell
def __(mo):
    mo.md(r"""### Shared Run Steps""")
    return


@app.cell
def __():
    # Bash fragments shared by sections 6, 7 and 8, so the gkmpredict sweep and
    # the summary/parquet steps are defined in exactly one place. Each fragment
    # assumes the CWD is a run directory (runs/$CHR) and that $CHR is set.
    GKM_PREDICT_MISSING = r"""
awk '{print $1" "$3}' resources/thresholds.obs.tsv | while read -r tf model; do
    if [ ! -f "tmp/${tf}.merge.gkm.tsv" ]; then echo "$tf $model"; fi
done > missing_tfs.txt
echo "[$CHR] $(wc -l < missing_tfs.txt) TFs to process"

parallel -j4 --colsep ' ' '
    tf={1}; model={2}
    scripts/gkmpredict -T 16 data/selex_allelic_oligos.ref.fa gkmsvm_models/${model}.model.txt tmp/${tf}.ref.gkm.tsv
    sort -k1,1 -o tmp/${tf}.ref.gkm.tsv tmp/${tf}.ref.gkm.tsv
    scripts/gkmpredict -T 16 data/selex_allelic_oligos.alt.fa gkmsvm_models/${model}.model.txt tmp/${tf}.alt.gkm.tsv
    sort -k1,1 -o tmp/${tf}.alt.gkm.tsv tmp/${tf}.alt.gkm.tsv
    paste tmp/${tf}.ref.gkm.tsv tmp/${tf}.alt.gkm.tsv | cut -f1,2,4 > tmp/${tf}.merge.gkm.tsv
    echo "Done: $tf"
' < missing_tfs.txt
"""

    SUMMARIZE = r"""
python scripts/obs_pred.py -s resources/thresholds.obs.tsv -o out/obs.pred.tsv
sort -k1,1 -k2,2 -o out/obs.pred.tsv out/obs.pred.tsv
paste out/obs.pred.tsv out/pbs.pred.tsv | cut -f1-5,7,9 | sort -k7,7 -k5,5r | sed '1i snp\ttf\tallele1_bind\tallele2_bind\tseq_binding\tdeltaSVM_score\tpreferred_allele' > out/summary.pred.tsv
"""

    WRITE_PARQUET = r"""
python3 - "$CHR" <<'PYEOF'
import sys

import pandas as pd

chrom = sys.argv[1]
df = pd.read_csv("out/summary.pred.tsv", sep="\t", low_memory=False)
df = df.rename(columns={"snp": "variant", "tf": "TfName",
                        "preferred_allele": "Effect", "deltaSVM_score": "Score"})
df["rsId"] = "."
df["TfId"] = "."
df = df[df["Effect"].isin(["Gain", "Loss"])]
df = df[["variant", "rsId", "TfId", "TfName", "Effect", "Score"]]
df.to_parquet(f"out/{chrom}_deltasvm.parquet", index=False)
print(f"{chrom} parquet done! rows: {len(df)}")
PYEOF
"""

    RESTART_CHR = (
        'cd "$BASE_DIR/runs/$CHR"\n'
        + GKM_PREDICT_MISSING
        + SUMMARIZE
        + WRITE_PARQUET
        + 'echo "=== $CHR FULLY DONE ==="\n'
    )
    return GKM_PREDICT_MISSING, RESTART_CHR, SUMMARIZE, WRITE_PARQUET


@app.cell
def __(mo):
    mo.md(r"""## 1. Prepare SNPs from dbSNP VCF""")
    return


@app.cell
def __():
    import gzip

    input_vcf = "/mnt/hdd_1/abdu_md/biocypher_data_bizon/dbsnp/00-common_all.vcf.gz"
    output_snp = "input_snp_common.tsv"
    output_rsid_map = "rsid_map.tsv"

    AUTOSOMES = {str(i) for i in range(1, 23)}

    count = 0
    written = 0

    with gzip.open(input_vcf, "rt") as _f, \
         open(output_snp, "w") as out_snp, \
         open(output_rsid_map, "w") as out_map:

        out_map.write("variant\trsId\n")

        for line in _f:
            if line.startswith("#"):
                continue

            cols = line.strip().split("\t")
            chrom, pos, rsid, ref, alt, info = cols[0], cols[1], cols[2], cols[3], cols[4], cols[7]

            count += 1
            if "VC=SNV" not in info:
                continue
            if chrom not in AUTOSOMES:
                continue
            if len(ref) != 1 or len(alt) != 1:
                continue

            variant = f"chr{chrom}_{pos}_{ref}_{alt}"
            out_snp.write(variant + "\n")
            out_map.write(f"{variant}\t{rsid}\n")
            written += 1

            if written % 1000000 == 0:
                print(f"Processed {count:,} lines, written {written:,} SNPs...")

    print(f"Done! Total lines: {count:,}, SNPs written: {written:,}")
    return count, written


@app.cell
def __(mo):
    mo.md(r"""## 2. Filter SNPs Using OpenTargets Variants""")
    return


@app.cell
def __():
    import os
    import pandas as pd
    import glob

    SNP_DIR = '/mnt/hdd_1/rediet/deltaSVM/snp_batches'
    CHROMS = [f'chr{i}' for i in range(1, 23)]

    print("Loading OpenTargets variants...")
    files = glob.glob('/mnt/hdd_1/rediet/opentargets_variants/variant/part-*.parquet')
    ot_variants = set()
    for _f in files:
        df = pd.read_parquet(_f, columns=['variantId'])
        df['variantId'] = 'chr' + df['variantId']
        ot_variants.update(df['variantId'].tolist())
        print(f"Loaded {_f.split('/')[-1]}: {len(ot_variants):,} variants so far")

    print(f"\nTotal OT variants: {len(ot_variants):,}")

    def filter_chrom_snps(chrom):
        """Filter one chromosome's SNP batch against the OpenTargets variant set."""
        snp_file = f'{SNP_DIR}/{chrom}.tsv'
        out_file = f'{SNP_DIR}/{chrom}_ot.tsv'

        if not os.path.exists(snp_file):
            print(f"{chrom}: SKIPPED, missing {snp_file}")
            return 0, 0

        with open(snp_file) as _f:
            snps = [line.strip() for line in _f if line.strip()]

        filtered = [s for s in snps if s in ot_variants]

        with open(out_file, 'w') as _f:
            for snp in filtered:
                _f.write(snp + '\n')

        kept = 100 * len(filtered) / len(snps) if snps else 0.0
        print(f"{chrom}: {len(snps):,} -> {len(filtered):,} SNPs after filtering ({kept:.1f}% kept)")
        return len(snps), len(filtered)

    total_in = 0
    total_out = 0
    for CHR in CHROMS:
        n_in, n_out = filter_chrom_snps(CHR)
        total_in += n_in
        total_out += n_out

    missing = [c for c in CHROMS if not os.path.exists(f'{SNP_DIR}/{c}_ot.tsv')]
    print(f"\nAll chromosomes filtered: {total_in:,} -> {total_out:,} SNPs")
    if missing:
        print(f"WARNING: no OT-filtered SNP batch for: {', '.join(missing)}")
    else:
        print("Every chromosome has an OT-filtered SNP batch.")
    return CHROMS, SNP_DIR, filter_chrom_snps, glob, os, ot_variants, pd


@app.cell
def __(mo):
    mo.md(r"""## 3. Set Up Per-Chromosome Working Directory""")
    return


@app.cell
def __(run_bash):
    CHR_SETUP = "chr1"
    result_setup = run_bash(f"""
set -euo pipefail
BASE_DIR="/mnt/hdd_1/rediet/deltaSVM"
CHR={CHR_SETUP}

SNP_FILE="$BASE_DIR/snp_batches/${{CHR}}_ot.tsv"

if [ ! -s "$SNP_FILE" ]; then
    echo "ERROR: $SNP_FILE missing or empty - run section 2 (OpenTargets filtering) first"
    exit 1
fi

echo "Setting up $CHR with $(wc -l < $SNP_FILE) OT-filtered SNPs..."
mkdir -p $BASE_DIR/runs/$CHR/{{data,tmp,out,log}}
cp -r $BASE_DIR/scripts $BASE_DIR/runs/$CHR/
cp -r $BASE_DIR/resources $BASE_DIR/runs/$CHR/
cp -r $BASE_DIR/gkmsvm_models $BASE_DIR/runs/$CHR/
cp "$SNP_FILE" $BASE_DIR/runs/$CHR/input_snp.tsv

cd $BASE_DIR/runs/$CHR
python scripts/generate_allelic_seqs.py -f resources/hs38/hs38.fa -s input_snp.tsv -o data/selex_allelic_oligos
scripts/deltasvm_subset_multi data/selex_allelic_oligos.ref.fa data/selex_allelic_oligos.alt.fa resources/models.weights.txt out/pbs.pred.tsv resources/thresholds.pbs.tsv
echo "$CHR setup done!"
    """, label="section 3 chromosome setup")
    return CHR_SETUP, result_setup


@app.cell
def __(mo):
    mo.md(r"""## 4. Run All Chromosomes in Parallel""")
    return


@app.cell
def __(run_bash):
    result_parallel = run_bash("""
set -euo pipefail
CHROMS="chr1 chr2 chr3 chr4 chr5 chr6 chr7 chr8 chr9 chr10 chr11 chr12 chr13 chr14 chr15 chr16 chr17 chr18 chr19 chr20 chr21 chr22"
BASE_DIR="/mnt/hdd_1/rediet/deltaSVM"

for CHROM in $CHROMS; do
    SNP_FILE="$BASE_DIR/snp_batches/${CHROM}_ot.tsv"
    if [ ! -s "$SNP_FILE" ]; then
        echo "Skipping $CHROM: $SNP_FILE missing or empty"
        continue
    fi
    WORK_DIR="$BASE_DIR/runs/$CHROM"
    mkdir -p "$WORK_DIR"
    cp -r "$BASE_DIR/scripts" "$WORK_DIR/"
    cp -r "$BASE_DIR/resources" "$WORK_DIR/"
    cp -r "$BASE_DIR/gkmsvm_models" "$WORK_DIR/"
    cp "$BASE_DIR/run.sh" "$WORK_DIR/"
    cp "$SNP_FILE" "$WORK_DIR/input_snp.tsv"
    echo "Starting $CHROM ($(wc -l < $SNP_FILE) OT-filtered SNPs)..."
    cd "$WORK_DIR"
    bash run.sh > "$WORK_DIR/run.log" 2>&1 &
    cd "$BASE_DIR"
done

echo "All chromosomes started in parallel!"
wait
echo "All done!"
    """, label="section 4 parallel run")
    return result_parallel,


@app.cell
def __(mo):
    mo.md(r"""## 5. Single Chromosome Full Run""")
    return


@app.cell
def __(run_bash):
    CHR_RUN = "chr1"
    # The bash body stays a plain string: it contains ${tf}/${model}, which an
    # f-string would try to interpolate. Only the $CHR line is built in Python.
    result_run = run_bash(
        "set -euo pipefail\n"
        'BASE_DIR="/mnt/hdd_1/rediet/deltaSVM"\n'
        f"CHR={CHR_RUN}\n"
        + """RUN_DIR="$BASE_DIR/runs/$CHR"

# This section wipes data/ tmp/ out/ log/, so refuse to touch anything that
# is not a prepared run directory rather than trusting the current CWD.
if [ ! -f "$RUN_DIR/input_snp.tsv" ]; then
    echo "ERROR: $RUN_DIR is not a prepared run directory (no input_snp.tsv) - run section 3 first"
    exit 1
fi
cd "$RUN_DIR"
echo "Running $CHR in $RUN_DIR"

rm -rf data tmp out log
mkdir data tmp out log
python scripts/generate_allelic_seqs.py -f resources/hs38/hs38.fa -s input_snp.tsv -o data/selex_allelic_oligos 2>log/selex_allelic_oligos.log
scripts/deltasvm_subset_multi data/selex_allelic_oligos.ref.fa data/selex_allelic_oligos.alt.fa resources/models.weights.txt out/pbs.pred.tsv resources/thresholds.pbs.tsv 2>log/deltasvm.log
sort -k1,1 -k3,3 -o out/pbs.pred.tsv out/pbs.pred.tsv
cat resources/thresholds.obs.tsv | while read tf score model; do
  scripts/gkmpredict -T 1 data/selex_allelic_oligos.ref.fa gkmsvm_models/${model}.model.txt tmp/${tf}.ref.gkm.tsv &>log/${model}.ref.gkm.log
  sort -k1,1 -o tmp/${tf}.ref.gkm.tsv tmp/${tf}.ref.gkm.tsv
  scripts/gkmpredict -T 1 data/selex_allelic_oligos.alt.fa gkmsvm_models/${model}.model.txt tmp/${tf}.alt.gkm.tsv &>log/${model}.alt.gkm.log
  sort -k1,1 -o tmp/${tf}.alt.gkm.tsv tmp/${tf}.alt.gkm.tsv
  paste tmp/${tf}.ref.gkm.tsv tmp/${tf}.alt.gkm.tsv | cut -f1,2,4 >tmp/${tf}.merge.gkm.tsv
done
python scripts/obs_pred.py -s resources/thresholds.obs.tsv -o out/obs.pred.tsv
sort -k1,1 -k2,2 -o out/obs.pred.tsv out/obs.pred.tsv
paste out/obs.pred.tsv out/pbs.pred.tsv | cut -f1-5,7,9 | sort -k7,7 -k5,5r | sed '1i snp\ttf\tallele1_bind\tallele2_bind\tseq_binding\tdeltaSVM_score\tpreferred_allele' >out/summary.pred.tsv
""", label="section 5 single chromosome run")
    return CHR_RUN, result_run


@app.cell
def __(mo):
    mo.md(r"""## 6. Run gkmpredict Across Multiple Chromosomes""")
    return


@app.cell
def __(GKM_PREDICT_MISSING, run_bash):
    CHROMS_GKM = "chr15 chr16 chr17 chr18 chr19 chr20"
    result_gkm = run_bash(
        'set -euo pipefail\n'
        'BASE_DIR="/mnt/hdd_1/rediet/deltaSVM"\n'
        'run_gkm_for_chr() {\n'
        '    CHR=$1\n'
        '    cd "$BASE_DIR/runs/$CHR"\n'
        '    echo "[$CHR] Starting gkmpredict..."\n'
        '    while [ ! -s out/pbs.pred.tsv ]; do sleep 60; done\n'
        '    sort -k1,1 -k3,3 -o out/pbs.pred.tsv out/pbs.pred.tsv\n'
        + GKM_PREDICT_MISSING
        + '    echo "[$CHR] gkmpredict done!"\n'
        '}\n'
        'export -f run_gkm_for_chr\n'
        'export BASE_DIR\n'
        f'for CHR in {CHROMS_GKM}; do\n'
        '    run_gkm_for_chr $CHR &\n'
        'done\n'
        'wait\n'
        'echo "All chromosomes done!"\n',
        label="section 6 gkmpredict sweep",
    )
    return CHROMS_GKM, result_gkm


@app.cell
def __(mo):
    mo.md(r"""## 7. Restart and Complete a Chromosome Run""")
    return


@app.cell
def __(RESTART_CHR, run_bash):
    CHR_RESTART = "chr1"
    result_restart = run_bash(
        'set -euo pipefail\n'
        'BASE_DIR="/mnt/hdd_1/rediet/deltaSVM"\n'
        f'CHR={CHR_RESTART}\n'
        + RESTART_CHR,
        label="section 7 restart",
    )
    return CHR_RESTART, result_restart


@app.cell
def __(mo):
    mo.md(r"""## 8. Orchestrate Full Pipeline Across Chromosomes""")
    return


@app.cell
def __(RESTART_CHR, run_bash):
    CHROMS_RUN = "chr1 chr2 chr3"
    result_pipeline = run_bash(
        'set -euo pipefail\n'
        'BASE_DIR="/mnt/hdd_1/rediet/deltaSVM"\n'
        f'for CHR in {CHROMS_RUN}; do\n'
        '    echo "=== Starting $CHR ==="\n'
        '    ( ' + RESTART_CHR + ' )\n'
        'done\n'
        'echo "ALL DONE!"\n',
        label="section 8 orchestration",
    )
    return CHROMS_RUN, result_pipeline


@app.cell
def __(mo):
    mo.md(r"""## 9. Check Pipeline Progress""")
    return


@app.cell
def __(run_bash):
    result_progress = run_bash("""
set -u
for CHR in chr1 chr2 chr3 chr4 chr5 chr6 chr7 chr8 chr9 chr10 chr11 chr12 chr13 chr14 chr15 chr16 chr17 chr18 chr19 chr20 chr21 chr22; do
    COUNT=$(ls /mnt/hdd_1/rediet/deltaSVM/runs/$CHR/tmp/*.merge.gkm.tsv 2>/dev/null | wc -l)
    if [ -f /mnt/hdd_1/rediet/deltaSVM/runs/$CHR/out/${CHR}_deltasvm.parquet ]; then PARQUET="done"; else PARQUET="not done"; fi
    OT=$(wc -l < /mnt/hdd_1/rediet/deltaSVM/snp_batches/${CHR}_ot.tsv 2>/dev/null || echo 0)
    USED=$(wc -l < /mnt/hdd_1/rediet/deltaSVM/runs/$CHR/input_snp.tsv 2>/dev/null || echo 0)
    if [ "$USED" -ne "$OT" ]; then FILTER="NOT OT-filtered ($USED vs $OT)"; else FILTER="OT-filtered ($USED SNPs)"; fi
    echo "$CHR: $COUNT/94 TFs | $PARQUET | $FILTER"
done
    """, label="section 9 progress check")
    return result_progress,


@app.cell
def __(mo):
    mo.md(r"""## 10. Convert Results to Parquet""")
    return


@app.cell
def __(pd):
    df_result = pd.read_csv("out/summary.pred.tsv", sep="\t")

    df_result = df_result.rename(columns={
        "snp": "variant",
        "tf": "TfName",
        "preferred_allele": "Effect",
        "deltaSVM_score": "Score"
    })

    df_result["rsId"] = "."
    df_result["TfId"] = "."

    df_result = df_result[df_result["Effect"].isin(["Gain", "Loss"])]
    df_result = df_result[["variant", "rsId", "TfId", "TfName", "Effect", "Score"]]

    print(df_result)
    print(f"\nTotal rows after filtering: {len(df_result)}")

    df_result.to_parquet("out/deltasvm_results.parquet", index=False)
    print("Saved to out/deltasvm_results.parquet")
    return df_result,


if __name__ == "__main__":
    app.run()
