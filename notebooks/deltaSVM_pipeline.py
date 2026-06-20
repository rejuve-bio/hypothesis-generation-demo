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
    result = subprocess.run(["bash", "-c", """
BASE_DIR="/mnt/hdd_1/rediet/deltaSVM"
mkdir -p $BASE_DIR/scripts

if [ ! -f "$BASE_DIR/scripts/gkmpredict" ]; then
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
    """], capture_output=True, text=True)
    print(result.stdout)
    print(result.stderr)
    return result, subprocess


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
            if chrom not in [str(i) for i in range(1, 23)] + ["X", "Y"]:
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
    import pandas as pd
    import glob

    print("Loading OpenTargets variants...")
    files = glob.glob('/mnt/hdd_1/rediet/opentargets_variants/variant/part-*.parquet')
    ot_variants = set()
    for _f in files:
        df = pd.read_parquet(_f, columns=['variantId'])
        df['variantId'] = 'chr' + df['variantId']
        ot_variants.update(df['variantId'].tolist())
        print(f"Loaded {_f.split('/')[-1]}: {len(ot_variants):,} variants so far")

    print(f"\nTotal OT variants: {len(ot_variants):,}")

    CHROMS = [f'chr{i}' for i in range(1, 23)]
    for CHR in CHROMS:
        snp_file = f'/mnt/hdd_1/rediet/deltaSVM/snp_batches/{CHR}.tsv'
        out_file = f'/mnt/hdd_1/rediet/deltaSVM/snp_batches/{CHR}_ot.tsv'

        with open(snp_file) as _f:
            snps = [line.strip() for line in _f]

        filtered = [s for s in snps if s in ot_variants]

        with open(out_file, 'w') as _f:
            _f.write('\n'.join(filtered))

        print(f"{CHR}: {len(snps):,} → {len(filtered):,} SNPs after filtering")
    return CHROMS, glob, ot_variants, pd


@app.cell
def __(mo):
    mo.md(r"""## 3. Set Up Per-Chromosome Working Directory""")
    return


@app.cell
def __(subprocess):
    CHR_SETUP = "chr1"
    result_setup = subprocess.run(["bash", "-c", f"""
BASE_DIR="/mnt/hdd_1/rediet/deltaSVM"
CHR={CHR_SETUP}

echo "Setting up $CHR..."
mkdir -p $BASE_DIR/runs/$CHR/{{data,tmp,out,log}}
cp -r $BASE_DIR/scripts $BASE_DIR/runs/$CHR/
cp -r $BASE_DIR/resources $BASE_DIR/runs/$CHR/
cp -r $BASE_DIR/gkmsvm_models $BASE_DIR/runs/$CHR/
cp $BASE_DIR/snp_batches/$CHR.tsv $BASE_DIR/runs/$CHR/input_snp.tsv

cd $BASE_DIR/runs/$CHR
python scripts/generate_allelic_seqs.py -f resources/hs38/hs38.fa -s input_snp.tsv -o data/selex_allelic_oligos
scripts/deltasvm_subset_multi data/selex_allelic_oligos.ref.fa data/selex_allelic_oligos.alt.fa resources/models.weights.txt out/pbs.pred.tsv resources/thresholds.pbs.tsv
echo "$CHR setup done!"
    """], capture_output=True, text=True)
    print(result_setup.stdout)
    print(result_setup.stderr)
    return CHR_SETUP, result_setup


@app.cell
def __(mo):
    mo.md(r"""## 4. Run All Chromosomes in Parallel""")
    return


@app.cell
def __(subprocess):
    result_parallel = subprocess.run(["bash", "-c", """
CHROMS="chr1 chr2 chr3 chr4 chr5 chr6 chr7 chr8 chr9 chr10 chr11 chr12 chr13 chr14 chr15 chr16 chr17 chr18 chr19 chr20 chr21 chr22"
BASE_DIR="/mnt/hdd_1/rediet/deltaSVM"

for CHROM in $CHROMS; do
    WORK_DIR="$BASE_DIR/runs/$CHROM"
    mkdir -p "$WORK_DIR"
    cp -r "$BASE_DIR/scripts" "$WORK_DIR/"
    cp -r "$BASE_DIR/resources" "$WORK_DIR/"
    cp -r "$BASE_DIR/gkmsvm_models" "$WORK_DIR/"
    cp "$BASE_DIR/run.sh" "$WORK_DIR/"
    cp "$BASE_DIR/snp_batches/$CHROM.tsv" "$WORK_DIR/input_snp.tsv"
    echo "Starting $CHROM..."
    cd "$WORK_DIR"
    bash run.sh > "$WORK_DIR/run.log" 2>&1 &
    cd "$BASE_DIR"
done

echo "All chromosomes started in parallel!"
wait
echo "All done!"
    """], capture_output=True, text=True)
    print(result_parallel.stdout)
    print(result_parallel.stderr)
    return result_parallel,


@app.cell
def __(mo):
    mo.md(r"""## 5. Single Chromosome Full Run""")
    return


@app.cell
def __(subprocess):
    result_run = subprocess.run(["bash", "-c", """
set -e
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
    """], capture_output=True, text=True)
    print(result_run.stdout)
    print(result_run.stderr)
    return result_run,


@app.cell
def __(mo):
    mo.md(r"""## 6. Run gkmpredict Across Multiple Chromosomes""")
    return


@app.cell
def __(subprocess):
    result_gkm = subprocess.run(["bash", "-c", """
BASE_DIR="/mnt/hdd_1/rediet/deltaSVM"

run_gkm_for_chr() {
    CHR=$1
    WORK_DIR="$BASE_DIR/runs/$CHR"
    cd "$WORK_DIR"
    echo "[$CHR] Starting gkmpredict..."
    while [ ! -s out/pbs.pred.tsv ]; do sleep 60; done
    sort -k1,1 -k3,3 -o out/pbs.pred.tsv out/pbs.pred.tsv
    cat resources/thresholds.obs.tsv | awk '{print $1" "$3}' | while read tf model; do
        if [ ! -f "tmp/${tf}.merge.gkm.tsv" ]; then echo "$tf $model"; fi
    done > missing_tfs.txt
    echo "[$CHR] $(wc -l < missing_tfs.txt) TFs to process"
    cat missing_tfs.txt | parallel -j4 --colsep ' ' '
        tf={1}; model={2}
        scripts/gkmpredict -T 16 data/selex_allelic_oligos.ref.fa gkmsvm_models/${model}.model.txt tmp/${tf}.ref.gkm.tsv
        sort -k1,1 -o tmp/${tf}.ref.gkm.tsv tmp/${tf}.ref.gkm.tsv
        scripts/gkmpredict -T 16 data/selex_allelic_oligos.alt.fa gkmsvm_models/${model}.model.txt tmp/${tf}.alt.gkm.tsv
        sort -k1,1 -o tmp/${tf}.alt.gkm.tsv tmp/${tf}.alt.gkm.tsv
        paste tmp/${tf}.ref.gkm.tsv tmp/${tf}.alt.gkm.tsv | cut -f1,2,4 > tmp/${tf}.merge.gkm.tsv
        echo "Done: $tf"
    '
    echo "[$CHR] gkmpredict done!"
}

export -f run_gkm_for_chr
export BASE_DIR

for CHR in chr15 chr16 chr17 chr18 chr19 chr20; do
    run_gkm_for_chr $CHR &
done
wait
echo "All chromosomes done!"
    """], capture_output=True, text=True)
    print(result_gkm.stdout)
    print(result_gkm.stderr)
    return result_gkm,


@app.cell
def __(mo):
    mo.md(r"""## 7. Restart and Complete a Chromosome Run""")
    return


@app.cell
def __(subprocess):
    CHR_RESTART = "chr1"
    result_restart = subprocess.run(["bash", "-c", f"""
CHR={CHR_RESTART}
cd /mnt/hdd_1/rediet/deltaSVM/runs/$CHR

cat resources/thresholds.obs.tsv | awk '{{print $1" "$3}}' | while read tf model; do
    if [ ! -f "tmp/${{tf}}.merge.gkm.tsv" ]; then echo "$tf $model"; fi
done > missing_tfs.txt
echo "$CHR missing: $(wc -l < missing_tfs.txt) TFs"

cat missing_tfs.txt | parallel -j4 --colsep ' ' '
    tf={{1}}; model={{2}}
    scripts/gkmpredict -T 16 data/selex_allelic_oligos.ref.fa gkmsvm_models/${{model}}.model.txt tmp/${{tf}}.ref.gkm.tsv
    sort -k1,1 -o tmp/${{tf}}.ref.gkm.tsv tmp/${{tf}}.ref.gkm.tsv
    scripts/gkmpredict -T 16 data/selex_allelic_oligos.alt.fa gkmsvm_models/${{model}}.model.txt tmp/${{tf}}.alt.gkm.tsv
    sort -k1,1 -o tmp/${{tf}}.alt.gkm.tsv tmp/${{tf}}.alt.gkm.tsv
    paste tmp/${{tf}}.ref.gkm.tsv tmp/${{tf}}.alt.gkm.tsv | cut -f1,2,4 > tmp/${{tf}}.merge.gkm.tsv
    echo "Done: $tf"
'

python scripts/obs_pred.py -s resources/thresholds.obs.tsv -o out/obs.pred.tsv
sort -k1,1 -k2,2 -o out/obs.pred.tsv out/obs.pred.tsv
paste out/obs.pred.tsv out/pbs.pred.tsv | cut -f1-5,7,9 | sort -k7,7 -k5,5r | sed '1i snp\ttf\tallele1_bind\tallele2_bind\tseq_binding\tdeltaSVM_score\tpreferred_allele' > out/summary.pred.tsv

python3 -c "
import pandas as pd
df = pd.read_csv('out/summary.pred.tsv', sep='\t', low_memory=False)
df = df.rename(columns={{'snp':'variant','tf':'TfName','preferred_allele':'Effect','deltaSVM_score':'Score'}})
df['rsId'] = '.'
df['TfId'] = '.'
df = df[df['Effect'].isin(['Gain','Loss'])]
df = df[['variant','rsId','TfId','TfName','Effect','Score']]
df.to_parquet('out/{CHR_RESTART}_deltasvm.parquet', index=False)
print('{CHR_RESTART} parquet done! rows:', len(df))
"
echo "=== $CHR FULLY DONE ==="
    """], capture_output=True, text=True)
    print(result_restart.stdout)
    print(result_restart.stderr)
    return CHR_RESTART, result_restart


@app.cell
def __(mo):
    mo.md(r"""## 8. Orchestrate Full Pipeline Across Chromosomes""")
    return


@app.cell
def __(subprocess):
    CHROMS_RUN = "chr1 chr2 chr3"
    result_pipeline = subprocess.run(["bash", "-c", f"""
cd /mnt/hdd_1/rediet/deltaSVM
for CHR in {CHROMS_RUN}; do
    echo "=== Starting $CHR ==="
    bash /mnt/hdd_1/rediet/deltaSVM/restart_chr.sh $CHR
    echo "=== $CHR FULLY DONE ==="
done
echo "ALL DONE!"
    """], capture_output=True, text=True)
    print(result_pipeline.stdout)
    print(result_pipeline.stderr)
    return CHROMS_RUN, result_pipeline


@app.cell
def __(mo):
    mo.md(r"""## 9. Check Pipeline Progress""")
    return


@app.cell
def __(subprocess):
    result_progress = subprocess.run(["bash", "-c", """
for CHR in chr1 chr2 chr3 chr4 chr5 chr6 chr7 chr8 chr9 chr10 chr11 chr12 chr13 chr14 chr15 chr16 chr17 chr18 chr19 chr20 chr21 chr22; do
    COUNT=$(ls /mnt/hdd_1/rediet/deltaSVM/runs/$CHR/tmp/*.merge.gkm.tsv 2>/dev/null | wc -l)
    PARQUET=$(ls /mnt/hdd_1/rediet/deltaSVM/runs/$CHR/out/*_deltasvm.parquet 2>/dev/null && echo "done" || echo "not done")
    echo "$CHR: $COUNT/94 TFs | $PARQUET"
done
    """], capture_output=True, text=True)
    print(result_progress.stdout)
    print(result_progress.stderr)
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
