#!/usr/bin/env bash

OUTPUT_DIR=$1
VCF_NORM=$2
CORES=${3:-8}

if [[ -z "$OUTPUT_DIR" || -z "$VCF_NORM" ]]; then
  echo "Usage: $0 <output_directory> <vcf norm dir> <num of cores>"
  exit 1
fi

PLINK_DIR="$OUTPUT_DIR/plink_format_b38"
# Create if it does not exist
mkdir -p "$PLINK_DIR"

export PLINK_DIR VCF_NORM
# Process vcfs

parallel -j "$CORES" '
  pop={1}; chr={2};
  outdir="'$PLINK_DIR'/{1}"; mkdir -p "$outdir";
  keep="$outdir/{1}.id";
  out="$outdir/{1}.chr{2}.1KG.GRCh38";

  # sanity
  vcf="'$VCF_NORM'/{2}.1KG.GRCh38.final.vcf.gz";
  if [[ ! -s "$vcf" ]]; then
    echo "WARN: missing VCF for chr{2}: $vcf" >&2; exit 0;
  fi
  if [[ ! -s "$keep" ]]; then
    echo "ERROR: keep file not found: $keep" >&2; exit 1;
  fi

  plink2 \
    --vcf "$vcf" \
    --keep "$keep" \
    --make-bed \
    --out "$out"
' ::: EUR AFR AMR EAS SAS ::: {1..22}

parallel -j "$CORES" '
  pop={1}; chr={2};
  outdir="'$PLINK_DIR'/{1}"; mkdir -p "$outdir";
  keep="$outdir/{1}.id";
  out="$outdir/{1}.chr{2}.1KG.GRCh38";

  # sanity
  vcf="'$VCF_NORM'/{2}.1KG.GRCh38.final.vcf.gz";
  if [[ ! -s "$vcf" ]]; then
    echo "WARN: missing VCF for chr{2}: $vcf" >&2; exit 0;
  fi
  if [[ ! -s "$keep" ]]; then
    echo "ERROR: keep file not found: $keep" >&2; exit 1;
  fi

  plink \
    --vcf "$vcf" \
    --keep "$keep" \
    --split-par hg38 \
    --impute-sex \
    --make-bed \
    --out "$out"
' ::: EUR AFR AMR EAS SAS ::: X