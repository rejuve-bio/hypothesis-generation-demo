#!/usr/bin/env bash
set -euo pipefail

OUT="$1"
CORES="${2:-8}"
if [[ -z "${OUT}" ]]; then
  echo "Usage: $0 <output_dir> [cores]"
  exit 1
fi

REF_DIR="$OUT/ref_grch38"
VCF_IN="$OUT/vcf"
VCF_NORM="$OUT/vcf_norm"
mkdir -p "$REF_DIR" "$VCF_NORM"

log() { printf '%s %s\n' "$(date -Iseconds)" "$*"; }

# GRCh38 reference (bgzip + faidx)
REF_BGZ="$REF_DIR/GRCh38.primary_assembly.fa.bgz"
if [[ ! -f "$REF_BGZ" ]]; then
  log "Fetching GRCh38 FASTA (no chr prefix)..."
  wget -O "$REF_DIR/GRCh38.fa.gz" \
    "https://ftp.ensembl.org/pub/release-110/fasta/homo_sapiens/dna/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz"
  zcat "$REF_DIR/GRCh38.fa.gz" | bgzip -c >"$REF_BGZ"
  samtools faidx "$REF_BGZ"
fi

export REF_BGZ

CHR_RENAME="$REF_DIR/chr_rename.txt"
if [[ ! -f "$CHR_RENAME" ]]; then
  log "Creating chromosome rename file..."
  for i in {1..22} X Y MT; do
    echo "chr${i} ${i}" >>"$CHR_RENAME"
  done
fi

log "Starting VCF normalization using $CORES cores..."
find "$VCF_IN" -maxdepth 1 -name "*.vcf.gz" | parallel -j "$CORES" '
  log "Working on '${VCF_IN}' ..."
  in="{}"
  b=$(basename "$in" .vcf.gz)

  # 0) Remove "chr" prefix from chromosome names
  bcftools annotate --rename-chrs "'"$CHR_RENAME"'" "$in" \
    -Oz -o "'"$VCF_NORM"'/${b}.nochr.vcf.gz" || exit 1
  tabix -f "'"$VCF_NORM"'/${b}.nochr.vcf.gz"

  # 1) Split multi-allelics & left-align to reference; ensure REF matches FASTA
  bcftools norm -m -both -f "$REF_BGZ" "'"$VCF_NORM"'/${b}.nochr.vcf.gz" \
    -Oz -o "'"$VCF_NORM"'/${b}.norm.vcf.gz" || exit 1
  tabix -f "'"$VCF_NORM"'/${b}.norm.vcf.gz"

  # 2) Deterministic IDs (CHR:POS:REF:ALT)
  bcftools annotate --set-id "%CHROM:%POS:%REF:%ALT" \
    "'"$VCF_NORM"'/${b}.norm.vcf.gz" -Oz -o "'"$VCF_NORM"'/${b}.setid.vcf.gz" || exit 1
  tabix -f "'"$VCF_NORM"'/${b}.setid.vcf.gz"

  # 3) Compute AF/AC/AN/MAF (NOTE: -Oz/-o BEFORE -- ; plugin opts AFTER --)
  bcftools +fill-tags "'"$VCF_NORM"'/${b}.setid.vcf.gz" \
    -Oz -o "'"$VCF_NORM"'/${b}.tags.vcf.gz" \
    -- -t MAF,AC,AN || exit 1
  tabix -f "'"$VCF_NORM"'/${b}.tags.vcf.gz"

  # 4) Keep common biallelic SNPs (tune thresholds as you like)
  bcftools view -i "MAF>=0.01 && TYPE=\"snp\"" \
    "'"$VCF_NORM"'/${b}.tags.vcf.gz" -Oz -o "'"$VCF_NORM"'/${b}.final.vcf.gz" || exit 1
  tabix -f "'"$VCF_NORM"'/${b}.final.vcf.gz"

  # Clean up intermediate files (optional)
  rm -f "'"$VCF_NORM"'/${b}.nochr.vcf.gz" "'"$VCF_NORM"'/${b}.nochr.vcf.gz.tbi"

  log "Done for '${VCF_IN}'. Result writen to '${VCF_NORM}'"
'
log "VCF normalisation script finished. Normalized files are in ${VCF_NORM}"
