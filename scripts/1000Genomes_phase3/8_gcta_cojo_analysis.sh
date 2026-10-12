#!/usr/bin/env bash
set -euo pipefail

die(){ echo "Error: $*" >&2; exit 2; }

OUTPUT_DIR=$1
BFIle=$2
SUMSTATS=$3
MAF=${MAF:-0.01}
population=${pop:-EUR}




if [ -z "$OUTPUT_DIR" ] || [ -z "$BFILE" ] || [ -z "$SUMSTATS" ]; then
  echo "Usage: $0 <output_directory> <bfile> <sumstats_file> [population] [cores]"
  exit 1
fi
# Create output directory if it does not exist
mkdir -p "$OUTPUT_DIR"

# Raise error if plink directory or summary statistics file do not exist
if [ ! -d "$BFILE" ]; then
  echo "Error: Plink directory $BFILE does not exist."
  exit 1
fi
if [ ! -f "$SUMSTATS" ]; then
  echo "Error: Summary statistics file $SUMSTATS does not exist."
  exit 1
fi


sumstats=$(basename $SUMSTATS)

# Run GCTA COJO analysis
echo "Running GCTA COJO analysis for population $population with MAF $MAF"

cojo_file_path="$OUTPUT_DIR/$population.$sumstats.maf.$MAF"

echo gcta64 --out $cojo_file_path \
            --bfile $BFILE \
            --cojo-file $SUMSTATS \
            --cojo-slct \
            --maf $MAF