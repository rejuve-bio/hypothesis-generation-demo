# Shared GWAS-SSF column-remap functions.
#
# Sourced by 6_harmoniser.sh as part of the full Nextflow harmonization
# pipeline, and reusable standalone (e.g. by harmonize_sumstats_with_nextflow
# in src/tasks/analysis.py) to cheaply reshape a Neale/PLINK2/FinnGen-format
# file into minimal GWAS-SSF columns without running Nextflow or touching a
# reference panel — no allele-flipping/strand-resolution, just column
# renaming, chromosome-name normalization, and dropping X/Y/MT.
#
# Callers must set BASEDIR (defaults to the caller's $PWD if unset) before
# sourcing, and SUMSTATS (required) / SUMSTATS_DIR / SUMSTATS_BASENAME / BUILD
# / COORD (all optional) before calling to_gwas_ssf.

: "${BASEDIR:=$(pwd -P)}"

# Resolve a path against BASEDIR, then normalize to an absolute path.
resolve_path() {
  local p="$1"
  # If it's relative, anchor to BASEDIR
  case "$p" in
    /*) : ;;                         # already absolute
    ~*) p="${p/#\~/$HOME}";;         # expand ~
    *)  p="${BASEDIR%/}/$p";;        # make absolute vs BASEDIR
  esac
  # Normalize even if it doesn't exist (portable fallback if realpath -m missing)
  if command -v realpath >/dev/null 2>&1; then
    realpath -m -- "$p"
  elif command -v python3 >/dev/null 2>&1; then
    python3 - "$p" <<'PY'
import os, sys
print(os.path.abspath(sys.argv[1]))
PY
  else
    # Fallback normalization (no symlink resolution)
    printf '%s\n' "$p"
  fi
}

to_gwas_ssf() {

  set -euo pipefail
  : "${SUMSTATS:?Set SUMSTATS to the input sumstats path}"
  BUILD="${BUILD:-GRCh38}"
  COORD="${COORD:-1-based}"

  SRC="$(resolve_path "$SUMSTATS")"

  # Output dir
  if [[ -n "${SUMSTATS_DIR:-}" ]]; then
    OUT_DIR="$(resolve_path "$SUMSTATS_DIR")"
  else
    OUT_DIR="$(dirname -- "$SRC")"
  fi
  mkdir -p "$OUT_DIR"

  # --- Normalize a "stem" even if SUMSTATS_BASENAME is provided ---
  _stem_from_name() {
    local n="$1"
    n="${n##*/}"
    n="${n%.tsv.gz}"
    n="${n%.tsv.bgz}"
    n="${n%.bgz}"
    n="${n%.gz}"
    n="${n%.tsv}"
    echo "$n"
  }

  if [[ -n "${SUMSTATS_BASENAME:-}" ]]; then
    STEM="$(_stem_from_name "$SUMSTATS_BASENAME")"
  else
    STEM="$(_stem_from_name "$SRC")"
  fi

  OUT_TSV="${OUT_DIR%/}/${STEM}.tsv"
  OUT_GZ="${OUT_TSV}.gz"
  OUT_YAML="${OUT_GZ}-meta.yaml"

  # Reader based on extension
  READER="cat"
  case "$SRC" in *.gz|*.bgz) READER="bgzip -dc";; esac

  # Build minimal GWAS-SSF (Neale/PLINK2/FinnGen/SSF autodetect), force chromosome to "chr*" form
  $READER "$SRC" | awk -v OFS="\t" '
    BEGIN{
      print "chromosome\tbase_pair_location\teffect_allele\tother_allele\tbeta\tstandard_error\tp_value\teffect_allele_frequency\trsid"
    }
    NR==1{
      for(i=1;i<=NF;i++) { col=tolower($i); gsub(/^#/, "", col); h[col]=i }
      is_neale  = h["variant"] && (h["beta"]||h["or"]) && (h["se"]||h["stderr"]||h["standard_error"]) && (h["pval"]||h["p"])
      is_plink  = ((h["chr"]||h["chrom"]) && (h["bp"]||h["pos"]) && h["a1"] && h["a2"] && (h["beta"]||h["or"]) && (h["se"]||h["stderr"]||h["standard_error"]) && (h["p"]||h["pval"]))
      is_finngen = h["chrom"] && h["pos"] && h["ref"] && h["alt"] && h["beta"] && h["sebeta"] && (h["pval"]||h["p"])
      is_ssf    = (h["chromosome"] && h["base_pair_location"] && h["effect_allele"] && h["other_allele"] && h["beta"] && h["standard_error"] && (h["p_value"]||h["p"]))
      if(!is_neale && !is_plink && !is_finngen && !is_ssf){ print "ERROR: unknown layout" > "/dev/stderr"; exit 2 }
      next
    }
    {
      chrom=""; pos=""; ea=""; oa=""; beta=""; se=""; p=""; eaf=""; rsid=""

      if(is_neale){
        split($h["variant"], v, ":"); chrom=v[1]; pos=v[2]; oa=v[3]; ea=v[4]
        if(h["beta"]) beta=$h["beta"]
        if(h["se"]) se=$h["se"]; else if(h["stderr"]) se=$h["stderr"]; else if(h["standard_error"]) se=$h["standard_error"]
        if(h["pval"]) p=$h["pval"]; else if(h["p"]) p=$h["p"]
        if(h["af"]) eaf=$h["af"]; else if(h["minor_af"]) eaf=$h["minor_af"]; else if(h["effect_allele_frequency"]) eaf=$h["effect_allele_frequency"]
        if(h["rsid"]) rsid=$h["rsid"]; else if(h["snp"]) rsid=$h["snp"]
      } else if(is_ssf) {
        chrom = $h["chromosome"]
        pos   = $h["base_pair_location"]
        ea    = $h["effect_allele"]
        oa    = $h["other_allele"]
        beta  = $h["beta"]
        se    = $h["standard_error"]
        if(h["p_value"]) p=$h["p_value"]; else p=$h["p"]
        if(h["effect_allele_frequency"]) eaf=$h["effect_allele_frequency"]; else eaf="NA"
        if(h["rsid"]) rsid=$h["rsid"]; else if(h["variant_id"]) rsid=$h["variant_id"]; else rsid="NA"
      } else if(is_finngen) {
        chrom = $h["chrom"]
        pos   = $h["pos"]
        ea    = $h["alt"]
        oa    = $h["ref"]
        beta  = $h["beta"]
        se    = $h["sebeta"]
        if(h["pval"]) p=$h["pval"]; else p=$h["p"]
        if(h["af_alt"]) eaf=$h["af_alt"]; else eaf="NA"
        if(h["rsids"]) rsid=$h["rsids"]; else if(h["rsid"]) rsid=$h["rsid"]; else rsid="NA"
      } else {
        chrom = (h["chr"]? $h["chr"] : $h["chrom"])
        pos   = (h["bp"]?  $h["bp"]  : $h["pos"])
        ea    = $h["a1"]; oa=$h["a2"]
        if(h["beta"]) beta=$h["beta"]
        if(h["se"]) se=$h["se"]; else if(h["stderr"]) se=$h["stderr"]; else if(h["standard_error"]) se=$h["standard_error"]
        p     = (h["p"]? $h["p"] : $h["pval"])
        if(h["a1_freq"]) eaf=$h["a1_freq"]; else if(h["frq"]) eaf=$h["frq"]; else if(h["effect_allele_frequency"]) eaf=$h["effect_allele_frequency"]
        if(h["id"]) rsid=$h["id"]; else if(h["snp"]) rsid=$h["snp"]; else if(h["rsid"]) rsid=$h["rsid"]
      }

      # --- normalize chromosome to chr* form ---
      # strip any existing "chr", convert numeric proxies 23/24/26, then re-prefix "chr"
      sub(/^chr/,"",chrom)
      if(chrom=="23") chrom="X";
      else if(chrom=="24") chrom="Y";
      else if(chrom=="26") chrom="MT";

      # FILTER: Skip X, Y, MT chromosoems
      if (chrom=="X" || chrom=="Y" || chrom=="MT") next

      if(eaf=="") eaf="NA"; if(rsid=="") rsid="NA"
      print chrom, pos, toupper(ea), toupper(oa), beta, se, p, eaf, rsid
    }
  ' > "$OUT_TSV"

  # Compress and index
  bgzip -f "$OUT_TSV"
  tabix -c N -S 1 -s 1 -b 2 -e 2 "$OUT_GZ" 2>/dev/null || true

  # md5 for sidecar (required by pipeline metadata model)
  MD5="$(md5sum < "$OUT_GZ" | awk "{print \$1}")"

  # Sidecar YAML
  cat > "$OUT_YAML" <<YAML
# Study meta-data
date_metadata_last_modified: $(date +%F)

# Genotyping Information
genome_assembly: GRCh${BUILD}
coordinate_system: ${COORD}

# Summary Statistic information
data_file_name: $(basename "$OUT_GZ")
file_type: GWAS-SSF v0.1
data_file_md5sum: ${MD5}

# Harmonization status
is_harmonised: false
is_sorted: false
YAML

  export SSF_GZ="$OUT_GZ"
  export SSF_YAML="$OUT_YAML"
  echo "$OUT_GZ"
}
