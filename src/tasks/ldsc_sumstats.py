from __future__ import annotations

import os

import pandas as pd
from loguru import logger


def _resolve_default_n(harmonized_path: str, df: pd.DataFrame) -> int:
    """Extract N from harmonized table columns. Raises if not found."""
    for col in df.columns:
        low = str(col).lower()
        if low == "n" or ("sample" in low and "size" in low) or low == "neff":
            try:
                return int(pd.to_numeric(df[col], errors="coerce").max())
            except Exception:
                pass
    raise ValueError(
        f"No sample size column (N / sample_size / Neff) found in "
        f"{os.path.basename(harmonized_path)}. "
        "Ensure the harmonized file contains an N column."
    )


def harmonized_to_ldsc_sumstats_zhang(
    harmonized_gz: str,
    out_gz: str,
    w_hm3_snplist: str,
    *,
    default_n: int | None = None,
) -> str:
    """
    Read harmonized .tsv.gz, apply HM3 snplist allele filter + strand-ambiguous removal,
    write LDSC sumstats.gz with columns SNP, A1, A2, Z, N.
    """
    harmonized_gz = os.path.abspath(harmonized_gz)
    out_gz = os.path.abspath(out_gz)
    os.makedirs(os.path.dirname(out_gz) or ".", exist_ok=True)

    df = pd.read_csv(harmonized_gz, sep="\t", compression="gzip", low_memory=False)
    if "rsid" not in df.columns and "variant_id" not in [c.lower() for c in df.columns]:
        df = pd.read_csv(harmonized_gz, sep="\t", compression="gzip", index_col=0, low_memory=False)
        if "rsid" not in df.columns and df.index.name and str(df.index.name).lower() in (
            "rsid",
            "variant_id",
        ):
            df = df.reset_index()

    col_lower = {c.lower(): c for c in df.columns}
    rename_map = {}
    for old, new in (
        ("rsid", "SNP"),
        ("effect_allele", "A1"),
        ("other_allele", "A2"),
        ("beta", "BETA"),
        ("standard_error", "SE"),
        ("p_value", "P"),
    ):
        if old in col_lower and col_lower[old] != new:
            rename_map[col_lower[old]] = new
    if rename_map:
        df = df.rename(columns=rename_map)

    if "SNP" not in df.columns:
        col_after = {c.lower(): c for c in df.columns}
        if "variant_id" in col_after:
            df = df.rename(columns={col_after["variant_id"]: "SNP"})
        else:
            raise ValueError("Harmonized file has no rsid / SNP / variant_id column")

    for c in ("A1", "A2", "BETA", "SE"):
        if c not in df.columns:
            raise ValueError(f"Harmonized file missing required column: {c}")
    df["BETA"] = pd.to_numeric(df["BETA"], errors="coerce")
    df["SE"] = pd.to_numeric(df["SE"], errors="coerce")
    df["A1"] = df["A1"].astype(str).str.upper()
    df["A2"] = df["A2"].astype(str).str.upper()
    df["Z"] = df["BETA"] / df["SE"]

    if "N" not in df.columns:
        n_val = default_n if default_n is not None else _resolve_default_n(harmonized_gz, df)
        df["N"] = n_val

    hm3 = pd.read_csv(w_hm3_snplist, sep="\t")[["SNP", "A1", "A2"]]
    df = df.merge(hm3, on="SNP", suffixes=("", "_hm3"))
    df = df[(df["A1"] == df["A1_hm3"]) | (df["A1"] == df["A2_hm3"])].drop(
        columns=["A1_hm3", "A2_hm3"]
    )

    ambig = df.apply(
        lambda r: set([r["A1"], r["A2"]]) in [{"A", "T"}, {"C", "G"}],
        axis=1,
    )
    df = df[~ambig]

    keep = [c for c in ["SNP", "A1", "A2", "Z", "N"] if c in df.columns]
    out = df[keep].dropna(subset=["SNP", "A1", "A2", "Z"])
    out.to_csv(out_gz, sep="\t", index=False, compression="gzip")
    logger.info(f"[LDSC] Wrote {len(out):,} SNPs to {out_gz} (mean |Z|={out['Z'].abs().mean():.3f})")
    return out_gz
