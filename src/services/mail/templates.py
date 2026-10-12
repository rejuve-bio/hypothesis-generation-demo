from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


@dataclass(frozen=True)
class EmailPayload:
    to: str
    subject: str
    body_html: str
    body_text: str


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d · %H:%M UTC")


_BASE_URL = "https://dev.rejuve.bio/assets/email"

_ICON_SUCCESS = (
    f'<img src="{_BASE_URL}/dna-completed.png" '
    'width="140" height="140" alt="" '
    'style="display:block;margin:0 auto;" />'
)

_ICON_FAILED = (
    f'<img src="{_BASE_URL}/dna-failed.png" '
    'width="140" height="140" alt="" '
    'style="display:block;margin:0 auto;" />'
)

_LOGO = (
    f'<img src="{_BASE_URL}/rejuve-logo.png" '
    'width="160" alt="Rejuve Biotech" '
    'style="display:block;margin:0 auto;" />'
)


def _get_user_friendly_error(raw: str | None) -> str:
    if not raw:
        return "An unexpected error occurred during analysis. Please contact your administrator if the issue persists."

    msg = raw.lower()

    if "harmoniz" in msg or "nextflow" in msg:
        return (
            "The harmonization step did not complete. "
            "This may be a temporary resource constraint issue. Please try again later. "
            "If the problem persists, contact your support team."
        )
    if "filter" in msg or "significant" in msg:
        return (
            "No genome-wide significant variants were found after filtering. "
            "Your GWAS file may not contain variants that pass the significance threshold."
        )
    if "cojo" in msg or "independent signal" in msg or "out-of-bounds" in msg or "indexer" in msg:
        return (
            "No independent signals were found after variant filtering. "
            "Try adjusting your MAF threshold or significance cutoff."
        )
    if "fine" in msg or "susie" in msg or "finemap" in msg or "fine-mapping" in msg:
        return (
            "The fine-mapping step did not complete. "
            "This may be due to insufficient variants in one or more genomic regions."
        )
    if "ldsc" in msg or "heritability" in msg or "tissue" in msg:
        return (
            "The tissue enrichment analysis (LDSC) did not complete successfully. "
            "This may be a temporary issue with the reference data."
        )
    if "memory" in msg or "oom" in msg or "killed" in msg or "timeout" in msg:
        return "The pipeline timed out. This may be a temporary issue — please try again."
    if "storage" in msg or "minio" in msg or "download" in msg or "upload" in msg:
        return (
            "There was a problem accessing or downloading your GWAS file. "
            "Please verify the file is available and try again."
        )

    return "An unexpected error occurred during analysis. Please contact your administrator if the issue persists."


def build_complete_email(
    to: str,
    project_name: str,
    project_id: str,
    app_base_url: str,
    first_name: str = "there",
) -> EmailPayload:
    result_url = f"{app_base_url.rstrip('/')}/hypothesis/{project_id}"
    ts = _now_utc()

    html = (
        '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1"></head>'
        '<body style="margin:0;padding:0;background:#ffffff;'
        'font-family:Inter,-apple-system,BlinkMacSystemFont,\'Segoe UI\',sans-serif;">'

        '<table width="100%" cellpadding="0" cellspacing="0" style="background:#ffffff;padding:48px 24px;">'
        '<tr><td align="center">'
        '<table width="620" cellpadding="0" cellspacing="0" style="max-width:620px;width:100%;text-align:center;">'

        '<tr><td align="center" style="padding-bottom:28px;">'
        + _LOGO +
        '</td></tr>'

        '<tr><td align="center" style="padding-bottom:32px;">'
        + _ICON_SUCCESS +
        '</td></tr>'

        '<tr><td align="center" style="padding-bottom:16px;">'
        '<h1 style="margin:0;font-size:26px;font-weight:800;color:#4a4a4a;letter-spacing:-0.02em;">Analysis Complete</h1>'
        '</td></tr>'

        '<tr><td align="center" style="padding-bottom:12px;">'
        f'<p style="margin:0 auto;font-size:20px;line-height:1.45;color:#818181;max-width:540px;">'
        f'Your analysis for project <strong style="color:#4a4a4a;font-weight:600;">{project_name}</strong> '
        'has finished running successfully. Your results are ready to view.</p>'
        '</td></tr>'

        '<tr><td align="center" style="padding-bottom:32px;">'
        f'<p style="margin:0;font-size:14px;font-weight:600;color:#a1a1a1;">Finished at {ts}</p>'
        '</td></tr>'

        '<tr><td align="center" style="padding-bottom:48px;">'
        f'<a href="{result_url}" style="display:inline-block;background:#151515;color:#ffffff;'
        'text-decoration:none;padding:15px 28px;border-radius:8px;'
        'font-size:18px;font-weight:700;">View Result →</a>'
        '</td></tr>'

        '<tr><td align="center" style="border-top:1px solid #f0f0f0;padding-top:24px;">'
        '<p style="margin:0;font-size:12px;color:#cccccc;">Rejuve Biotech · Hypothesis Generation Platform</p>'
        '</td></tr>'

        '</table></td></tr></table>'
        '</body></html>'
    )

    plain = (
        f"REJUVE BIOTECH — Analysis Complete\n"
        f"{'─' * 40}\n\n"
        f"Your analysis for '{project_name}' completed successfully.\n\n"
        f"Finished at : {ts}\n"
        f"View results: {result_url}\n\n"
        "Rejuve Biotech · Hypothesis Generation Platform"
    )

    return EmailPayload(
        to=to,
        subject=f"Rejuve Biotech | Analysis complete — {project_name}",
        body_html=html,
        body_text=plain,
    )


def build_failed_email(
    to: str,
    project_name: str,
    error: Optional[str] = None,
    first_name: str = "there",
) -> EmailPayload:
    ts = _now_utc()
    friendly = _get_user_friendly_error(error)

    html = (
        '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1"></head>'
        '<body style="margin:0;padding:0;background:#ffffff;'
        'font-family:Inter,-apple-system,BlinkMacSystemFont,\'Segoe UI\',sans-serif;">'

        '<table width="100%" cellpadding="0" cellspacing="0" style="background:#ffffff;padding:48px 24px;">'
        '<tr><td align="center">'
        '<table width="620" cellpadding="0" cellspacing="0" style="max-width:620px;width:100%;text-align:center;">'

        '<tr><td align="center" style="padding-bottom:28px;">'
        + _LOGO +
        '</td></tr>'

        '<tr><td align="center" style="padding-bottom:32px;">'
        + _ICON_FAILED +
        '</td></tr>'

        '<tr><td align="center" style="padding-bottom:16px;">'
        '<h1 style="margin:0;font-size:26px;font-weight:800;color:#4a4a4a;letter-spacing:-0.02em;">Analysis failed</h1>'
        '</td></tr>'

        '<tr><td align="center" style="padding-bottom:12px;">'
        f'<p style="margin:0 auto;font-size:20px;line-height:1.45;color:#818181;max-width:540px;">'
        f'Unfortunately your analysis for project <strong style="color:#4a4a4a;font-weight:600;">{project_name}</strong> '
        'did not complete successfully.</p>'
        '</td></tr>'

        '<tr><td align="center" style="padding-bottom:32px;">'
        f'<p style="margin:0;font-size:14px;font-weight:600;color:#a1a1a1;">Failed at {ts}</p>'
        '</td></tr>'

        '<tr><td align="left" style="padding-bottom:40px;">'
        '<table width="100%" cellpadding="0" cellspacing="0" '
        'style="background:#fef2f2;border:1px solid #fecaca;border-radius:12px;">'
        '<tr><td style="padding:20px 22px;text-align:left;">'
        '<p style="margin:0 0 8px;font-size:16px;font-weight:800;color:#991b1b;">What went wrong</p>'
        f'<p style="margin:0;font-size:15px;line-height:1.55;color:#7f1d1d;">{friendly}</p>'
        '</td></tr></table>'
        '</td></tr>'

        '<tr><td align="center" style="border-top:1px solid #f0f0f0;padding-top:24px;">'
        '<p style="margin:0;font-size:12px;color:#cccccc;">Rejuve Biotech · Hypothesis Generation Platform</p>'
        '</td></tr>'

        '</table></td></tr></table>'
        '</body></html>'
    )

    plain = (
        f"REJUVE BIOTECH — Analysis failed\n"
        f"{'─' * 40}\n\n"
        f"The analysis for project '{project_name}' did not complete.\n\n"
        f"Failed at : {ts}\n"
        f"Details   : {friendly}\n\n"
        "Please retry or contact your administrator.\n\n"
        "Rejuve Biotech · Hypothesis Generation Platform"
    )

    return EmailPayload(
        to=to,
        subject=f"Rejuve Biotech | Analysis failed — {project_name}",
        body_html=html,
        body_text=plain,
    )