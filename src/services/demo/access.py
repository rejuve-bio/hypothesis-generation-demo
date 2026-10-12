"""Resolve read/write access for owned projects and shared demo templates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from fastapi import HTTPException

from src.db.demo_template_handler import DemoTemplateHandler


@dataclass(frozen=True)
class ProjectAccess:
    requesting_user_id: str
    owner_user_id: str
    project_id: str
    mode: str  # "owner" | "demo_read"
    template: Optional[dict] = None

    @property
    def is_demo_read(self) -> bool:
        return self.mode == "demo_read"


def resolve_project_access(
    demo_templates: DemoTemplateHandler,
    user_id: str,
    project_id: str,
) -> ProjectAccess:
    """Return the MongoDB owner user_id to use for reads and the access mode."""
    template = demo_templates.get_template_by_project_id(project_id)
    if template:
        owner_user_id = DemoTemplateHandler.get_owner_user_id(template)
        return ProjectAccess(
            requesting_user_id=user_id,
            owner_user_id=owner_user_id,
            project_id=project_id,
            mode="demo_read",
            template=template,
        )

    project = demo_templates.get_project_by_id(project_id)
    if project and project.get("user_id") == user_id:
        return ProjectAccess(
            requesting_user_id=user_id,
            owner_user_id=user_id,
            project_id=project_id,
            mode="owner",
        )

    raise HTTPException(status_code=404, detail="Project not found or access denied")


def resolve_project_access_or_none(
    demo_templates: DemoTemplateHandler,
    user_id: str,
    project_id: str,
) -> Optional[ProjectAccess]:
    try:
        return resolve_project_access(demo_templates, user_id, project_id)
    except HTTPException:
        return None
