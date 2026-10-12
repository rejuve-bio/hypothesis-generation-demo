from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from src.services.demo.access import resolve_project_access, resolve_project_access_or_none


def test_template_resolves_to_demo_owner():
    registry = MagicMock()
    registry.get_template_by_project_id.return_value = {
        "template_project_id": "template-1", "demo_owner_id": "owner"
    }
    access = resolve_project_access(registry, "viewer", "template-1")
    assert access.requesting_user_id == "viewer"
    assert access.owner_user_id == "owner"
    assert access.mode == "demo_read"
    assert access.is_demo_read is True


def test_owned_project_resolves_to_owner_mode():
    registry = MagicMock()
    registry.get_template_by_project_id.return_value = None
    registry.get_project_by_id.return_value = {"user_id": "user-1"}
    access = resolve_project_access(registry, "user-1", "project-1")
    assert access.owner_user_id == "user-1"
    assert access.mode == "owner"


def test_denied_project_raises_or_returns_none():
    registry = MagicMock()
    registry.get_template_by_project_id.return_value = None
    registry.get_project_by_id.return_value = {"user_id": "another-user"}
    with pytest.raises(HTTPException) as exc:
        resolve_project_access(registry, "user-1", "project-1")
    assert exc.value.status_code == 404
    assert resolve_project_access_or_none(registry, "user-1", "project-1") is None
