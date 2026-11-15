import asyncio
import gzip
import hashlib
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import requests
from bson import ObjectId
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api import dependencies as deps
from src.api.auth import get_current_user_email, get_current_user_id
from src.api.routes import gwas as gwas_routes
from src.api.routes import projects as project_routes
from src.services import files_api
from src.tasks import project as project_tasks

FILE_ID = "f_" + "0123456789abcdef" * 2


@pytest.fixture
def enabled(monkeypatch):
    monkeypatch.setenv("FILES_API_URL", "http://files.test")
    monkeypatch.setenv("FILES_API_SERVICE_TOKEN", "svc")


def _respond(monkeypatch, status=200, json=None, content=b""):
    calls = []

    def fake_request(method, url, headers=None, **kwargs):
        calls.append((method, url, headers))
        return SimpleNamespace(status_code=status, json=lambda: json, content=content)

    monkeypatch.setattr(files_api.requests, "request", fake_request)
    return calls


def test_disabled_returns_503(monkeypatch):
    monkeypatch.delenv("FILES_API_URL", raising=False)
    with pytest.raises(files_api.FilesApiError) as exc:
        files_api.read_head(FILE_ID, "Bearer u")
    assert exc.value.status_code == 503


@pytest.mark.parametrize(
    ("status", "expected"),
    [(401, 401), (403, 404), (404, 404), (409, 409), (429, 429), (500, 502)],
)
def test_service_errors_map_to_status(enabled, monkeypatch, status, expected):
    _respond(monkeypatch, status=status, json={"code": "x", "message": "refused"})
    with pytest.raises(files_api.FilesApiError) as exc:
        files_api.read_head(FILE_ID, "Bearer u")
    assert exc.value.status_code == expected


def test_rejected_service_key_is_not_a_sign_in_error(enabled, monkeypatch):
    _respond(monkeypatch, status=401, json={"code": "invalid_consumer_key", "message": "no"})
    with pytest.raises(files_api.FilesApiError) as exc:
        files_api.register_file(FILE_ID, "Bearer u", "user-1", MagicMock())
    assert exc.value.status_code == 502 and "FILES_API_SERVICE_TOKEN" in exc.value.detail


def test_unreachable_service_returns_502(enabled, monkeypatch):
    def boom(*args, **kwargs):
        raise requests.ConnectionError("down")

    monkeypatch.setattr(files_api.requests, "request", boom)
    with pytest.raises(files_api.FilesApiError) as exc:
        files_api.read_head(FILE_ID, "Bearer u")
    assert exc.value.status_code == 502


def test_register_rejects_non_file_service_ids(enabled, monkeypatch):
    calls = _respond(monkeypatch)
    with pytest.raises(files_api.FilesApiError) as exc:
        files_api.register_file("gx_abc", "Bearer u", "user-1", MagicMock())
    assert exc.value.status_code == 400 and not calls


def test_register_gets_grant_and_creates_metadata_once(enabled, monkeypatch):
    grant = {"grant_id": "g-1", "name": "a.tsv.gz", "size": 10, "sha256": "ab"}
    calls = []

    def fake_request(method, url, headers=None, **kwargs):
        calls.append((method, url, headers, kwargs))
        return SimpleNamespace(status_code=201, json=lambda: grant)

    monkeypatch.setattr(files_api.requests, "request", fake_request)
    files = MagicMock()
    files.find_by_external_id.return_value = None
    files.create_file_metadata.return_value = "doc-1"

    doc, grant_id = files_api.register_file(FILE_ID, "Bearer u", "user-1", files)
    assert (doc["_id"], grant_id) == ("doc-1", "g-1")
    method, url, headers, kwargs = calls[0]
    assert (method, url) == ("POST", f"http://files.test/v1/files/{FILE_ID}/grants")
    assert headers == {"Authorization": "Bearer u", files_api.SERVICE_HEADER: "svc"}
    assert kwargs["json"] == {"for": "hypothesis"}
    assert files.create_file_metadata.call_args.kwargs["source"] == "files_api"

    files.find_by_external_id.return_value = {"_id": "doc-1"}
    files.create_file_metadata.reset_mock()
    assert files_api.register_file(FILE_ID, "Bearer u", "user-1", files)[0]["_id"] == "doc-1"
    files.create_file_metadata.assert_not_called()


DATA = b"SNP\tP\nrs1\t0.1\n"


def _grant_link(monkeypatch, status=200, sha256=None, data=DATA, redirect=302):
    link = {"url": "http://file-service.test/v1/links/tok/a.tsv", "size": len(DATA),
            "sha256": sha256 or hashlib.sha256(DATA).hexdigest()}
    calls = _respond(monkeypatch, status=status, json=link)
    first = SimpleNamespace(status_code=redirect, headers={"Location": "http://storage.test/o?s"})
    stream = MagicMock(status_code=200)
    stream.iter_content.return_value = [data]
    stream.__enter__.return_value = stream
    get = MagicMock(side_effect=[first, stream])
    monkeypatch.setattr(files_api.requests, "get", get)
    return calls, get


def test_download_granted_checks_and_writes(enabled, monkeypatch, tmp_path):
    calls, get = _grant_link(monkeypatch)
    dest = tmp_path / "a.tsv"
    files_api.download_granted("g-1", str(dest))
    assert dest.read_bytes() == DATA
    assert calls[0][1] == "http://files.test/v1/grants/g-1/link"
    assert calls[0][2] == {files_api.SERVICE_HEADER: "svc"}
    assert get.call_args_list[1].args[0] == "http://storage.test/o?s"
    assert all(c.kwargs["allow_redirects"] is False for c in get.call_args_list)


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"sha256": "0" * 64}, "checksum"),
        ({"data": DATA + b"extra"}, "checksum"),
        ({"status": 404}, "no longer available"),
        ({"redirect": 200}, "download failed"),
    ],
)
def test_download_granted_refuses_bad_or_dead(enabled, monkeypatch, tmp_path, kwargs, error):
    _grant_link(monkeypatch, **kwargs)
    dest = tmp_path / "a.tsv"
    with pytest.raises(RuntimeError, match=error):
        files_api.download_granted("g-1", str(dest))
    assert not dest.exists() and not (tmp_path / "a.tsv.part").exists()


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    app = FastAPI()
    app.include_router(project_routes.router)
    projects = MagicMock()
    projects.create_project.return_value = "project-1"
    app.dependency_overrides.update({
        get_current_user_id: lambda: "user-1",
        get_current_user_email: lambda: None,
        deps.get_project_handler: lambda: projects,
        deps.get_file_handler: lambda: MagicMock(),
        deps.get_config: lambda: MagicMock(),
        deps.get_storage: lambda: None,
        deps.get_gwas_library_handler: lambda: MagicMock(),
    })
    invoke = MagicMock()
    monkeypatch.setattr(project_routes, "invoke_analysis_pipeline_deployment", invoke)
    return TestClient(app), invoke, projects


FORM = {"project_name": "p", "phenotype": "height"}


def test_route_rejects_file_id_with_gwas_file(client):
    http, _, _ = client
    resp = http.post("/analysis-pipeline", data={**FORM, "file_id": FILE_ID, "gwas_file": "lib-1"})
    assert resp.status_code == 400


def test_route_starts_pipeline_from_file_id(client, monkeypatch):
    http, invoke, projects = client
    register = MagicMock(
        return_value=({"_id": "doc-1", "filename": "a.tsv.gz", "file_size": 10}, "g-1")
    )
    monkeypatch.setattr(files_api, "register_file", register)

    resp = http.post(
        "/analysis-pipeline",
        data={**FORM, "file_id": FILE_ID},
        headers={"Authorization": "Bearer u"},
    )
    assert resp.status_code == 202, resp.text
    assert register.call_args.args[:3] == (FILE_ID, "Bearer u", "user-1")
    assert projects.create_project.call_args.kwargs["file_grant_id"] == "g-1"
    assert invoke.call_args.kwargs["file_metadata_id"] == "doc-1"


def test_worker_downloads_with_saved_grant(monkeypatch, tmp_path):
    files = MagicMock()
    files.get_file_metadata.return_value = {"filename": "height.tsv", "source": "files_api"}
    projects = MagicMock()
    projects.get_projects.return_value = {"file_grant_id": "g-1"}
    monkeypatch.setattr(project_tasks, "get_deps", lambda: {"files": files, "projects": projects})
    monkeypatch.setattr(project_tasks, "emit_analysis_update", lambda *a: None)
    download = MagicMock(side_effect=lambda grant, dest: open(dest, "wb").write(DATA))
    monkeypatch.setattr(files_api, "download_granted", download)

    path = project_tasks.prepare_gwas_file_task.fn(
        "user-1", "project-1", "doc-1", "", output_dir=str(tmp_path)
    )
    assert path == str(tmp_path / "height.tsv")
    assert download.call_args.args[0] == "g-1"
    files.update_file_metadata.assert_called_once()


@pytest.mark.parametrize(
    ("file_id", "local_doc", "requested"),
    [
        (FILE_ID, None, FILE_ID),
        (str(ObjectId()), {"external_file_id": FILE_ID}, FILE_ID),
    ],
)
def test_preview_reads_file_head(enabled, monkeypatch, file_id, local_doc, requested):
    head = gzip.compress(b"SNP\tP\tN\nrs1\t0.1\t1000\n")
    read = MagicMock(return_value=head)
    monkeypatch.setattr(files_api, "read_head", read)
    files = MagicMock()
    files.get_file_metadata.return_value = local_doc
    library = MagicMock()
    library.get_gwas_entry.return_value = None

    resp = asyncio.run(gwas_routes.get_sample_size_info(
        SimpleNamespace(headers={"authorization": "Bearer u"}),
        file_id, gwas_source="upload", current_user_id="user-1",
        gwas_library=library, files=files,
    ))
    assert read.call_args.args == (requested, "Bearer u")
    assert resp.sample_size_source == "file_has_n"


def test_preview_does_not_ask_file_service_for_other_ids(enabled, monkeypatch):
    read = MagicMock()
    monkeypatch.setattr(files_api, "read_head", read)
    library = MagicMock()
    library.get_gwas_entry.return_value = None

    with pytest.raises(gwas_routes.HTTPException) as exc:
        asyncio.run(gwas_routes.get_sample_size_info(
            SimpleNamespace(headers={}), "not-a-file", gwas_source="upload",
            current_user_id="user-1", gwas_library=library, files=MagicMock(),
        ))
    assert exc.value.status_code == 404
    read.assert_not_called()
