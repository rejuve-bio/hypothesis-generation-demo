"""Client for the platform file service (``/v1/files``).

When an analysis starts, the user's token and our consumer key ask the file service
for a grant on the chosen file. The worker later trades the grant id and the key
(FILES_API_SERVICE_TOKEN) for a short link, so no user token is kept.
"""
import hashlib
import os
import re

import requests

TIMEOUT = 30
DOWNLOAD_TIMEOUT = 600
HEAD_BYTES = 1024 * 1024
FILE_ID = re.compile(r"^f_[0-9a-f]{32}$")

SERVICE_HEADER = "X-Consumer-Key"

FILE_GONE = "This file is no longer available. Pick it again to rerun."


class FilesApiError(Exception):
    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


def _base_url() -> str:
    return os.getenv("FILES_API_URL", "").rstrip("/")


def _service_headers() -> dict:
    return {SERVICE_HEADER: os.environ["FILES_API_SERVICE_TOKEN"]}


def _error(resp) -> dict:
    try:
        body = resp.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def _call(method: str, path: str, headers: dict, **kwargs) -> requests.Response:
    if not (_base_url() and os.getenv("FILES_API_SERVICE_TOKEN")):
        raise FilesApiError(503, "File service is not configured")
    try:
        resp = requests.request(
            method, f"{_base_url()}{path}", headers=headers, timeout=TIMEOUT, **kwargs
        )
    except requests.RequestException as exc:
        raise FilesApiError(502, f"File service unreachable: {exc}")
    status = resp.status_code
    if status == 401 and _error(resp).get("code") == "invalid_consumer_key":
        raise FilesApiError(502, "File service rejected FILES_API_SERVICE_TOKEN")
    if status == 401:
        raise FilesApiError(401, "Sign in again to use this file")
    if status in (403, 404):
        raise FilesApiError(404, "File not found")
    if status in (409, 429):  # not ready / not usable by Hypothesis, or too many links
        raise FilesApiError(status, _error(resp).get("message") or f"File service refused ({status})")
    if status >= 400:
        raise FilesApiError(502, f"File service error {status}")
    return resp


def read_head(file_id: str, authorization: str) -> bytes:
    headers = {"Authorization": authorization, "Range": f"bytes=0-{HEAD_BYTES - 1}"}
    return _call("GET", f"/v1/files/{file_id}/content", headers).content


def register_file(file_id: str, authorization: str, user_id: str, files) -> tuple[dict, str]:
    """Get a grant on the file and return its local file_metadata doc and the grant id.

    The file service refuses with 409 when the file is not ready or not usable by Hypothesis.
    """
    if not FILE_ID.match(file_id):
        raise FilesApiError(400, "file_id must be a file service id (f_...)")
    headers = {"Authorization": authorization, **_service_headers()}
    meta = _call(
        "POST", f"/v1/files/{file_id}/grants", headers, json={"for": "hypothesis"}
    ).json()
    grant_id = meta["grant_id"]

    doc = files.find_by_external_id(user_id, file_id)
    if not doc:
        name = meta["name"]
        doc = {
            "user_id": user_id,
            "filename": name,
            "original_filename": name,
            "file_path": "",
            "file_type": "gwas",
            "file_size": meta["size"],
            "source": "files_api",
            "external_file_id": file_id,
        }
        doc["_id"] = files.create_file_metadata(**doc)
    return doc, grant_id


def download_granted(grant_id: str | None, dest_path: str) -> None:
    """Download a granted file to *dest_path*, checking its size and sha256."""
    if not grant_id:
        raise RuntimeError(FILE_GONE)
    try:
        link = _call("POST", f"/v1/grants/{grant_id}/link", _service_headers()).json()
    except FilesApiError as exc:
        if exc.status_code in (404, 409):  # grant revoked or expired, or file deleted
            raise RuntimeError(FILE_GONE) from exc
        raise

    size, expected = link["size"], link.get("sha256")
    digest, written = hashlib.sha256(), 0
    partial = f"{dest_path}.part"
    # The link redirects exactly once, to a short storage URL.
    first = requests.get(link["url"], allow_redirects=False, timeout=TIMEOUT)
    location = first.headers.get("Location")
    if first.status_code not in (302, 303, 307) or not location:
        raise RuntimeError(f"File download failed: HTTP {first.status_code}")
    try:
        with requests.get(location, stream=True, allow_redirects=False,
                          timeout=DOWNLOAD_TIMEOUT) as resp:
            if resp.status_code != 200:
                raise RuntimeError(f"File download failed: HTTP {resp.status_code}")
            with open(partial, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    written += len(chunk)
                    if written > size:
                        break
                    digest.update(chunk)
                    fh.write(chunk)
        if written != size or (expected and digest.hexdigest() != expected):
            raise RuntimeError("Downloaded file does not match the file service's size or checksum")
        os.replace(partial, dest_path)
    finally:
        if os.path.exists(partial):
            os.remove(partial)
