"""Client for the platform file service (``/v1/files``).

When an analysis starts, the user's token and our consumer key ask the file service
for a grant on the chosen file. The worker later trades the grant id and the key
(FILES_API_SERVICE_TOKEN) for a short link, so no user token is kept.
"""
import hashlib
import os
import re

import requests
from loguru import logger

TIMEOUT = 30
DOWNLOAD_TIMEOUT = 600
HEAD_BYTES = 1024 * 1024
FILE_ID = re.compile(r"^f_[0-9a-f]{32}$")

SERVICE_HEADER = "X-Consumer-Key"

FILE_GONE = "This file is no longer available. Pick it again to rerun."
DOWNLOAD_FAILED = "Couldn't download the file from the file service. Try again in a moment."
TRY_AGAIN = "The file service isn't available right now. Try again in a moment."
REFUSED = {  # file service error code -> what the user sees
    "not_ready": "This file is still being processed. Try again once it's ready.",
    "not_compatible": "This file can't be used for an analysis. Pick a GWAS summary statistics file.",
}


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
        logger.error("[FILES_API] FILES_API_URL or FILES_API_SERVICE_TOKEN is not set")
        raise FilesApiError(503, "Files from the file service can't be used right now.")
    try:
        resp = requests.request(
            method, f"{_base_url()}{path}", headers=headers, timeout=TIMEOUT, **kwargs
        )
    except requests.RequestException as exc:
        logger.warning(f"[FILES_API] {method} {path} failed: {exc}")
        raise FilesApiError(502, TRY_AGAIN)
    status = resp.status_code
    if status < 400:
        return resp
    error = _error(resp)
    code = error.get("code")
    logger.warning(f"[FILES_API] {method} {path} -> {status} {code}: {error.get('message')}")
    if code == "invalid_consumer_key":
        logger.error("[FILES_API] The file service rejected FILES_API_SERVICE_TOKEN")
        raise FilesApiError(502, TRY_AGAIN)
    if status == 401:
        raise FilesApiError(401, "Your session has expired. Sign in again and retry.")
    if status in (403, 404):
        raise FilesApiError(404, "File not found. It may have been deleted.")
    if status == 409:
        raise FilesApiError(409, REFUSED.get(code, "This file can't be used right now."))
    if status == 429:
        raise FilesApiError(429, "Too many requests to the file service. Try again in a few minutes.")
    raise FilesApiError(502, TRY_AGAIN)


def read_head(file_id: str, authorization: str) -> bytes:
    headers = {"Authorization": authorization, "Range": f"bytes=0-{HEAD_BYTES - 1}"}
    return _call("GET", f"/v1/files/{file_id}/content", headers).content


def register_file(file_id: str, authorization: str, user_id: str, files) -> tuple[dict, str]:
    """Get a grant on the file and return its local file_metadata doc and the grant id.

    The file service refuses with 409 when the file is not ready or not usable by Hypothesis.
    """
    if not FILE_ID.match(file_id):
        raise FilesApiError(400, "file_id must be a file service id (starting with f_).")
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
    try:
        # The link redirects exactly once, to a short storage URL.
        first = requests.get(link["url"], allow_redirects=False, timeout=TIMEOUT)
        location = first.headers.get("Location")
        if first.status_code not in (302, 303, 307) or not location:
            raise RuntimeError(f"link answered HTTP {first.status_code}, expected a redirect")
        with requests.get(location, stream=True, allow_redirects=False,
                          timeout=DOWNLOAD_TIMEOUT) as resp:
            if resp.status_code != 200:
                raise RuntimeError(f"storage answered HTTP {resp.status_code}")
            with open(partial, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    written += len(chunk)
                    if written > size:
                        break
                    digest.update(chunk)
                    fh.write(chunk)
        if written != size or (expected and digest.hexdigest() != expected):
            raise RuntimeError(f"got {written} bytes, expected {size} with sha256 {expected}")
        os.replace(partial, dest_path)
    except (requests.RequestException, RuntimeError) as exc:
        logger.error(f"[FILES_API] Download for grant {grant_id} failed: {exc}")
        raise RuntimeError(DOWNLOAD_FAILED) from exc
    finally:
        if os.path.exists(partial):
            os.remove(partial)
