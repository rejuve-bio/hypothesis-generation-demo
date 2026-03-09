import importlib.util
from pathlib import Path

_app_py = Path(__file__).resolve().parent / "app.py"
_spec = importlib.util.spec_from_file_location("root_app", _app_py)
_root_app = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_root_app)

create_app_with_config = _root_app.create_app_with_config
