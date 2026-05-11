"""Static task discovery - Layer 1 + part of Layer 3.

Walks the filesystem paths supplied by the framework adapter
(``DiscoveryHints.app_paths``) looking for ``tasks.py`` files, then
AST-parses each one to find ``@shared_task``, ``@app.task``, and
``@task`` decorators. Returns a list of task definitions flagged
``loaded=False`` - these tasks are declared in the codebase but
may not be imported into the current process yet.

Static scanning uses ``ast.parse`` only - it never imports the
source files. This is safe:

- Avoids triggering side effects in tasks.py
- Avoids the "Django apps not ready yet" trap during startup
- Doesn't crash on files with import errors

The trade-off is that dynamically registered tasks (e.g. via a
metaclass or a runtime loop) are NOT picked up here - they land
in Layer 2 (opportunistic) or Layer 3 (reconciliation) instead.
"""

from __future__ import annotations

import ast
import logging
from collections.abc import Iterable
from pathlib import Path

from z4j_core.models import TaskDefinition

logger = logging.getLogger("z4j.adapter.celery.discovery.static")

_ENGINE = "celery"

# File names scanned inside each INSTALLED_APP path.
_CANDIDATE_FILENAMES = (
    "tasks.py",
    "celery.py",
    "celery_tasks.py",
)

# Hard cap on file size - anything larger than this is skipped during
# the static scan. Real ``tasks.py`` files are usually <50 KiB; a 2 MiB
# file is almost certainly auto-generated, vendored, or hostile. We
# refuse to AST-parse it because (a) it would chew through CPU and
# memory on every cold-start scan, and (b) a malicious tasks.py is a
# DoS surface against the agent.
_MAX_TASKS_FILE_BYTES: int = 2 * 1024 * 1024


def discover_static(app_paths: Iterable[Path]) -> list[TaskDefinition]:
    """Static scan of ``tasks.py`` files in the given app paths.

    Args:
        app_paths: Filesystem paths to walk. For Django, these are
                   the directories of each ``INSTALLED_APPS`` entry.

    Returns:
        Task definitions found. Each has ``loaded=False`` so the
        dashboard can flag them with a "declared, not yet loaded"
        badge until the runtime scan confirms otherwise.
    """
    result: list[TaskDefinition] = []
    for root in app_paths:
        for file_path in _candidate_files(root):
            try:
                definitions = _scan_file(file_path)
            except Exception:  # noqa: BLE001
                logger.exception("static scan failed for %s", file_path)
                continue
            result.extend(definitions)
    return result


def _candidate_files(root: Path) -> list[Path]:
    """Return every ``tasks.py``-like file under ``root``.

    Symlinks are filtered out: a symlink in the host's source tree
    might point anywhere on disk (``/etc/passwd``, a NAS mount, the
    parent's ``site-packages`` ...) and silently following it expands
    the agent's read surface beyond what the operator intended. Real
    ``tasks.py`` files are not symlinks; the rare project that uses
    one will have to import the module the normal way.
    """
    if not root.is_dir() or root.is_symlink():
        return []
    results: list[Path] = []
    # Top-level candidates
    for name in _CANDIDATE_FILENAMES:
        candidate = root / name
        if candidate.is_file() and not candidate.is_symlink():
            results.append(candidate)
    # Also scan subdirectories of apps for a ``tasks`` package form
    # (``myapp/tasks/__init__.py`` + ``myapp/tasks/*.py``).
    tasks_pkg = root / "tasks"
    if tasks_pkg.is_dir() and not tasks_pkg.is_symlink():
        for file_path in tasks_pkg.glob("*.py"):
            if file_path.is_symlink():
                continue
            if file_path.name == "__init__.py" or not file_path.name.startswith("_"):
                results.append(file_path)
    return results


def _scan_file(file_path: Path) -> list[TaskDefinition]:
    """Parse one Python file with ``ast`` and extract decorated tasks."""
    try:
        stat = file_path.stat()
    except OSError:
        return []
    if stat.st_size > _MAX_TASKS_FILE_BYTES:
        logger.warning(
            "z4j celery static scan: skipping %s (%d bytes > %d limit)",
            file_path,
            stat.st_size,
            _MAX_TASKS_FILE_BYTES,
        )
        return []
    try:
        text = file_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return []

    try:
        tree = ast.parse(text, filename=str(file_path))
    except SyntaxError:
        # Don't crash on files with syntax errors - the user's own
        # linter will catch them. Just skip this file.
        return []

    module_name = _guess_module_name(file_path)
    found: list[TaskDefinition] = []

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if not _has_task_decorator(node):
                continue
            full_name = f"{module_name}.{node.name}" if module_name else node.name
            signature = _render_signature(node)
            found.append(
                TaskDefinition(
                    name=full_name,
                    module=module_name,
                    engine=_ENGINE,
                    queue=_queue_from_decorator(node),
                    signature=signature,
                    declared_in=str(file_path),
                    loaded=False,
                    tags=[],
                ),
            )
    return found


def _has_task_decorator(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """Return True if the function has a Celery-task decorator."""
    for decorator in node.decorator_list:
        name = _decorator_name(decorator)
        if name is None:
            continue
        # Match: @shared_task, @app.task, @celery.task, @task
        short = name.rsplit(".", 1)[-1]
        if short in {"shared_task", "task"}:
            return True
    return False


def _decorator_name(decorator: ast.expr) -> str | None:
    """Return a dotted name for a decorator node, or None if unknown."""
    if isinstance(decorator, ast.Call):
        return _decorator_name(decorator.func)
    if isinstance(decorator, ast.Name):
        return decorator.id
    if isinstance(decorator, ast.Attribute):
        parts: list[str] = [decorator.attr]
        current: ast.expr = decorator.value
        while isinstance(current, ast.Attribute):
            parts.append(current.attr)
            current = current.value
        if isinstance(current, ast.Name):
            parts.append(current.id)
        return ".".join(reversed(parts))
    return None


def _queue_from_decorator(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str | None:
    """Extract ``queue="..."`` from a decorator call, if present."""
    for decorator in node.decorator_list:
        if not isinstance(decorator, ast.Call):
            continue
        for keyword in decorator.keywords:
            if keyword.arg == "queue" and isinstance(keyword.value, ast.Constant):
                value = keyword.value.value
                if isinstance(value, str):
                    return value
    return None


def _render_signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    """Best-effort human-readable signature from the AST."""
    args = node.args
    parts: list[str] = []

    positional = args.posonlyargs + args.args
    for arg in positional:
        parts.append(_render_arg(arg))

    if args.vararg is not None:
        parts.append("*" + args.vararg.arg)

    for kwonly in args.kwonlyargs:
        parts.append(_render_arg(kwonly))

    if args.kwarg is not None:
        parts.append("**" + args.kwarg.arg)

    rendered = "(" + ", ".join(parts) + ")"
    if len(rendered) > 2000:
        return rendered[:1997] + "..."
    return rendered


def _render_arg(arg: ast.arg) -> str:
    if arg.annotation is None:
        return arg.arg
    try:
        annotation = ast.unparse(arg.annotation)
    except Exception:  # noqa: BLE001
        return arg.arg
    return f"{arg.arg}: {annotation}"


def _guess_module_name(file_path: Path) -> str | None:
    """Guess the dotted import name for a file.

    We walk up the directory tree looking for the furthest ancestor
    containing an ``__init__.py`` - that root plus the remaining
    path gives us a dotted module name. Not always correct (e.g.
    editable installs, namespace packages) but good enough to
    populate a human-readable module field in the dashboard.
    """
    parts: list[str] = []
    current = file_path.parent
    stem = file_path.stem

    while (current / "__init__.py").is_file():
        parts.append(current.name)
        current = current.parent

    if not parts:
        return None

    parts.reverse()
    parts.append(stem)
    return ".".join(parts)


__all__ = ["discover_static"]
