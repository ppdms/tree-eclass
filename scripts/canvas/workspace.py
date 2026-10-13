"""Owned disposable workspace, resource guard and exact input checks for the Taffy recipe."""

import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time

REPO = Path(__file__).resolve().parents[2]
RECIPES = Path(__file__).resolve().parent


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def verify_hashes(root, inventory):
    for relative, expected in inventory.items():
        path = contained(root, relative)
        if not path.is_file() or digest(path) != expected:
            raise RuntimeError(f"Pinned input hash mismatch: {path}")


def contained(root, relative):
    root = Path(root).resolve(strict=True)
    relative = Path(relative)
    if relative.is_absolute() or ".." in relative.parts:
        raise RuntimeError(f"Expected a bounded relative path: {relative}")
    path = root / relative
    if not path.resolve().is_relative_to(root):
        raise RuntimeError(f"Path escapes its owned root: {path}")
    return path


def verify_products(root, inventory, exact=False):
    if exact:
        actual = {str(path.relative_to(root)) for path in root.rglob("*") if path.is_file()}
        if actual - {"build-manifest.json"} != set(inventory):
            raise RuntimeError("Packaged file inventory differs from the pinned candidate")
        if any(path.is_symlink() for path in root.rglob("*")):
            raise RuntimeError("Packaged files must not contain symlinks")
    for relative, expected in inventory.items():
        path = contained(root, relative)
        if (
            path.is_symlink()
            or not path.is_file()
            or path.stat().st_size != expected["bytes"]
            or digest(path) != expected["sha256"]
        ):
            raise RuntimeError(f"Pinned product byte count or SHA-256 mismatch: {path}")


def fingerprint():
    inventory = {}
    for path in sorted(RECIPES.rglob("*")):
        if "__pycache__" in path.parts:
            continue
        if path.is_symlink():
            raise RuntimeError(f"Permanent recipe must not depend on symlinks: {path}")
        if path.is_file():
            inventory[str(path.relative_to(RECIPES))] = digest(path)
    return inventory


class Workspace:
    def __init__(self, path, kind, policy, create=False, cleaning=False):
        supplied = Path(path).expanduser().absolute()
        if supplied.is_symlink():
            raise RuntimeError("Workspace itself must not be a symlink")
        self.root = supplied.resolve()
        self.policy = policy
        build_root = REPO / ".tree-build"
        in_build = self.root != build_root and self.root.is_relative_to(build_root)
        if (self.root.is_relative_to(REPO) and not in_build) or REPO.is_relative_to(self.root):
            raise RuntimeError("Workspace must be outside the source tree or below .tree-build")
        if create:
            self.root.mkdir(exist_ok=True)
            if any(self.root.iterdir()):
                raise RuntimeError("Preparation requires a new, empty workspace")
        self.identity = self.root.stat()
        marker = self.root / ".canvas-workspace.json"
        self.owner = {
            "kind": kind,
            "root": str(self.root),
            "device": self.identity.st_dev,
            "inode": self.identity.st_ino,
        }
        if create:
            self.guard(initial=True)
            marker.write_text(json.dumps({**self.owner, "recipe": fingerprint()}, indent=2) + "\n")
        else:
            receipt = self.receipt()
            if not cleaning and receipt.get("recipe") != fingerprint():
                raise RuntimeError("Recipe changed; use a new workspace, or clean this owned workspace explicitly")
        self.lock = os.open(self.root / ".canvas-workspace.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.env = {} if cleaning else self.environment()

    def receipt(self):
        current = self.root.stat()
        if self.root.is_symlink() or (current.st_dev, current.st_ino) != (self.identity.st_dev, self.identity.st_ino):
            raise RuntimeError("Workspace identity changed")
        marker = self.root / ".canvas-workspace.json"
        if marker.is_symlink() or not marker.is_file():
            raise RuntimeError("Missing regular workspace ownership marker")
        receipt = json.loads(marker.read_text())
        if any(receipt.get(key) != value for key, value in self.owner.items()):
            raise RuntimeError("Workspace ownership marker does not match this directory")
        return receipt

    def environment(self):
        allowed = (
            "PATH",
            "LANG",
            "LC_ALL",
            "SSL_CERT_FILE",
            "SSL_CERT_DIR",
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "NO_PROXY",
            "http_proxy",
            "https_proxy",
            "no_proxy",
        )
        result = {key: os.environ[key] for key in allowed if key in os.environ}
        for variable, name in (("HOME", "home"), ("TMPDIR", "tmp"), ("XDG_CACHE_HOME", "cache")):
            target = contained(self.root, name)
            if target.is_symlink():
                raise RuntimeError(f"Owned environment directory must not be a symlink: {target}")
            target.mkdir(exist_ok=True)
            result[variable] = str(target)
        configs = {}
        for name in ("user", "global"):
            target = contained(self.root, f"home/npm-{name}config")
            if target.is_symlink() or (target.exists() and (not target.is_file() or target.stat().st_size)):
                raise RuntimeError(f"Owned npm configuration must be an empty regular file: {target}")
            target.touch(exist_ok=True)
            configs[name] = str(target)
        result.update(
            npm_config_cache=str(self.root / "cache/npm"),
            npm_config_userconfig=configs["user"],
            npm_config_globalconfig=configs["global"],
            npm_config_ignore_scripts="true",
            npm_config_audit="false",
            npm_config_fund="false",
            npm_config_update_notifier="false",
            npm_config_maxsockets="1",
            BINARYEN_CORES="1",
            PYTHONDONTWRITEBYTECODE="1",
        )
        return result

    def guard(self, initial=False):
        size = 0
        for directory, directories, names in os.walk(self.root):
            for name in directories + names:
                path = Path(directory) / name
                try:
                    if path.is_symlink():
                        if not path.resolve().is_relative_to(self.root):
                            raise RuntimeError(f"Workspace symlink escapes its owned root: {path}")
                    elif path.is_file():
                        size += path.stat().st_size
                except FileNotFoundError:
                    pass
        floor = self.policy["minimum_initial_free_bytes" if initial else "minimum_remaining_free_bytes"]
        free = shutil.disk_usage(self.root).free
        if size > self.policy["maximum_workspace_bytes"] or free < floor:
            raise RuntimeError(f"Disk guard: workspace={size} bytes, available={free} bytes")

    def run(self, command, cwd, deadline=3600):
        self.receipt()
        self.guard()
        print("+", " ".join(map(str, command)), flush=True)
        return self.command(command, cwd, deadline)

    def command(self, command, cwd, deadline):
        process = subprocess.Popen(list(map(str, command)), cwd=cwd, env=self.env, start_new_session=True)
        started = time.monotonic()
        try:
            while process.poll() is None:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.guard()
                    if time.monotonic() - started > deadline:
                        raise RuntimeError("Recipe command deadline exceeded")
            self.guard()
            if process.returncode:
                raise subprocess.CalledProcessError(process.returncode, command)
        except BaseException:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            if process.poll() is None:
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    pass
            try:
                # The leader can exit before an orphaned compiler/download child.
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            if process.poll() is None:
                process.wait()
            raise

    def remove(self, relative):
        self.receipt()
        path = contained(self.root, relative)
        if path == self.root:
            raise RuntimeError("Use clean for the owned workspace root")
        if path.is_symlink():
            raise RuntimeError(f"Refusing to recursively remove a symlink: {path}")
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()

    def prune_caches(self):
        for name in ("cache", "tmp"):
            self.remove(name)
            (self.root / name).mkdir()

    def clean(self):
        self.receipt()
        shutil.rmtree(self.root)
