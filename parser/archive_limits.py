"""Native child limits shared by the bounded archive subprocess runner."""

import shutil
import sys

from .extractors import ExtractionError


def resource_limited_command(command, timeout_seconds):
    cpu_seconds = max(1, int(timeout_seconds)) + 5
    if sys.platform == "darwin":
        # Darwin's shared-cache address space makes a Linux RLIMIT_AS ceiling
        # inappropriate. Bound CPU/descriptors here; Go supervises RSS and time.
        code = (
            "import os,resource,sys; "
            "cpu=int(sys.argv[1]); "
            "resource.setrlimit(resource.RLIMIT_CPU,(cpu,cpu)); "
            "resource.setrlimit(resource.RLIMIT_NOFILE,(64,64)); "
            "os.execv(sys.argv[2],sys.argv[2:])"
        )
        return [sys.executable, "-I", "-S", "-c", code, str(cpu_seconds), *command]
    limiter = shutil.which("prlimit")
    if not limiter:
        raise ExtractionError("prlimit is required for bounded RAR ingestion", reason="archive_tool_unavailable")
    return [
        limiter,
        f"--cpu={cpu_seconds}",
        "--as=1073741824",
        "--nofile=64",
        "--nproc=1",
        "--",
        *command,
    ]
