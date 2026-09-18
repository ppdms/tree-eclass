"""One-operation JSON protocol for Go-owned parsing; no application initialization."""

from dataclasses import asdict
import base64
import json
import os
from pathlib import Path
import sys
import tempfile

from .extractors import ExtractionError, ExtractionLimits, extractor_for
from .models import SourceMetadata


def emit(event_type, **value):
    print(json.dumps({"type": event_type, **value}, ensure_ascii=False), flush=True)


def deny_network(event, _args):
    if event.startswith("socket."):
        raise RuntimeError("Document parsers cannot open network connections")


def extract(request):
    source = SourceMetadata(**request["source"])
    limits = ExtractionLimits(**request.get("limits", {}))
    _, extractor = extractor_for(source.display_name, source.mime_type, kind=request.get("kind"))
    document = extractor(request["path"], source, limits)
    emit("document", title=document.title, kind=document.kind, metadata=document.metadata)
    for unit in document.units:
        emit("unit", **asdict(unit))
    emit("complete", warnings=document.warnings)


def archive(request):
    from .archive_members import discover_archive_members, read_archive_member

    limits = ExtractionLimits(**request.get("limits", {}))
    if request["operation"] == "archive-list":
        scan = discover_archive_members(request["path"], limits)
        for member in scan.members:
            emit("member", **asdict(member))
        emit("complete", warnings=list(scan.warnings))
        return
    content = read_archive_member(
        request["path"],
        request["archive_format"],
        tuple(request["member_chain"]),
        limits,
        **request.get("options", {}),
    )
    artifact = Path(request["output"]) / "member.bin"
    artifact.write_bytes(content)
    emit("artifact", path=artifact.name, bytes=len(content))
    emit("complete", warnings=[])


def render(request):
    from .vision import render_image_page, render_pdf_pages

    options = request.get("options", {})
    if request.get("kind") == "image":
        images = render_image_page(request["path"], **options)
    else:
        images = render_pdf_pages(request["path"], request["pages"], **options)
    for item in images:
        path = Path(request["output"]) / f"page-{item['page']}.jpg"
        path.write_bytes(base64.b64decode(item.pop("base64")))
        emit("artifact", path=path.name, **item)
    emit("complete", warnings=[])


def main():
    sys.addaudithook(deny_network)
    request = json.load(sys.stdin)
    output = Path(request["output"]).resolve(strict=True)
    tempfile.tempdir = str(output)
    os.environ["TMPDIR"] = str(output)
    operation = request["operation"]
    try:
        if operation == "extract":
            extract(request)
        elif operation in {"archive-list", "archive-member"}:
            archive(request)
        elif operation == "render":
            render(request)
        else:
            raise ValueError("Unknown parser operation")
    except (ExtractionError, ValueError, OSError, RuntimeError) as exc:
        emit("error", reason=getattr(exc, "reason", "parser_failure"), message=str(exc)[:2000])
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
