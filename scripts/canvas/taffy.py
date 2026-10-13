#!/usr/bin/env python3
"""Prepare pinned published bytes/tools, then trim and remove callback Style natively."""

import argparse
import json
import shutil

from workspace import RECIPES, REPO, Workspace, digest, fingerprint, verify_hashes, verify_products

INPUTS = RECIPES / "taffy"
VENDOR = REPO / "frontend/vendor/taffy"
POLICY = {
    "maximum_workspace_bytes": 1073741824,
    "minimum_initial_free_bytes": 2147483648,
    "minimum_remaining_free_bytes": 1073741824,
}


def tools(work):
    inventory = json.loads((INPUTS / "tool-hashes.json").read_text())
    verify_hashes(work.root / "node_modules", inventory)


def prepare(work, allow_downloads):
    if not allow_downloads:
        raise RuntimeError("Inspect package-lock.json and explicitly pass --allow-downloads")
    for name in ("package.json", "package-lock.json"):
        shutil.copyfile(INPUTS / name, work.root / name)
    work.run(["npm", "ci", "--ignore-scripts", "--no-audit", "--no-fund", "--maxsockets=1"], work.root)
    tools(work)
    (work.root / "prepared.json").write_text(json.dumps({"recipe": fingerprint()}, indent=2) + "\n")


def generate(work):
    prepared = json.loads((work.root / "prepared.json").read_text())
    if prepared["recipe"] != fingerprint():
        raise RuntimeError("Prepared recipe changed; use a new disposable workspace")
    tools(work)
    modules = work.root / "node_modules"
    work.run(
        [
            "node",
            INPUTS / "generate.mjs",
            "--input",
            modules / "taffy-layout",
            "--output",
            work.root / "dist",
            "--typescript",
            modules / "typescript",
            "--binaryen",
            modules / "binaryen",
        ],
        work.root,
        deadline=900,
    )
    dist = work.root / "dist"
    manifest = json.loads((VENDOR / "build-manifest.json").read_text())
    verify_products(VENDOR, manifest["packagedFiles"], exact=True)
    diagnostics = work.root / "diagnostics"
    diagnostics.mkdir()
    for name in manifest["buildOnlyOutputsNotPackaged"]:
        (dist / name).rename(diagnostics / name)
    verify_products(dist, manifest["packagedFiles"], exact=True)
    manifest["permanentRecipeFingerprint"] = fingerprint()
    manifest["recipe"]["newRecipeExecution"] = (
        "Pinned trimming, native four-argument measure ABI and exact output checks completed."
    )
    manifest["proof"] = {
        "generation": "Pinned native measure-body, ABI/export and exact output hash checks completed.",
        "runtimeDifferential": "Not run by this recipe",
        "coldStartup": "Unmeasured",
    }
    manifest["packagedFiles"] = {
        str(p.relative_to(dist)): {"bytes": p.stat().st_size, "sha256": digest(p)}
        for p in sorted(dist.rglob("*"))
        if p.is_file()
    }
    (dist / "build-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    work.guard()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("prepare", "generate", "clean"))
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--allow-downloads", action="store_true")
    args = parser.parse_args()
    if args.stage == "prepare" and not args.allow_downloads:
        parser.error("prepare requires explicit --allow-downloads")
    work = Workspace(args.workspace, "taffy", POLICY, create=args.stage == "prepare", cleaning=args.stage == "clean")
    if args.stage == "clean":
        work.clean()
        return
    try:
        if args.stage == "prepare":
            prepare(work, args.allow_downloads)
        else:
            generate(work)
    except BaseException:
        work.clean()
        raise
    else:
        work.prune_caches()


if __name__ == "__main__":
    main()
