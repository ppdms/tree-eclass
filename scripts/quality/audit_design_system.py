#!/usr/bin/env python3
"""Design-system compliance gate for the visual findings in the product UX audit.

Guards the fixes from docs/product-ux-fix-plan.md Phase 1:
  1-1  `.btn-primary` / `.btn-danger` exist and paint an explicit background,
       so no primary button ever falls through to transparent text.
  1-2  No `border-style: outset` (the UA button chrome) anywhere in source CSS.
  1-3  The reader toolbar selectors declare the unified radius and row height.
  1-4  No off-palette hex colours in the design-system or reader stylesheets
       (the audit-measured `#fff2a8`, `#d12f2f`, `#fb923c` offenders).

Usage: python3 scripts/quality/audit_design_system.py
Exit 0 = compliant, 1 = violations found.
"""

import re
import sys

ROOT = "frontend/src/styles"
DESIGN_SYSTEM = f"{ROOT}/core/controls.css"
CSS_FILES = (
    f"{ROOT}/core/accessibility.css",
    DESIGN_SYSTEM,
    "frontend/src/features/session/reader/reader.css",
    "frontend/src/features/session/reader/textlayer.css",
    f"{ROOT}/themes/eink.css",
)
READER_COMPONENTS = (
    "frontend/src/features/session/components/SessionToolbarChrome.tsx",
    "frontend/src/features/session/components/SessionToolbarButtons.tsx",
)

OFF_PALETTE_HEXES = (
    "#fff2a8",  # old note-surface (dark)
    "#fef3c7",  # old note-surface (light)
    "#362f1c",  # old note-text (dark)
    "#422006",  # old note-text (light)
    "#66520d",  # old note-muted (dark)
    "#713f12",  # old note-muted (light)
    "#8a321f",  # old note-danger (dark)
    "#991b1b",  # old note-danger (light)
    "#d12f2f",  # old bookmark-color (dark)
    "#fb923c",  # old lv-2 orange
    "#9a3412",  # old lv-2 light orange
)


def read(path: str) -> str:
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def rule_bodies(css: str, selector: str):
    """Return the bodies of every selector block, including grouped rules."""
    return re.findall(re.escape(selector) + r"(?:,\s*[^{]+)?\s*\{([^}]*)\}", css, re.DOTALL)


def has_declaration(body: str, name: str) -> bool:
    """True if body declares `name: …` (not merely references a --name var)."""
    return re.search(r"(?:^|;)\s*" + re.escape(name) + r"\s*:", body) is not None


def violations() -> list[str]:
    found = []
    design = read(DESIGN_SYSTEM)
    css_sources = [read(path) for path in CSS_FILES]
    all_css = "\n".join(css_sources)

    for name in ("btn-primary", "btn-danger"):
        body = next(iter(rule_bodies(design, f".{name}")), None)
        if body is None:
            found.append(f"MISSING .{name} rule in {DESIGN_SYSTEM}")
            continue
        if not has_declaration(body, "background"):
            found.append(f".{name} in {DESIGN_SYSTEM} sets no background")
        if not has_declaration(body, "color"):
            found.append(f".{name} in {DESIGN_SYSTEM} sets no color")

    for css_file, body in zip(CSS_FILES, css_sources):
        if re.search(r"border-style\s*:\s*outset", body):
            found.append(f"UA button chrome (border-style: outset) in {css_file}")

    components = "\n".join(read(path) for path in READER_COMPONENTS)
    for class_name in ("session-control-group", "session-fit-button", "session-action-toggle"):
        matches = re.findall(r'className="([^"]*\b' + class_name + r'\b[^"]*)"', components)
        if not matches:
            found.append(f"MISSING .{class_name} reader control")
        elif not any("h-10" in match and "rounded-sm" in match for match in matches):
            found.append(f".{class_name} reader control lacks shared size/radius classes")

    for hex_value in OFF_PALETTE_HEXES:
        if hex_value.lower() in all_css.lower():
            found.append(f"off-palette colour {hex_value} present in source CSS")
    return found


def main() -> int:
    issues = violations()
    if issues:
        print("scripts/quality/audit_design_system.py: violations found:")
        for issue in issues:
            print(f"  {issue}")
        return 1
    print("scripts/quality/audit_design_system.py: design system is compliant")
    return 0


if __name__ == "__main__":
    sys.exit(main())
