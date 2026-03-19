"""
SVG sanitization utilities for safely handling user-provided SVG content.
"""

import xml.etree.ElementTree as ET
from typing import Final
from urllib.parse import quote

import bleach

# Default SVG namespace so ET.tostring() emits <svg xmlns="..."> not <ns0:svg>.
_SVG_NS = "http://www.w3.org/2000/svg"
ET.register_namespace("", _SVG_NS)

# Allowed SVG elements - safe for rendering without script execution
_ALLOWED_ELEMENTS: Final = {
    "svg",
    "path",
    "circle",
    "rect",
    "ellipse",
    "line",
    "polyline",
    "polygon",
    "text",
    "g",
    "defs",
    "use",
    "symbol",
    "marker",
    "clipPath",
    "mask",
    "pattern",
    "gradient",
    "stop",
    "linearGradient",
    "radialGradient",
}

# Allowed SVG attributes - safe presentation and structural attributes
_ALLOWED_ATTRIBUTES: Final = {
    "fill",
    "stroke",
    "stroke-width",
    "stroke-linecap",
    "stroke-linejoin",
    "stroke-dasharray",
    "stroke-dashoffset",
    "fill-opacity",
    "stroke-opacity",
    "opacity",
    "transform",
    "d",
    "cx",
    "cy",
    "r",
    "rx",
    "ry",
    "x",
    "y",
    "width",
    "height",
    "points",
    "x1",
    "y1",
    "x2",
    "y2",
    "text-anchor",
    "font-family",
    "font-size",
    "font-weight",
    "font-style",
    "text-decoration",
    "letter-spacing",
    "word-spacing",
    "writing-mode",
    "direction",
    "dominant-baseline",
    "alignment-baseline",
    "baseline-shift",
    "id",
    "class",
    "style",
    "href",
    "xmlns",
    "viewBox",
    "preserveAspectRatio",
}


def _local_tag(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1].lower()
    return tag.lower()


def _parse_svg_length(raw: str | None) -> float | None:
    if raw is None:
        return None
    s = raw.strip().lower()
    if not s or s.endswith("%"):
        return None
    for suffix in ("px", "pt", "em", "rem", "ex", "ch", "cm", "mm", "in", "pc"):
        if s.endswith(suffix):
            s = s[: -len(suffix)].strip()
            break
    try:
        return float(s)
    except ValueError:
        return None


def _inject_intrinsic_size_from_viewbox(root: ET.Element) -> None:
    """Set root width/height from viewBox so Cytoscape's Image() gets intrinsic dimensions."""
    if _local_tag(root.tag) != "svg":
        return
    if root.get("width") or root.get("height"):
        return
    vb = root.get("viewBox") or root.get("viewbox")
    if not vb:
        return
    parts = [p for p in vb.replace(",", " ").split() if p]
    if len(parts) != 4:
        return
    try:
        vw = float(parts[2])
        vh = float(parts[3])
    except ValueError:
        return
    if vw > 0 and vh > 0:
        root.set("width", str(vw))
        root.set("height", str(vh))


def svg_markup_to_cytoscape_data_uri(svg: str | None) -> str | None:
    """
    Build a percent-encoded UTF-8 data URI for Cytoscape node background-image.

    Uses ``data:image/svg+xml;charset=utf-8,`` + percent-encoding (RFC 2397). We emit
    only the ``<svg>...</svg>`` fragment — no XML declaration or ``<!DOCTYPE svg>``:
    WebKit/Safari rejects many data-URL SVGs that include an incomplete doctype, and
    ``;utf8,`` without ``charset=`` is non-standard and also fails in Safari.

    Normalizes the root and adds width/height from viewBox when missing so Image() gets
    intrinsic dimensions.

    Fragments like ``<svg viewBox="...">`` (no ``xmlns``) are common from icon editors;
    ElementTree serializes them without a namespace, which is invalid for SVG-as-image in
    WebKit — we always set ``xmlns`` on the root before encoding.
    """
    if svg is None:
        return None
    stripped = svg.strip()
    if not stripped:
        return None
    media = "data:image/svg+xml;charset=utf-8,"
    try:
        root = ET.fromstring(stripped)
    except ET.ParseError:
        return media + quote(stripped, safe="")
    if _local_tag(root.tag) != "svg":
        return media + quote(stripped, safe="")
    _normalize_svg_root_for_icon_slot(root)
    _inject_intrinsic_size_from_viewbox(root)
    _ensure_svg_xmlns_for_standalone_image(root)
    out = ET.tostring(root, encoding="unicode")
    return media + quote(out, safe="")


def normalize_svg_icon_for_display(svg: str | None) -> str | None:
    """
    Re-apply root normalization for SVG already stored (legacy rows) or round-tripped XML.

    Does not run bleach; use sanitize_svg for untrusted input.
    """
    if svg is None:
        return None
    stripped = svg.strip()
    if not stripped:
        return None
    try:
        root = ET.fromstring(stripped)
    except ET.ParseError:
        return svg
    _normalize_svg_root_for_icon_slot(root)
    return ET.tostring(root, encoding="unicode")


def _strip_root_svg_xy(root: ET.Element) -> None:
    """Remove root x/y so the viewport is not shifted inside fixed UI slots."""
    to_del = [k for k in root.attrib if _local_tag(k) in ("x", "y")]
    for k in to_del:
        del root.attrib[k]


def _ensure_svg_xmlns_for_standalone_image(root: ET.Element) -> None:
    """WebKit/Safari rejects SVG loaded via Image()/img unless the root declares the SVG namespace."""
    if _local_tag(root.tag) != "svg":
        return
    if root.tag.startswith("{http://www.w3.org/2000/svg}"):
        return
    root.set("xmlns", _SVG_NS)


def _normalize_svg_root_for_icon_slot(root: ET.Element) -> None:
    """Make root <svg> scale inside fixed CSS boxes (admin list/detail, form preview)."""
    if _local_tag(root.tag) != "svg":
        return
    _strip_root_svg_xy(root)
    has_view = root.get("viewBox") or root.get("viewbox")
    if not has_view:
        w = _parse_svg_length(root.get("width"))
        h = _parse_svg_length(root.get("height"))
        if w is not None and h is not None and w > 0 and h > 0:
            root.set("viewBox", f"0 0 {w:g} {h:g}")
    if root.get("preserveAspectRatio") is None:
        root.set("preserveAspectRatio", "xMidYMid meet")
    for attr in list(root.attrib):
        if _local_tag(attr) in ("width", "height"):
            del root.attrib[attr]


def sanitize_svg(svg: str | None, max_size_kb: int = 20) -> str:
    """
    Sanitize an SVG string to remove potentially dangerous content.

    This function removes script tags, event handlers, and dangerous attributes
    while preserving safe SVG elements and presentation attributes. It also
    validates that the SVG is well-formed XML and within size limits.

    Args:
        svg: The SVG string to sanitize. Must not be None.
        max_size_kb: Maximum allowed size in kilobytes (default: 20).

    Returns:
        The sanitized SVG string.

    Raises:
        ValueError: If svg is None, exceeds size limit, is malformed XML,
            or sanitization fails.
    """
    if svg is None:
        raise ValueError("SVG content cannot be None")

    # Check size limit
    max_bytes = max_size_kb * 1024
    if len(svg.encode("utf-8")) > max_bytes:
        raise ValueError(f"SVG exceeds maximum size of {max_size_kb}KB")

    # Sanitize using bleach - removes scripts and dangerous attributes
    sanitized = bleach.clean(
        svg,
        tags=list(_ALLOWED_ELEMENTS),
        attributes={tag: list(_ALLOWED_ATTRIBUTES) for tag in _ALLOWED_ELEMENTS},
        strip=True,
    )

    # Validate, normalize for embedding in fixed-size UI slots, re-serialize
    try:
        root = ET.fromstring(sanitized)
    except ET.ParseError as e:
        raise ValueError(f"Sanitized SVG is not valid XML: {e}")

    _normalize_svg_root_for_icon_slot(root)

    return ET.tostring(root, encoding="unicode")
