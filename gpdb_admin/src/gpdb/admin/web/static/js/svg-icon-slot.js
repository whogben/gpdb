/**
 * Single place to normalize user SVG icons for fixed-size UI slots and Cytoscape.
 * - Strips root width/height/x/y so CSS controls size.
 * - Infers viewBox when missing.
 * - Tightens viewBox to painted bounds (getBBox) so loose artboards center correctly.
 */
(function () {
  "use strict";

  function parseSvgLength(s) {
    if (!s) return NaN;
    var t = String(s).trim().toLowerCase();
    if (t.endsWith("%")) return NaN;
    t = t.replace(/(px|pt|em|rem|ex|ch|cm|mm|in|pc)$/i, "");
    var n = parseFloat(t);
    return isNaN(n) ? NaN : n;
  }

  function normalizeRootSvgElement(svg) {
    if (!svg || svg.tagName.toLowerCase() !== "svg") return;
    svg.removeAttribute("x");
    svg.removeAttribute("y");
    if (!svg.getAttribute("viewBox")) {
      var w = parseSvgLength(svg.getAttribute("width"));
      var h = parseSvgLength(svg.getAttribute("height"));
      if (w > 0 && h > 0) {
        svg.setAttribute("viewBox", "0 0 " + w + " " + h);
      }
    }
    svg.removeAttribute("width");
    svg.removeAttribute("height");
    if (!svg.getAttribute("xmlns")) {
      svg.setAttribute("xmlns", "http://www.w3.org/2000/svg");
    }
    if (!svg.getAttribute("preserveAspectRatio")) {
      svg.setAttribute("preserveAspectRatio", "xMidYMid meet");
    }
  }

  function isValidBBox(b) {
    return (
      b &&
      isFinite(b.width) &&
      isFinite(b.height) &&
      isFinite(b.x) &&
      isFinite(b.y) &&
      b.width > 0 &&
      b.height > 0
    );
  }

  /**
   * Tighten viewBox to union of painted geometry (fixes huge empty margins).
   */
  function applyPaintedBoundsViewBox(svg) {
    if (!svg || svg.tagName.toLowerCase() !== "svg") return;
    try {
      var b = svg.getBBox();
      if (!isValidBBox(b)) return;
      var pad = 0.02 * Math.max(b.width, b.height);
      svg.setAttribute(
        "viewBox",
        b.x -
          pad +
          " " +
          (b.y - pad) +
          " " +
          (b.width + 2 * pad) +
          " " +
          (b.height + 2 * pad)
      );
      svg.setAttribute("preserveAspectRatio", "xMidYMid meet");
    } catch (e) {
      /* ignore */
    }
  }

  var _sandboxEl = null;

  function getMeasureSandbox() {
    if (_sandboxEl && _sandboxEl.parentNode) return _sandboxEl;
    var el = document.createElement("div");
    el.setAttribute("aria-hidden", "true");
    el.style.cssText =
      "position:fixed;left:-9999px;top:0;width:4096px;height:4096px;visibility:hidden;pointer-events:none;overflow:hidden";
    document.documentElement.appendChild(el);
    _sandboxEl = el;
    return el;
  }

  /**
   * Parse markup, measure in an off-screen tree, return serialized SVG string.
   */
  function fitSvgMarkupToContent(markup) {
    var raw = (markup || "").replace(/^\n+/, "").trim();
    if (!raw) return raw;
    var doc = new DOMParser().parseFromString(raw, "image/svg+xml");
    if (doc.querySelector("parsererror")) return raw;
    var svg = doc.documentElement;
    if (!svg || svg.tagName.toLowerCase() !== "svg") return raw;
    normalizeRootSvgElement(svg);
    var imported;
    try {
      imported = document.importNode(svg, true);
    } catch (e) {
      return raw;
    }
    var box = getMeasureSandbox();
    box.appendChild(imported);
    try {
      applyPaintedBoundsViewBox(imported);
      var vb = imported.getAttribute("viewBox");
      if (vb) svg.setAttribute("viewBox", vb);
      svg.setAttribute(
        "preserveAspectRatio",
        imported.getAttribute("preserveAspectRatio") || "xMidYMid meet"
      );
    } finally {
      box.removeChild(imported);
    }
    normalizeRootSvgElement(svg);
    try {
      return new XMLSerializer().serializeToString(svg);
    } catch (e2) {
      return svg.outerHTML || raw;
    }
  }

  function upgradeSlot(host) {
    if (!host) return;
    var svg = host.querySelector("svg");
    if (!svg) return;
    normalizeRootSvgElement(svg);
    applyPaintedBoundsViewBox(svg);
  }

  function upgradeAll(root) {
    var scope = root || document;
    var nodes = scope.querySelectorAll("[data-svg-icon-slot]");
    for (var i = 0; i < nodes.length; i++) {
      upgradeSlot(nodes[i]);
    }
  }

  window.gpdbSvgIconSlot = {
    normalizeRootSvgElement: normalizeRootSvgElement,
    fitSvgMarkupToContent: fitSvgMarkupToContent,
    upgrade: upgradeSlot,
    upgradeAll: upgradeAll,
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () {
      upgradeAll(document);
    });
  } else {
    upgradeAll(document);
  }
})();
