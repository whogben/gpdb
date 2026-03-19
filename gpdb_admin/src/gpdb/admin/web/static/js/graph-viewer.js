(function () {
  "use strict";

  var DEBOUNCE_MS = 350;
  var debounceTimer = null;
  var POLL_INTERVAL_MS = 1000;
  var pollTimer = null;
  var lastDataHash = null;
  var ICON_RASTER_CACHE = Object.create(null);
  var ICON_RASTER_MIN_PX = 64;
  var edgeIconRafByCy = new WeakMap();

  function scheduleSyncEdgeIcons(cy) {
    if (!cy) return;
    var pending = edgeIconRafByCy.get(cy);
    if (pending !== undefined) return;
    var raf = requestAnimationFrame(function () {
      edgeIconRafByCy.delete(cy);
      syncEdgeIconPositions(cy);
    });
    edgeIconRafByCy.set(cy, raf);
  }

  function syncEdgeIconPositions(cy) {
    if (!cy) return;
    cy.batch(function () {
      cy.nodes("[edgeIconHost]").forEach(function (ghost) {
        var hostId = ghost.data("edgeIconHost");
        if (!hostId) return;
        var edge = cy.getElementById(hostId);
        if (!edge || edge.empty() || !edge.isEdge()) return;
        ghost.position(edge.midpoint());
      });
    });
  }

  /** Cytoscape cannot draw background-image on edges; add midpoint ghost nodes. */
  function injectEdgeIconGhostNodes(data) {
    if (!data || !Array.isArray(data.elements)) return;
    var toAdd = [];
    for (var i = 0; i < data.elements.length; i++) {
      var el = data.elements[i];
      if (!el || el.group !== "edges" || !el.data) continue;
      var uri = el.data.iconUri;
      if (!uri || typeof uri !== "string") continue;
      toAdd.push({
        group: "nodes",
        data: {
          id: "__gpdb_edge_icon__" + el.data.id,
          edgeIconHost: el.data.id,
          iconUri: uri,
          label: "",
        },
      });
    }
    for (var j = 0; j < toAdd.length; j++) {
      data.elements.push(toAdd[j]);
    }
  }

  function getGraphId() {
    var el = document.getElementById("viewer-filter-form");
    return el && el.getAttribute("data-graph-id");
  }

  function getDataUrl() {
    var graphId = getGraphId();
    if (!graphId) return null;
    var base = document.querySelector('script[data-viewer-base]');
    var path = (base && base.getAttribute("data-viewer-base")) || "";
    return path + "/graphs/" + encodeURIComponent(graphId) + "/viewer/data";
  }

  function getFilterParams() {
    var params = {};
    var names = [
      "node_type", "node_schema_name", "node_parent_id", "node_filter", "node_limit",
      "edge_type", "edge_schema_name", "edge_source_id", "edge_target_id", "edge_filter", "edge_limit"
    ];
    for (var i = 0; i < names.length; i++) {
      var input = document.querySelector("#viewer-filter-form [name=\"" + names[i] + "\"]");
      if (input) {
        var val = input.value ? input.value.trim() : "";
        if (val || names[i] === "node_limit" || names[i] === "edge_limit") params[names[i]] = val || input.value;
      }
    }
    return params;
  }

  function setStatus(text, isError) {
    var el = document.getElementById("viewer-status");
    if (!el) return;
    el.textContent = text;
    el.className = "resource-note" + (isError ? " form-error" : "");
  }

  function buildQueryString(params) {
    var parts = [];
    for (var key in params) {
      if (params.hasOwnProperty(key) && params[key] !== "") {
        parts.push(encodeURIComponent(key) + "=" + encodeURIComponent(params[key]));
      }
    }
    return parts.length ? "?" + parts.join("&") : "";
  }

  function fetchData(callback) {
    var url = getDataUrl();
    if (!url) {
      setStatus("Missing graph id.", true);
      callback(null);
      return;
    }
    var params = getFilterParams();
    var fullUrl = url + buildQueryString(params);
    setStatus("Loading…");
    var req = new XMLHttpRequest();
    req.open("GET", fullUrl);
    req.setRequestHeader("Accept", "application/json");
    req.onload = function () {
      if (req.status !== 200) {
        try {
          var body = JSON.parse(req.responseText);
          setStatus(body.error || "Request failed", true);
        } catch (e) {
          setStatus("Request failed: " + req.status, true);
        }
        callback(null);
        return;
      }
      var responseText = req.responseText || "";
      var data = null;
      if (responseText.trim()) {
        try {
          data = JSON.parse(responseText);
        } catch (e) {
          setStatus("Could not load graph data", true);
          data = { elements: [], node_count: 0, edge_count: 0 };
        }
      }
      if (!data) {
        data = { elements: [], node_count: 0, edge_count: 0 };
      }
      if (!Array.isArray(data.elements)) {
        data = { elements: [], node_count: 0, edge_count: 0 };
      }
      setStatus(
        "Nodes: " + (data.node_count || 0) + ", Edges: " + (data.edge_count || 0)
      );
      callback(data);
    };
    req.onerror = function () {
      setStatus("Network error", true);
      callback(null);
    };
    req.send();
  }

  function applySchemaMetadata(cy, data) {
    if (!cy || !data) return;
    cy.batch(function () {
      cy.elements().forEach(function (ele) {
        var displayLabel = ele.data("display_label");
        if (
          ele.group() === "nodes" &&
          ele.data("iconUri") &&
          !ele.data("edgeIconHost")
        ) {
          ele.data("label", "");
        } else if (ele.group() === "edges" && ele.data("iconUri")) {
          ele.data("label", "");
        } else if (displayLabel) {
          ele.data("label", displayLabel);
        }
      });
    });
  }

  function applyToCytoscapePreservingState(cy, data) {
    if (!cy || !data || !data.elements) return;

    prepareCytoscapeViewerDataAsync(data).then(function () {
      var pan = cy.pan();
      var zoom = cy.zoom();
      var positions = cy.nodes().map(function (node) {
        return { id: node.id(), position: node.position() };
      });

      cy.elements().remove();
      if (data.elements.length) cy.add(data.elements);

      var positionMap = {};
      for (var i = 0; i < positions.length; i++) {
        positionMap[positions[i].id] = positions[i].position;
      }

      cy.nodes().forEach(function (node) {
        var id = node.id();
        if (positionMap[id]) {
          node.position(positionMap[id]);
        }
      });

      var newRegular = cy.nodes("[^edgeIconHost]").filter(function (node) {
        return !positionMap[node.id()];
      });
      if (newRegular.length > 0) {
        var layoutRun = cy.layout({
          name: "cose",
          animate: true,
          randomize: false,
          fit: positions.length === 0,
          eles: cy.nodes("[^edgeIconHost]").union(cy.edges()),
        });
        layoutRun.one("layoutstop", function () {
          applySchemaMetadata(cy, data);
          refreshCytoscapeStyles(cy);
          scheduleSyncEdgeIcons(cy);
        });
        layoutRun.run();
      }

      if (positions.length > 0) {
        cy.pan(pan);
        cy.zoom(zoom);
      }

      applySchemaMetadata(cy, data);
      refreshCytoscapeStyles(cy);
      scheduleSyncEdgeIcons(cy);
    });
  }

  function computeDataHash(data) {
    if (!data || !data.elements) return "";
    function elementSignature(el) {
      var d = el.data || {};
      var keys = Object.keys(d).sort();
      var parts = [];
      for (var k = 0; k < keys.length; k++) {
        parts.push(keys[k] + "=" + JSON.stringify(d[keys[k]]));
      }
      return (el.group === "edges" ? "e:" : "n:") + parts.join(",");
    }
    return data.elements.map(elementSignature).sort().join("|");
  }

  function hasDataChanged(data) {
    var newHash = computeDataHash(data);
    if (newHash !== lastDataHash) {
      lastDataHash = newHash;
      return true;
    }
    return false;
  }

  function getThemeColors() {
    var root = document.documentElement;
    var styles = getComputedStyle(root);
    return {
      textColor: styles.getPropertyValue("--text-color").trim() || "#000",
      mutedText: styles.getPropertyValue("--muted-text").trim() || "#666",
      borderColor: styles.getPropertyValue("--border-color").trim() || "#ccc",
      accentColor: styles.getPropertyValue("--accent-color").trim() || "#325dff",
      surfaceBg: styles.getPropertyValue("--surface-bg").trim() || "#fff",
    };
  }

  function recolorSvgDataUriForRaster(uri, themeTextColor) {
    if (!uri || typeof uri !== "string" || uri.indexOf("data:image/svg+xml") !== 0) {
      return uri;
    }
    var comma = uri.indexOf(",");
    if (comma < 0) return uri;
    var prefix = uri.slice(0, comma + 1);
    var payload = uri.slice(comma + 1);
    var decoded;
    try {
      decoded = decodeURIComponent(payload);
    } catch (e) {
      return uri;
    }
    if (decoded.indexOf("currentColor") === -1 && decoded.indexOf("currentcolor") === -1) {
      return uri;
    }
    var recolored = decoded.replace(/currentColor/gi, themeTextColor);
    return prefix + encodeURIComponent(recolored);
  }

  function applySvgIconThemeRecolor(data) {
    if (!data || !Array.isArray(data.elements)) return;
    var textColor = getThemeColors().textColor;
    for (var i = 0; i < data.elements.length; i++) {
      var el = data.elements[i];
      if (!el || !el.data) continue;
      if (el.group !== "nodes" && el.group !== "edges") continue;
      var uri = el.data.iconUri;
      if (uri && typeof uri === "string") {
        el.data.iconUri = recolorSvgDataUriForRaster(uri, textColor);
      }
    }
  }

  function tightenSvgDataUriForRaster(svgUri) {
    if (
      !svgUri ||
      typeof svgUri !== "string" ||
      svgUri.indexOf("data:image/svg+xml") !== 0
    ) {
      return svgUri;
    }
    var slot = window.gpdbSvgIconSlot;
    if (!slot || typeof slot.fitSvgMarkupToContent !== "function") {
      return svgUri;
    }
    var comma = svgUri.indexOf(",");
    if (comma < 0) return svgUri;
    var prefix = svgUri.slice(0, comma + 1);
    var payload = svgUri.slice(comma + 1);
    var decoded;
    try {
      decoded = decodeURIComponent(payload);
    } catch (e) {
      return svgUri;
    }
    var fitted = slot.fitSvgMarkupToContent(decoded);
    if (!fitted || fitted === decoded) {
      return svgUri;
    }
    return prefix + encodeURIComponent(fitted);
  }

  function rasterizeSvgDataUriToPng(svgUri, sizePx) {
    return new Promise(function (resolve) {
      if (
        !svgUri ||
        typeof svgUri !== "string" ||
        svgUri.indexOf("data:image/svg+xml") !== 0
      ) {
        resolve(svgUri);
        return;
      }
      var cached = ICON_RASTER_CACHE[svgUri];
      if (cached) {
        resolve(cached);
        return;
      }
      var drawUri = tightenSvgDataUriForRaster(svgUri);
      var img = new Image();
      function fallback() {
        ICON_RASTER_CACHE[svgUri] = svgUri;
        resolve(svgUri);
      }
      img.onload = function () {
        try {
          var canvas = document.createElement("canvas");
          canvas.width = sizePx;
          canvas.height = sizePx;
          var ctx = canvas.getContext("2d", { alpha: true });
          if (!ctx) {
            fallback();
            return;
          }
          ctx.clearRect(0, 0, sizePx, sizePx);
          ctx.drawImage(img, 0, 0, sizePx, sizePx);
          var png = canvas.toDataURL("image/png");
          if (png.indexOf("data:image/png") === 0) {
            ICON_RASTER_CACHE[svgUri] = png;
            resolve(png);
          } else {
            fallback();
          }
        } catch (e) {
          fallback();
        }
      };
      img.onerror = function () {
        fallback();
      };
      img.src = drawUri;
    });
  }

  function prepareCytoscapeViewerDataAsync(data) {
    if (!data || !Array.isArray(data.elements)) return Promise.resolve();
    applySvgIconThemeRecolor(data);
    var uniq = [];
    var seen = Object.create(null);
    for (var i = 0; i < data.elements.length; i++) {
      var el = data.elements[i];
      if (!el || !el.data || !el.data.iconUri) continue;
      if (el.group !== "nodes" && el.group !== "edges") continue;
      var u = el.data.iconUri;
      if (typeof u !== "string" || u.indexOf("data:image/svg+xml") !== 0) {
        continue;
      }
      if (!seen[u]) {
        seen[u] = true;
        uniq.push(u);
      }
    }
    var dpr = window.devicePixelRatio || 1;
    var sizePx = Math.max(ICON_RASTER_MIN_PX, Math.round(20 * Math.max(1, dpr)));
    function applyRasterMap(pngs) {
      var map = Object.create(null);
      for (var j = 0; j < uniq.length; j++) {
        map[uniq[j]] = pngs[j];
      }
      for (var k = 0; k < data.elements.length; k++) {
        var elt = data.elements[k];
        if (!elt || !elt.data || !elt.data.iconUri) continue;
        if (elt.group !== "nodes" && elt.group !== "edges") continue;
        var mapped = map[elt.data.iconUri];
        if (mapped) elt.data.iconUri = mapped;
      }
      injectEdgeIconGhostNodes(data);
    }
    if (uniq.length === 0) {
      injectEdgeIconGhostNodes(data);
      return Promise.resolve();
    }
    return Promise.all(
      uniq.map(function (u) {
        return rasterizeSvgDataUriToPng(u, sizePx);
      })
    ).then(function (pngs) {
      applyRasterMap(pngs);
    });
  }

  function refreshCytoscapeStyles(cy) {
    if (!cy) return;
    var sty = cy.style();
    if (sty && typeof sty.update === "function") {
      sty.update();
    }
  }

  function initCytoscape(container) {
    if (typeof window.cytoscape !== "function") return null;
    var colors = getThemeColors();
    return window.cytoscape({
      container: container,
      elements: [],
      style: [
        {
          selector: "node",
          style: {
            label: "data(label)",
            "text-valign": "center",
            "text-halign": "center",
            width: 20,
            height: 20,
            "background-color": colors.accentColor,
            "background-image": function (ele) {
              return ele.data("iconUri") || "none";
            },
            "background-fit": "contain",
            "background-repeat": "no-repeat",
            "background-clip": "node",
            "background-opacity": 1,
            "background-position-x": "50%",
            "background-position-y": "50%",
            "border-width": 0,
            "border-opacity": 0,
            color: colors.textColor,
            "text-outline-color": colors.surfaceBg,
            "text-outline-width": 2,
          },
        },
        {
          selector: "edge",
          style: {
            "curve-style": "bezier",
            "target-arrow-color": colors.borderColor,
            "target-arrow-shape": "triangle",
            width: 1,
            "line-color": colors.borderColor,
            label: "data(label)",
            color: colors.mutedText,
            "font-size": 10,
            "text-rotation": "autorotate",
            "text-margin-y": -10,
          },
        },
        {
          selector: "node[edgeIconHost]",
          style: {
            width: 16,
            height: 16,
            label: "",
            "z-index": 10,
            grabbable: false,
            selectable: false,
            /* Canvas renderer: "transparent" often paints black; opacity applies to body
               color only, not the background-image (see Cytoscape style docs). */
            "background-color": colors.surfaceBg,
            "background-opacity": 0,
            "background-image": function (ele) {
              return ele.data("iconUri") || "none";
            },
            "background-image-opacity": 1,
            "background-fit": "contain",
            "background-repeat": "no-repeat",
            "background-clip": "node",
            "border-width": 0,
            "border-opacity": 0,
            color: colors.textColor,
            "text-outline-width": 0,
          },
        },
      ],
      layout: { name: "cose", animate: true },
    });
  }

  function run() {
    var container = document.getElementById("viewer-cy");
    if (!container) return;

    var cy = initCytoscape(container);
    if (!cy) {
      setStatus("Cytoscape not loaded.", true);
      return;
    }

    cy.on("position", "node", function (evt) {
      if (evt.target.data("edgeIconHost")) return;
      scheduleSyncEdgeIcons(cy);
    });

    var viewerContainer = document.getElementById("viewer-container");
    var expandBtn = document.getElementById("viewer-expand-btn");
    var liveToggle = document.getElementById("viewer-live-toggle");
    var isExpanded = false;
    var isLive = false;

    function toggleExpand() {
      isExpanded = !isExpanded;

      if (isExpanded) {
        viewerContainer.classList.add("expanded");
        expandBtn.setAttribute("aria-label", "Collapse graph view");
        expandBtn.setAttribute("title", "Collapse graph view");
        expandBtn.innerHTML = '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="4 14 10 14 10 20"></polyline><polyline points="20 10 14 10 14 4"></polyline><line x1="14" y1="10" x2="21" y2="3"></line><line x1="3" y1="21" x2="10" y2="14"></line></svg>';
      } else {
        viewerContainer.classList.remove("expanded");
        expandBtn.setAttribute("aria-label", "Expand graph view");
        expandBtn.setAttribute("title", "Expand graph view");
        expandBtn.innerHTML = '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="15 3 21 3 21 9"></polyline><polyline points="9 21 3 21 3 15"></polyline><line x1="21" y1="3" x2="14" y2="10"></line><line x1="3" y1="21" x2="10" y2="14"></line></svg>';
      }

      setTimeout(function () {
        cy.fit(undefined, 50);
      }, 300);
    }

    if (expandBtn) {
      expandBtn.addEventListener("click", toggleExpand);
    }

    function refresh() {
      fetchData(function (data) {
        if (data && data.elements) {
          lastDataHash = computeDataHash(data);
          applyToCytoscapePreservingState(cy, data);
        }
      });
    }

    function debouncedRefresh() {
      if (debounceTimer) clearTimeout(debounceTimer);
      debounceTimer = setTimeout(refresh, DEBOUNCE_MS);
    }

    function liveRefresh() {
      fetchData(function (data) {
        if (data && data.elements && hasDataChanged(data)) {
          applyToCytoscapePreservingState(cy, data);
        }
      });
    }

    function startPolling() {
      if (pollTimer) clearInterval(pollTimer);
      pollTimer = setInterval(liveRefresh, POLL_INTERVAL_MS);
    }

    function stopPolling() {
      if (pollTimer) {
        clearInterval(pollTimer);
        pollTimer = null;
      }
    }

    function toggleLive() {
      isLive = liveToggle.checked;
      if (isLive) {
        startPolling();
      } else {
        stopPolling();
      }
    }

    if (liveToggle) {
      liveToggle.addEventListener("change", toggleLive);
    }

    document.querySelectorAll("#viewer-filter-form input, #viewer-filter-form select").forEach(function (input) {
      input.addEventListener("change", debouncedRefresh);
      input.addEventListener("input", debouncedRefresh);
    });

    window.addEventListener("beforeunload", function () {
      stopPolling();
    });

    document.addEventListener("visibilitychange", function () {
      if (document.hidden) {
        stopPolling();
      } else if (isLive) {
        startPolling();
      }
    });

    refresh();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", run);
  } else {
    run();
  }
})();
