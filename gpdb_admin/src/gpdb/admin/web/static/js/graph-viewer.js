/**
 * Graph viewer integration: loads graph data from /viewer/data and renders with Cytoscape.
 * Expects global Cytoscape (loaded from cytoscape.min.js).
 * Container: #viewer-cy, status: #viewer-status, filter form: #viewer-filter-form (data-graph-id).
 */
(function () {
  "use strict";

  var DEBOUNCE_MS = 350;
  var debounceTimer = null;
  var POLL_INTERVAL_MS = 1000;
  var pollTimer = null;
  var lastDataHash = null;

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

  function applyToCytoscape(cy, data) {
    if (!cy || !data || !data.elements) return;
    cy.elements().remove();
    if (data.elements.length) cy.add(data.elements);
    cy.layout({ name: "cose", animate: true }).run();
  }

  function applyToCytoscapePreservingState(cy, data) {
    if (!cy || !data || !data.elements) return;

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

    var newNodes = cy.nodes().filter(function (node) {
      return !positionMap[node.id()];
    });
    if (newNodes.length > 0) {
      cy.layout({
        name: "cose",
        animate: true,
        randomize: false,
        fit: positions.length === 0
      }).run();
    }

    if (positions.length > 0) {
      cy.pan(pan);
      cy.zoom(zoom);
    }
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
            color: colors.textColor,
            "text-outline-color": colors.surfaceBg,
            "text-outline-width": 2,
          },
        },
        {
          selector: "node[type]",
          style: {
            label: function (ele) {
              var label = ele.data("label") || "";
              var type = ele.data("type") || "";
              if (type && label !== type) {
                return label + "\n(" + type + ")";
              }
              return label;
            },
            "text-valign": "center",
            "text-halign": "center",
            "text-wrap": "wrap",
            "text-max-width": "80px",
            "font-size": 10,
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
        if (data && data.elements) applyToCytoscapePreservingState(cy, data);
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
