/**
 * Graph viewer integration: loads graph data from /viewer/data and renders with Cytoscape.
 * Expects global Cytoscape (loaded from cytoscape.min.js).
 * Container: #viewer-cy, status: #viewer-status, filter form: #viewer-filter-form (data-graph-id).
 */
(function () {
  "use strict";

  var DEBOUNCE_MS = 350;
  var debounceTimer = null;

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

  function initCytoscape(container) {
    if (typeof window.cytoscape !== "function") return null;
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
            "background-color": "#666",
          },
        },
        {
          selector: "edge",
          style: {
            "curve-style": "bezier",
            "target-arrow-color": "#ccc",
            "target-arrow-shape": "triangle",
            width: 1,
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

    function refresh() {
      fetchData(function (data) {
        if (data && data.elements) applyToCytoscape(cy, data);
      });
    }

    function debouncedRefresh() {
      if (debounceTimer) clearTimeout(debounceTimer);
      debounceTimer = setTimeout(refresh, DEBOUNCE_MS);
    }

    document.querySelectorAll("#viewer-filter-form input, #viewer-filter-form select").forEach(function (input) {
      input.addEventListener("change", debouncedRefresh);
      input.addEventListener("input", debouncedRefresh);
    });

    refresh();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", run);
  } else {
    run();
  }
})();
