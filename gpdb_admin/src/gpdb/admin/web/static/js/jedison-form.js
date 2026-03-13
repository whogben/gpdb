(function () {
  "use strict";

  function parseJson(text) {
    return JSON.parse(text);
  }

  function formatJson(value) {
    return JSON.stringify(value, null, 2);
  }

  function setStatus(statusEl, message, state) {
    if (!statusEl) {
      return;
    }
    statusEl.textContent = message;
    if (state) {
      statusEl.dataset.state = state;
      return;
    }
    delete statusEl.dataset.state;
  }

  function clearMount(mountEl) {
    if (typeof mountEl.replaceChildren === "function") {
      mountEl.replaceChildren();
      return;
    }
    mountEl.innerHTML = "";
  }

  function createEditor(mountEl, schema, data) {
    if (!window.Jedison || !window.Jedison.Create || !window.Jedison.Theme) {
      throw new Error("Jedison runtime is unavailable.");
    }
    return new window.Jedison.Create({
      container: mountEl,
      theme: new window.Jedison.Theme(),
      schema: schema,
      data: data,
      showErrors: "change",
    });
  }

  function initFormEditor(rootEl) {
    var schemaScript = rootEl.querySelector("[data-jedison-schema-map]");
    var mountEl = rootEl.querySelector("[data-jedison-mount]");
    var statusEl = rootEl.querySelector("[data-jedison-status]");
    var refreshButton = rootEl.querySelector("[data-jedison-refresh]");
    var schemaSelect = document.getElementById(rootEl.dataset.schemaSelectId || "");
    var jsonField = document.getElementById(rootEl.dataset.jsonFieldId || "");
    var schemaMap = {};
    var editor = null;

    if (!schemaScript || !mountEl || !schemaSelect || !jsonField) {
      return;
    }

    try {
      schemaMap = parseJson(schemaScript.textContent || "{}");
    } catch (error) {
      setStatus(statusEl, "Schema editor configuration could not be loaded.", "error");
      return;
    }

    function destroyEditor() {
      if (editor && typeof editor.destroy === "function") {
        editor.destroy();
      }
      editor = null;
      clearMount(mountEl);
    }

    function syncTextareaFromEditor() {
      if (!editor || typeof editor.getValue !== "function") {
        return;
      }
      jsonField.value = formatJson(editor.getValue());
      setStatus(statusEl, "");
    }

    function mountEditorFromTextarea() {
      var schemaName = schemaSelect.value;
      var schema = schemaMap[schemaName];

      destroyEditor();

      if (!schemaName || !schema) {
        setStatus(statusEl, "");
        rootEl.style.display = "none";
        return;
      }

      rootEl.style.display = "";

      try {
        editor = createEditor(mountEl, schema, parseJson(jsonField.value || "{}"));
        if (typeof editor.on === "function") {
          editor.on("change", syncTextareaFromEditor);
        }
        syncTextareaFromEditor();
      } catch (error) {
        destroyEditor();
        setStatus(
          statusEl,
          "Could not render this schema from the current JSON.",
          "error"
        );
      }
    }

    schemaSelect.addEventListener("change", mountEditorFromTextarea);
    jsonField.addEventListener("blur", mountEditorFromTextarea);

    mountEditorFromTextarea();
  }

  function initReadonlyViewer(rootEl) {
    var schemaScript = rootEl.querySelector("[data-jedison-schema]");
    var dataScript = rootEl.querySelector("[data-jedison-data]");
    var mountEl = rootEl.querySelector("[data-jedison-mount]");
    var statusEl = rootEl.querySelector("[data-jedison-status]");
    var editor = null;

    if (!schemaScript || !dataScript || !mountEl) {
      return;
    }

    try {
      editor = createEditor(
        mountEl,
        parseJson(schemaScript.textContent || "{}"),
        parseJson(dataScript.textContent || "{}")
      );
      if (editor && typeof editor.disable === "function") {
        editor.disable();
      }
      setStatus(statusEl, "");
    } catch (error) {
      if (editor && typeof editor.destroy === "function") {
        editor.destroy();
      }
      clearMount(mountEl);
      setStatus(
        statusEl,
        "Could not render this schema view.",
        "error"
      );
    }
  }

  function initJedisonRoot(rootEl) {
    if ((rootEl.dataset.mode || "form") === "readonly") {
      initReadonlyViewer(rootEl);
      return;
    }
    initFormEditor(rootEl);
  }

  document.addEventListener("DOMContentLoaded", function () {
    var schemaEditors = document.querySelectorAll("[data-jedison-root]");
    schemaEditors.forEach(initJedisonRoot);
  });
})();
