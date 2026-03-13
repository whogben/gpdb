/**
 * JSONJoy Editor Wrapper
 * 
 * Provides a simple global API for mounting JSONJoy Builder editors
 * in non-React applications. Mirrors the pattern used by jedison-form.js.
 */
(function () {
    "use strict";

    const instances = new Map();

    function formatJson(value) {
        try {
            return JSON.stringify(value, null, 2);
        } catch (e) {
            return String(value);
        }
    }

    function parseJson(text) {
        try {
            return JSON.parse(text || '{}');
        } catch (e) {
            return { type: 'object' };
        }
    }

    /**
     * Global API for JSONJoy Builder integration
     */
    window.JsonJoyEditor = {
        /**
         * Mount a JSONJoy editor on a DOM element
         * @param {string} elementId - The ID of the container element
         * @param {object} schema - JSON Schema object to edit
         * @param {object} options - Configuration options
         * @param {boolean} options.readOnly - Whether the editor is read-only
         * @param {Function} options.onChange - Callback when schema changes
         * @param {HTMLElement} options.textarea - Optional textarea/input to sync with
         */
        mount(elementId, schema, options) {
            const container = document.getElementById(elementId);
            if (!container) {
                console.error(`JsonJoyEditor: element #${elementId} not found`);
                return;
            }

            // Clean up existing instance
            this.destroy(elementId);

            // Check dependencies
            if (!window.React || !window.ReactDOM || !window.JsonJoyBuilder) {
                console.error("JsonJoyEditor: Required dependencies not loaded (React, ReactDOM, or JsonJoyBuilder)");
                container.innerHTML = '<p class="form-error">Editor failed to load. Please refresh the page.</p>';
                return;
            }

            const React = window.React;
            const ReactDOM = window.ReactDOM;
            const JsonJoyBuilder = window.JsonJoyBuilder;

            // Create React root
            const root = ReactDOM.createRoot(container);

            // Track current schema value
            let currentSchema = schema;

            // Create the editor component
            const EditorComponent = JsonJoyBuilder.JsonSchemaEditor || JsonJoyBuilder.SchemaEditor || JsonJoyBuilder.default?.JsonSchemaEditor || JsonJoyBuilder.default?.SchemaEditor;

            if (!EditorComponent) {
                console.error("JsonJoyEditor: SchemaEditor component not found");
                console.error("JsonJoyEditor: Available exports:", Object.keys(JsonJoyBuilder || {}));
                container.innerHTML = '<p class="form-error">Editor component not available.</p>';
                return;
            }

            const element = React.createElement(EditorComponent, {
                schema: schema,
                readOnly: options.readOnly || false,
                onChange: options.readOnly ? undefined : function (newSchema) {
                    currentSchema = newSchema;
                    if (typeof options.onChange === 'function') {
                        options.onChange(newSchema);
                    }
                    if (options.textarea) {
                        options.textarea.value = formatJson(newSchema);
                    }
                },
            });

            root.render(element);

            // Store instance
            instances.set(elementId, {
                root: root,
                getValue: function () { return currentSchema; },
                container: container,
            });
        },

        /**
         * Get the current schema value from an editor
         * @param {string} elementId - The ID of the mounted editor
         * @returns {object|null} The current schema or null if not found
         */
        getValue(elementId) {
            const instance = instances.get(elementId);
            return instance ? instance.getValue() : null;
        },

        /**
         * Destroy an editor instance and clean up React
         * @param {string} elementId - The ID of the mounted editor
         */
        destroy(elementId) {
            const instance = instances.get(elementId);
            if (instance) {
                try {
                    instance.root.unmount();
                } catch (e) {
                    console.warn(`JsonJoyEditor: error unmounting #${elementId}:`, e);
                }
                instances.delete(elementId);
            }
        },

        /**
         * Check if an editor is mounted
         * @param {string} elementId - The ID to check
         * @returns {boolean}
         */
        isMounted(elementId) {
            return instances.has(elementId);
        },
    };
})();
