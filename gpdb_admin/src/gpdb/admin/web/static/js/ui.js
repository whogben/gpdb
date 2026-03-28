(function () {
    "use strict";

    var STORAGE_KEY = "gpdb_selected_graph";

    var navToggle, navMenu;
    var navOverlay;
    var infoStrap, infoStrapContent, infoStrapClose;
    var graphSelect, graphSettingsLink;
    var NAV_MENU_STATE_KEY = "gpdb_nav_menu_open";
    var NAV_PUSH_BREAKPOINT = 801;

    function saveNavMenuState(isOpen) {
        try {
            localStorage.setItem(NAV_MENU_STATE_KEY, isOpen ? "open" : "closed");
        } catch (e) {
        }
    }

    function loadNavMenuState() {
        try {
            var state = localStorage.getItem(NAV_MENU_STATE_KEY);
            return state === "open";
        } catch (e) {
            return false;
        }
    }

    // Apply the correct classes for the 4 states:
    //   Wide + Open:    nav-menu--open nav-menu--push   (sticky, visible, squishes content)
    //   Wide + Closed:  (no extra classes)               (fixed, hidden off-screen)
    //   Narrow + Open:  nav-menu--open nav-menu--overlay (fixed, visible, overlay with scrim)
    //   Narrow + Closed: (no extra classes)               (fixed, hidden off-screen)
    function applyNavState(isOpen, skipTransition) {
        if (skipTransition) {
            navMenu.style.transition = "none";
        }

        // Reset all mode classes
        navMenu.classList.remove("nav-menu--open", "nav-menu--push", "nav-menu--overlay");
        if (navOverlay) {
            navOverlay.classList.remove("nav-overlay--open");
        }

        if (isOpen) {
            var isWideScreen = window.innerWidth >= NAV_PUSH_BREAKPOINT;
            navMenu.classList.add("nav-menu--open");
            if (isWideScreen) {
                navMenu.classList.add("nav-menu--push");
            } else {
                navMenu.classList.add("nav-menu--overlay");
                if (navOverlay) {
                    navOverlay.classList.add("nav-overlay--open");
                }
            }
        }

        saveNavMenuState(isOpen);

        if (skipTransition) {
            navMenu.offsetHeight; // force reflow
            navMenu.style.transition = "";
        }
    }

    function toggleNavMenu(show, skipTransition) {
        if (show === undefined) {
            show = !navMenu.classList.contains("nav-menu--open");
        }
        applyNavState(show, skipTransition);
    }

    function closeAllMenus() {
        applyNavState(false, false);
    }

    function restoreNavMenuState() {
        var isOpen = loadNavMenuState();
        var isWideScreen = window.innerWidth >= NAV_PUSH_BREAKPOINT;
        // Only restore open state on wide screens; narrow always starts closed
        if (isOpen && isWideScreen) {
            applyNavState(true, true);
        }
    }

    function showInfoStrap(message, type) {
        infoStrapContent.textContent = decodeHtmlEntities(message);
        infoStrap.className = "info-strap";
        if (type) {
            infoStrap.classList.add("info-strap--" + type);
        }
        infoStrap.hidden = false;
    }

    function hideInfoStrap() {
        infoStrap.hidden = true;
    }

    function saveSelectedGraph(graphId) {
        try {
            localStorage.setItem(STORAGE_KEY, graphId);
        } catch (e) {
        }
    }

    function loadSelectedGraph() {
        try {
            return localStorage.getItem(STORAGE_KEY);
        } catch (e) {
            return null;
        }
    }

    function updateGraphSettingsLink(graphId) {
        if (!graphSettingsLink || !graphId) return;
        var href = graphSettingsLink.getAttribute("href") || "";
        // Replace any existing /graphs/{id} segment or append if missing.
        if (href.indexOf("/graphs/") !== -1) {
            href = href.replace(/\/graphs\/[^\/]+/, "/graphs/" + graphId);
        } else {
            href = "/graphs/" + graphId;
        }
        graphSettingsLink.setAttribute("href", href);
    }

    function updateNavLinks(graphId) {
        if (!graphId) return;
        var navLinks = document.querySelectorAll('[data-nav-link]');
        navLinks.forEach(function (link) {
            var href = link.getAttribute("href") || "";
            if (href.indexOf("/graphs/") !== -1) {
                href = href.replace(/\/graphs\/[^\/]+/, "/graphs/" + graphId);
                link.setAttribute("href", href);
            }
        });
    }

    function initGraphSelector() {
        if (!graphSelect) return;

        var savedGraphId = loadSelectedGraph();
        var effectiveGraphId = null;

        if (savedGraphId) {
            var option = graphSelect.querySelector('option[value="' + savedGraphId + '"]');
            if (option) {
                graphSelect.value = savedGraphId;
                effectiveGraphId = savedGraphId;
            }
        }

        if (!effectiveGraphId) {
            // Fall back to whatever the server marked as selected (or first option).
            var selectedOption = graphSelect.options[graphSelect.selectedIndex];
            if (selectedOption && selectedOption.value) {
                effectiveGraphId = selectedOption.value;
            } else if (graphSelect.options.length > 0) {
                effectiveGraphId = graphSelect.options[0].value;
                graphSelect.value = effectiveGraphId;
            }
        }

        if (effectiveGraphId) {
            saveSelectedGraph(effectiveGraphId);
            updateGraphSettingsLink(effectiveGraphId);
            updateNavLinks(effectiveGraphId);
        }

        graphSelect.addEventListener("change", function () {
            var graphId = graphSelect.value;
            if (!graphId) {
                return;
            }
            saveSelectedGraph(graphId);
            updateGraphSettingsLink(graphId);
            updateNavLinks(graphId);

            if (graphId) {
                var currentPath = window.location.pathname;
                var newPath = currentPath.replace(/\/graphs\/[^\/]+/, "/graphs/" + graphId);
                if (newPath !== currentPath) {
                    window.location.href = newPath;
                }
            }
        });
    }

    function initEventListeners() {
        if (navToggle) {
            navToggle.addEventListener("click", function () {
                toggleNavMenu();
            });
        }

        if (navOverlay) {
            navOverlay.addEventListener("click", function () {
                applyNavState(false, false);
            });
        }

        if (infoStrapClose) {
            infoStrapClose.addEventListener("click", function () {
                hideInfoStrap();
            });
        }

        document.addEventListener("click", function (e) {
            var navLink = e.target.closest(".nav-link");
            if (navLink) {
                // On wide screens, keep nav menu open; on narrow screens, close it
                if (window.innerWidth < NAV_PUSH_BREAKPOINT) {
                    closeAllMenus();
                }
            }
        });

        document.addEventListener("keydown", function (e) {
            if (e.key === "Escape") {
                closeAllMenus();
            }
        });

        // Re-apply nav state when crossing the push/overlay breakpoint
        var mql = window.matchMedia("(min-width: " + NAV_PUSH_BREAKPOINT + "px)");
        mql.addEventListener("change", function () {
            var isOpen = navMenu.classList.contains("nav-menu--open");
            applyNavState(isOpen, false);
        });

        // Highlight active navigation link (exact or path-prefix match)
        var currentPath = window.location.pathname;
        var navLinks = document.querySelectorAll('[data-nav-link]');
        navLinks.forEach(function (link) {
            var linkPath = link.getAttribute("href");
            if (linkPath && (currentPath === linkPath || currentPath.indexOf(linkPath + "/") === 0)) {
                link.classList.add("nav-link--active");
            }
        });

        // Swipe-to-open nav on touch devices
        if ("ontouchstart" in window || navigator.maxTouchPoints > 0) {
            var touchStartX = null;
            document.addEventListener("touchstart", function (e) {
                touchStartX = e.touches[0].clientX;
            }, { passive: true });
            document.addEventListener("touchmove", function (e) {
                if (touchStartX === null) return;
                var deltaX = e.touches[0].clientX - touchStartX;
                if (touchStartX < 30 && deltaX > 50) {
                    touchStartX = null;
                    toggleNavMenu(true);
                }
            }, { passive: true });
            document.addEventListener("touchend", function () {
                touchStartX = null;
            }, { passive: true });
        }
    }

    function decodeHtmlEntities(value) {
        if (value == null || value === "") {
            return "";
        }
        var div = document.createElement("div");
        div.innerHTML = value;
        return div.textContent || div.innerText || "";
    }

    // --- Filter bar toggle ---

    var filterToggle, filterBar;

    function countActiveFilters() {
        if (!filterBar) return 0;
        var count = 0;
        var inputs = filterBar.querySelectorAll("input, select");
        for (var i = 0; i < inputs.length; i++) {
            var el = inputs[i];
            if (el.type === "hidden") continue;
            if (el.name === "sort" || el.name === "limit" || el.name === "node_limit" || el.name === "edge_limit") continue;
            if (el.type === "checkbox") {
                if (el.checked) count++;
            } else if (el.value && el.value.trim() !== "") {
                count++;
            }
        }
        return count;
    }

    function updateFilterToggleText() {
        if (!filterToggle) return;
        var n = countActiveFilters();
        filterToggle.textContent = n > 0 ? "⚙ (" + n + ")" : "⚙";
    }

    function initFilterBar() {
        filterToggle = document.querySelector("[data-filter-toggle]");
        filterBar = document.querySelector("[data-filter-bar]");
        if (!filterToggle || !filterBar) return;

        filterToggle.addEventListener("click", function () {
            if (filterBar.hasAttribute("hidden")) {
                filterBar.removeAttribute("hidden");
            } else {
                filterBar.setAttribute("hidden", "hidden");
            }
        });

        // Update count on input changes
        filterBar.addEventListener("input", updateFilterToggleText);
        filterBar.addEventListener("change", updateFilterToggleText);

        updateFilterToggleText();
    }

    // --- Data editor toggle (JSON / Editor) ---

    function initDataEditorToggle() {
        var root = document.querySelector("[data-data-editor]");
        if (!root) return;

        var buttons = root.querySelectorAll("[data-data-toggle]");
        var panels = root.querySelectorAll("[data-data-panel]");

        buttons.forEach(function (btn) {
            btn.addEventListener("click", function () {
                var target = btn.getAttribute("data-data-toggle");

                buttons.forEach(function (b) {
                    b.classList.remove("data-editor-toggle__btn--active");
                });
                btn.classList.add("data-editor-toggle__btn--active");

                panels.forEach(function (p) {
                    if (p.getAttribute("data-data-panel") === target) {
                        p.classList.add("data-panel--active");
                    } else {
                        p.classList.remove("data-panel--active");
                    }
                });
            });
        });
    }

    function init() {
        navToggle = document.querySelector("[data-nav-toggle]");
        navMenu = document.querySelector("[data-nav-menu]");
        navOverlay = document.querySelector("[data-nav-overlay]");
        infoStrap = document.querySelector("[data-info-strap]");
        infoStrapContent = document.querySelector("[data-info-strap-content]");
        infoStrapClose = document.querySelector("[data-info-strap-close]");
        graphSelect = document.querySelector("[data-graph-select]");
        graphSettingsLink = document.querySelector("[data-graph-settings-link]");

        initEventListeners();
        initGraphSelector();
        initFilterBar();
        initDataEditorToggle();
        restoreNavMenuState();
    }

    // --- Click-to-copy ID utility ---

    function copyToClipboard(text) {
        if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(text).then(function () {
                showInfoStrap("Copied!", "success");
            });
        } else {
            // Fallback for insecure contexts
            var ta = document.createElement("textarea");
            ta.value = text;
            ta.style.position = "fixed";
            ta.style.opacity = "0";
            document.body.appendChild(ta);
            ta.select();
            try {
                document.execCommand("copy");
                showInfoStrap("Copied!", "success");
            } catch (e) {
                // silent
            }
            document.body.removeChild(ta);
        }
    }

    function initCopyId() {
        document.addEventListener("click", function (e) {
            var el = e.target.closest("[data-copy-id]");
            if (!el) return;
            e.preventDefault();
            var text = el.getAttribute("data-copy-id") || el.textContent;
            copyToClipboard(text);
        });
    }

    window.showInfoStrap = showInfoStrap;
    window.hideInfoStrap = hideInfoStrap;

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", function () {
            init();
            initCopyId();
        });
    } else {
        init();
        initCopyId();
    }
})();
