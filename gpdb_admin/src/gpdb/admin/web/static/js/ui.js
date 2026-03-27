(function () {
    "use strict";

    var STORAGE_KEY = "gpdb_selected_graph";

    var navToggle, navMenu;
    var navOverlay;
    var infoStrap, infoStrapContent, infoStrapClose;
    var graphSelect, graphSettingsLink;

    function toggleNavMenu(show) {
        if (show === undefined) {
            show = !navMenu.classList.contains("nav-menu--open");
        }
        if (show) {
            navMenu.classList.add("nav-menu--open");
            if (navOverlay) {
                navOverlay.classList.add("nav-overlay--open");
            }
        } else {
            navMenu.classList.remove("nav-menu--open");
            if (navOverlay) {
                navOverlay.classList.remove("nav-overlay--open");
            }
        }
    }

    function closeAllMenus() {
        navMenu.classList.remove("nav-menu--open");
        if (navOverlay) {
            navOverlay.classList.remove("nav-overlay--open");
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
                navMenu.classList.remove("nav-menu--open");
                if (navOverlay) {
                    navOverlay.classList.remove("nav-overlay--open");
                }
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
                closeAllMenus();
            }
        });

        document.addEventListener("keydown", function (e) {
            if (e.key === "Escape") {
                closeAllMenus();
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
    }

    window.showInfoStrap = showInfoStrap;
    window.hideInfoStrap = hideInfoStrap;

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", init);
    } else {
        init();
    }
})();
