(function () {
  var mediaQuery = window.matchMedia("(prefers-color-scheme: dark)");
  var root = document.documentElement;

  function applyTheme(isDark) {
    root.dataset.theme = isDark ? "dark" : "light";
  }

  applyTheme(mediaQuery.matches);

  if (typeof mediaQuery.addEventListener === "function") {
    mediaQuery.addEventListener("change", function (event) {
      applyTheme(event.matches);
    });
  } else if (typeof mediaQuery.addListener === "function") {
    mediaQuery.addListener(function (event) {
      applyTheme(event.matches);
    });
  }
})();
