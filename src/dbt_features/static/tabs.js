// Tiny tabs implementation. ARIA-correct, no dependencies.
(function () {
  document.querySelectorAll("[data-tabs]").forEach(function (root) {
    var tabs = Array.from(root.querySelectorAll("[role='tab']"));
    var panels = Array.from(root.querySelectorAll("[role='tabpanel']"));

    function activate(name) {
      tabs.forEach(function (t) {
        var on = t.getAttribute("data-tab") === name;
        t.classList.toggle("is-active", on);
        t.setAttribute("aria-selected", on ? "true" : "false");
        t.tabIndex = on ? 0 : -1;
      });
      panels.forEach(function (p) {
        var on = p.getAttribute("data-panel") === name;
        p.classList.toggle("is-active", on);
        p.hidden = !on;
      });
    }

    tabs.forEach(function (t, i) {
      t.addEventListener("click", function () {
        activate(t.getAttribute("data-tab"));
      });
      t.addEventListener("keydown", function (e) {
        if (e.key === "ArrowRight" || e.key === "ArrowLeft") {
          e.preventDefault();
          var dir = e.key === "ArrowRight" ? 1 : -1;
          var next = tabs[(i + dir + tabs.length) % tabs.length];
          next.focus();
          activate(next.getAttribute("data-tab"));
        }
      });
    });
  });
})();
