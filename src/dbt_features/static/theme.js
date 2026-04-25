// Two-state theme toggle: light ↔ dark.
//
// Stored preference is "light" or "dark" in localStorage under
// "dbt-features-theme". The pre-paint init in <head> already applies
// the correct attribute before the stylesheet loads — this module
// wires up the click handler.
(function () {
  var STORAGE_KEY = "dbt-features-theme";
  var btn = document.getElementById("theme-toggle");
  if (!btn) return;

  function getPref() {
    try {
      return localStorage.getItem(STORAGE_KEY) || "dark";
    } catch (e) {
      return "dark";
    }
  }

  function apply(pref) {
    if (pref === "light") {
      document.documentElement.setAttribute("data-theme", "light");
    } else {
      document.documentElement.removeAttribute("data-theme");
    }
    try {
      localStorage.setItem(STORAGE_KEY, pref);
    } catch (e) {}
    btn.setAttribute("aria-pressed", pref === "dark" ? "true" : "false");
  }

  apply(getPref());

  btn.addEventListener("click", function () {
    apply(getPref() === "dark" ? "light" : "dark");
  });
})();
