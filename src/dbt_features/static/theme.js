// Theme toggle. Dark is the default; light is an opt-in override.
//
// The pre-paint init in <head> reads localStorage and sets data-theme="light"
// on <html> if the user previously chose light, before the stylesheet
// applies — so light-mode users don't see a flash of dark theme.
(function () {
  var btn = document.getElementById("theme-toggle");
  if (!btn) return;

  function currentEffectiveTheme() {
    return document.documentElement.getAttribute("data-theme") === "light"
      ? "light"
      : "dark";
  }

  function setTheme(theme) {
    if (theme === "light") {
      document.documentElement.setAttribute("data-theme", "light");
    } else {
      document.documentElement.removeAttribute("data-theme");
    }
    try {
      localStorage.setItem("dbt-features-theme", theme);
    } catch (e) {
      // Private mode / blocked storage — fine, just won't persist.
    }
    btn.setAttribute("aria-pressed", theme === "dark" ? "true" : "false");
  }

  btn.setAttribute(
    "aria-pressed",
    currentEffectiveTheme() === "dark" ? "true" : "false"
  );

  btn.addEventListener("click", function () {
    setTheme(currentEffectiveTheme() === "dark" ? "light" : "dark");
  });
})();
