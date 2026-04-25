// Index page filter logic.
//
// Facets ↔ data attributes on each card:
//   data-entities  (comma-separated)
//   data-tags      (comma-separated)
//   data-types     (comma-separated, feature types within the group)
//   data-lifecycle (single)
//   data-owner     (single)
//   data-freshness (single)
//
// Filter semantics: AND across facets, OR within a facet. So
// "lifecycle=preview, type=numeric" means "preview AND has-a-numeric-feature".
// State is serialized to the URL querystring so users can paste filtered
// links into Slack.
(function () {
  var toolbar = document.getElementById("index-filters");
  var sectionsRoot = document.getElementById("entity-sections");
  var emptyState = document.getElementById("filter-empty");
  if (!toolbar || !sectionsRoot) return;

  var counter = document.querySelector("[data-card-counter]");
  var clearBtns = document.querySelectorAll("[data-clear-filters]");
  var totalShown = 0;
  var totalTotal = sectionsRoot.querySelectorAll("[data-card]").length;

  // active[facet] = Set of values
  var active = {};

  function readFromURL() {
    active = {};
    var sp = new URLSearchParams(window.location.search);
    sp.forEach(function (val, key) {
      val.split(",").forEach(function (v) {
        if (!v) return;
        if (!active[key]) active[key] = new Set();
        active[key].add(v);
      });
    });
  }

  function writeToURL() {
    var sp = new URLSearchParams();
    Object.keys(active).forEach(function (k) {
      var vals = Array.from(active[k]);
      if (vals.length) sp.set(k, vals.join(","));
    });
    var qs = sp.toString();
    var url = window.location.pathname + (qs ? "?" + qs : "") + window.location.hash;
    window.history.replaceState(null, "", url);
  }

  function syncChipState() {
    toolbar.querySelectorAll(".chip").forEach(function (chip) {
      var f = chip.getAttribute("data-facet");
      var v = chip.getAttribute("data-value");
      var on = active[f] && active[f].has(v);
      chip.classList.toggle("is-active", !!on);
      chip.setAttribute("aria-checked", on ? "true" : "false");
    });
  }

  function cardMatches(card) {
    return Object.keys(active).every(function (facet) {
      var wanted = active[facet];
      if (!wanted || wanted.size === 0) return true;
      var attr = card.getAttribute("data-" + (facet === "tag" ? "tags" :
                                              facet === "entity" ? "entities" :
                                              facet === "type" ? "types" : facet));
      if (attr === null) return false;
      var got = attr.split(",").filter(Boolean);
      return Array.from(wanted).some(function (v) { return got.indexOf(v) !== -1; });
    });
  }

  function applyFilters() {
    totalShown = 0;
    sectionsRoot.querySelectorAll(".entity-section").forEach(function (section) {
      var visibleInSection = 0;
      section.querySelectorAll("[data-card]").forEach(function (card) {
        var match = cardMatches(card);
        card.style.display = match ? "" : "none";
        if (match) visibleInSection++;
      });
      var sc = section.querySelector("[data-section-count]");
      if (sc) {
        sc.textContent = visibleInSection + " group" + (visibleInSection === 1 ? "" : "s");
      }
      section.style.display = visibleInSection === 0 ? "none" : "";
      totalShown += visibleInSection;
    });
    if (counter) {
      counter.textContent =
        totalShown === totalTotal
          ? totalTotal + " shown"
          : totalShown + " of " + totalTotal + " shown";
    }
    var hasActive = Object.values(active).some(function (s) { return s.size > 0; });
    clearBtns.forEach(function (b) { b.hidden = !hasActive; });
    if (emptyState) emptyState.hidden = totalShown !== 0;
  }

  toolbar.addEventListener("click", function (e) {
    var chip = e.target.closest(".chip");
    if (!chip) return;
    var f = chip.getAttribute("data-facet");
    var v = chip.getAttribute("data-value");
    if (!active[f]) active[f] = new Set();
    if (active[f].has(v)) active[f].delete(v);
    else active[f].add(v);
    if (active[f].size === 0) delete active[f];
    syncChipState();
    applyFilters();
    writeToURL();
  });

  clearBtns.forEach(function (b) {
    b.addEventListener("click", function () {
      active = {};
      syncChipState();
      applyFilters();
      writeToURL();
    });
  });

  readFromURL();
  syncChipState();
  applyFilters();
})();
