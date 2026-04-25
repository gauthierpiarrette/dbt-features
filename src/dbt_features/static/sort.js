// Feature table interactivity: sorting and type/lifecycle filter chips.
// No dependencies.

// --- Filter chips ---
(function () {
  var bar = document.getElementById("feature-filters");
  var table = document.getElementById("features-table");
  if (!bar || !table) return;

  var tbody = table.querySelector("tbody");
  if (!tbody) return;
  var rows = Array.from(tbody.querySelectorAll("tr[data-feature-type]"));

  var types = {};
  rows.forEach(function (r) {
    var t = r.getAttribute("data-feature-type");
    types[t] = (types[t] || 0) + 1;
  });

  var activeFilter = null;

  Object.keys(types)
    .sort()
    .forEach(function (t) {
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className = "filter-chip pill pill-" + t;
      btn.textContent = t + " (" + types[t] + ")";
      btn.setAttribute("data-filter", t);
      btn.addEventListener("click", function () {
        if (activeFilter === t) {
          activeFilter = null;
          bar.querySelectorAll(".filter-chip").forEach(function (b) {
            b.classList.remove("filter-active");
          });
          rows.forEach(function (r) {
            r.style.display = "";
          });
        } else {
          activeFilter = t;
          bar.querySelectorAll(".filter-chip").forEach(function (b) {
            b.classList.toggle(
              "filter-active",
              b.getAttribute("data-filter") === t
            );
          });
          rows.forEach(function (r) {
            r.style.display =
              r.getAttribute("data-feature-type") === t ? "" : "none";
          });
        }
      });
      bar.appendChild(btn);
    });
})();

// --- Sortable columns ---
(function () {
  document.querySelectorAll(".features-table").forEach(function (table) {
    var headers = table.querySelectorAll("thead th");
    var tbody = table.querySelector("tbody");
    if (!tbody) return;

    var currentCol = -1;
    var ascending = true;

    headers.forEach(function (th, colIdx) {
      th.style.cursor = "pointer";
      th.setAttribute("aria-sort", "none");
      var arrow = document.createElement("span");
      arrow.className = "sort-arrow";
      arrow.textContent = "";
      th.appendChild(arrow);

      th.addEventListener("click", function () {
        if (currentCol === colIdx) {
          ascending = !ascending;
        } else {
          currentCol = colIdx;
          ascending = true;
        }

        headers.forEach(function (h, i) {
          var a = h.querySelector(".sort-arrow");
          if (i === colIdx) {
            a.textContent = ascending ? " \u25B2" : " \u25BC";
            h.setAttribute("aria-sort", ascending ? "ascending" : "descending");
          } else {
            a.textContent = "";
            h.setAttribute("aria-sort", "none");
          }
        });

        var isNumeric = th.classList.contains("numeric");
        var rows = Array.from(tbody.querySelectorAll("tr"));

        rows.sort(function (a, b) {
          var aCell = a.children[colIdx];
          var bCell = b.children[colIdx];
          if (!aCell || !bCell) return 0;
          var aVal = cellValue(aCell, isNumeric);
          var bVal = cellValue(bCell, isNumeric);
          var cmp;
          if (isNumeric) {
            cmp = aVal - bVal;
          } else {
            cmp = String(aVal).localeCompare(String(bVal), undefined, {
              sensitivity: "base",
            });
          }
          return ascending ? cmp : -cmp;
        });

        rows.forEach(function (r) {
          tbody.appendChild(r);
        });
      });
    });
  });

  function cellValue(td, isNumeric) {
    if (isNumeric) {
      var text = td.textContent.trim().replace(/,/g, "");
      if (text === "\u2014" || text === "-" || text === "") return -Infinity;
      // Handle humanized suffixes: 1.2k, 3.4M, etc.
      var match = text.match(/^([\d.]+)\s*([kKmMbB]?)%?$/);
      if (match) {
        var n = parseFloat(match[1]);
        var suffix = match[2].toLowerCase();
        if (suffix === "k") n *= 1000;
        else if (suffix === "m") n *= 1000000;
        else if (suffix === "b") n *= 1000000000;
        return n;
      }
      var parsed = parseFloat(text);
      return isNaN(parsed) ? -Infinity : parsed;
    }
    // For pill/badge columns, prefer the pill text; fall back to full cell text
    var pill = td.querySelector(".pill, .lifecycle");
    return (pill ? pill.textContent : td.textContent).trim().toLowerCase();
  }
})();
