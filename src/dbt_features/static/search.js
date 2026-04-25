// Tiny client-side search. Substring matching over name/description/tags.
// Good enough for low thousands of features. Swap for lunr if it becomes
// painful.
(function () {
  const input = document.getElementById("search-input");
  const results = document.getElementById("search-results");
  if (!input || !results) return;

  let index = [];
  let activeIdx = -1;
  let lastQuery = "";

  function score(item, q) {
    const haystack = [
      item.name,
      item.description || "",
      (item.tags || []).join(" "),
      item.group || "",
      item.owner || "",
    ]
      .join(" ")
      .toLowerCase();
    if (!haystack.includes(q)) return 0;
    let s = 1;
    if (item.name.toLowerCase().startsWith(q)) s += 5;
    else if (item.name.toLowerCase().includes(q)) s += 3;
    if ((item.description || "").toLowerCase().includes(q)) s += 1;
    if (item.kind === "feature") s += 0.5;
    return s;
  }

  function highlightMatch(text, q) {
    if (!q || !text) return escapeHtml(text || "");
    var escaped = escapeHtml(text);
    var idx = text.toLowerCase().indexOf(q);
    if (idx === -1) return escaped;
    var before = escapeHtml(text.slice(0, idx));
    var match = escapeHtml(text.slice(idx, idx + q.length));
    var after = escapeHtml(text.slice(idx + q.length));
    return before + "<mark>" + match + "</mark>" + after;
  }

  function render(items, q) {
    if (!items.length && !q) {
      results.hidden = true;
      input.setAttribute("aria-expanded", "false");
      results.innerHTML = "";
      return;
    }
    results.hidden = false;
    input.setAttribute("aria-expanded", "true");
    if (!items.length) {
      results.innerHTML =
        '<li class="search-empty">No results for \u201c' +
        escapeHtml(q) +
        '\u201d</li>';
      activeIdx = -1;
      return;
    }
    results.innerHTML = items
      .map(
        (item, i) => `
        <li data-idx="${i}" data-url="${item.url}" role="option">
          <span class="search-kind">${item.kind}</span>
          <span class="search-name">${highlightMatch(item.name, q)}</span>
          ${item.kind === "feature" ? `<span class="search-context">in ${escapeHtml(item.group)}</span>` : ""}
          ${item.description ? `<div class="muted small">${highlightMatch(truncate(item.description, 120), q)}</div>` : ""}
        </li>`
      )
      .join("");
    activeIdx = -1;
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function truncate(s, n) {
    return s.length > n ? s.slice(0, n - 1) + "\u2026" : s;
  }

  function parseQuery(raw) {
    var q = raw.trim().toLowerCase();
    var typeFilter = null;
    var match = q.match(/^type:(\S+)\s*/);
    if (match) {
      typeFilter = match[1];
      q = q.slice(match[0].length);
    }
    return { q: q, typeFilter: typeFilter };
  }

  function search(raw) {
    var parsed = parseQuery(raw);
    lastQuery = parsed.q;
    if (!parsed.q && !parsed.typeFilter) {
      render([], "");
      return;
    }
    var scored = index
      .map(function (item) {
        if (parsed.typeFilter) {
          var ft = (item.feature_type || "").toLowerCase();
          if (ft !== parsed.typeFilter) return { item: item, s: 0 };
        }
        if (!parsed.q) return { item: item, s: 1 };
        return { item: item, s: score(item, parsed.q) };
      })
      .filter(function (x) { return x.s > 0; })
      .sort(function (a, b) { return b.s - a.s; })
      .slice(0, 20)
      .map(function (x) { return x.item; });
    render(scored, parsed.q);
  }

  function navigateTo(li) {
    if (!li) return;
    const url = li.getAttribute("data-url");
    if (url) window.location.href = (window.SEARCH_BASE_URL || ".") + url;
  }

  function setActive(idx) {
    const lis = results.querySelectorAll("li[data-url]");
    lis.forEach((li) => li.classList.remove("active"));
    if (idx >= 0 && idx < lis.length) {
      lis[idx].classList.add("active");
      lis[idx].setAttribute("aria-selected", "true");
      activeIdx = idx;
    } else {
      activeIdx = -1;
    }
  }

  input.addEventListener("input", function (e) { search(e.target.value); });
  input.addEventListener("keydown", (e) => {
    const lis = results.querySelectorAll("li[data-url]");
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActive(Math.min(activeIdx + 1, lis.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActive(Math.max(activeIdx - 1, 0));
    } else if (e.key === "Enter") {
      e.preventDefault();
      if (activeIdx >= 0) navigateTo(lis[activeIdx]);
      else if (lis.length) navigateTo(lis[0]);
    } else if (e.key === "Escape") {
      input.value = "";
      render([], "");
      input.blur();
      input.setAttribute("aria-expanded", "false");
    }
  });
  results.addEventListener("click", (e) => {
    const li = e.target.closest("li[data-url]");
    if (li) navigateTo(li);
  });
  document.addEventListener("click", (e) => {
    if (!results.contains(e.target) && e.target !== input) {
      results.hidden = true;
      input.setAttribute("aria-expanded", "false");
    }
  });

  // "/" shortcut to focus search (standard in GitHub, Grafana, Datadog)
  document.addEventListener("keydown", (e) => {
    if (e.key !== "/") return;
    var tag = (e.target.tagName || "").toLowerCase();
    if (tag === "input" || tag === "textarea" || tag === "select") return;
    if (e.target.isContentEditable) return;
    e.preventDefault();
    input.focus();
  });

  fetch(window.SEARCH_INDEX_URL)
    .then((r) => r.json())
    .then((data) => {
      index = data;
    })
    .catch(() => {
      // Fail silently — search is non-critical.
    });
})();
