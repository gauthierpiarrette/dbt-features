// Cmd-K-style search modal. Substring matching over name/description/tags
// with a few smart boosts. Good enough for low thousands of features —
// swap for lunr/fuse if it becomes painful.
(function () {
  var dialog = document.getElementById("search-dialog");
  var input = document.getElementById("search-input");
  var results = document.getElementById("search-results");
  var trigger = document.getElementById("search-trigger");
  if (!dialog || !input || !results) return;

  var index = [];
  var activeIdx = -1;
  var lastFocused = null;

  function score(item, q) {
    var haystack = [
      item.name,
      item.description || "",
      (item.tags || []).join(" "),
      item.group || "",
      item.owner || "",
    ]
      .join(" ")
      .toLowerCase();
    if (!haystack.includes(q)) return 0;
    var s = 1;
    var name = item.name.toLowerCase();
    if (name === q) s += 8;
    else if (name.startsWith(q)) s += 5;
    else if (name.includes(q)) s += 3;
    if ((item.description || "").toLowerCase().includes(q)) s += 1;
    if (item.kind === "feature") s += 0.5;
    if (item.kind === "model") s += 0.5;
    if (item.lifecycle === "deprecated") s -= 0.5;
    return s;
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function highlightMatch(text, q) {
    if (!q || !text) return escapeHtml(text || "");
    var idx = text.toLowerCase().indexOf(q);
    if (idx === -1) return escapeHtml(text);
    var before = escapeHtml(text.slice(0, idx));
    var match = escapeHtml(text.slice(idx, idx + q.length));
    var after = escapeHtml(text.slice(idx + q.length));
    return before + "<mark>" + match + "</mark>" + after;
  }

  function truncate(s, n) {
    return s.length > n ? s.slice(0, n - 1) + "\u2026" : s;
  }

  function badgeHtml(item) {
    var parts = [];
    if (item.lifecycle && item.lifecycle !== "active") {
      parts.push(
        '<span class="search-badge lifecycle lifecycle-' +
          escapeHtml(item.lifecycle) +
          '">' +
          escapeHtml(item.lifecycle) +
          "</span>"
      );
    }
    if (item.freshness) {
      parts.push(
        '<span class="search-badge"><span class="freshness-dot freshness-dot-' +
          escapeHtml(item.freshness) +
          '"></span>' +
          escapeHtml(item.freshness) +
          "</span>"
      );
    }
    if (item.feature_type) {
      parts.push(
        '<span class="search-badge pill pill-' +
          escapeHtml(item.feature_type) +
          '">' +
          escapeHtml(item.feature_type) +
          "</span>"
      );
    }
    return parts.join("");
  }

  function render(items, q) {
    if (!items.length && !q) {
      results.innerHTML =
        '<li class="search-empty">Start typing to search features, groups, and models.</li>';
      activeIdx = -1;
      return;
    }
    if (!items.length) {
      results.innerHTML =
        '<li class="search-empty">No results for \u201c' +
        escapeHtml(q) +
        '\u201d</li>';
      activeIdx = -1;
      return;
    }
    results.innerHTML = items
      .map(function (item, i) {
        var lifecycleClass =
          item.lifecycle === "deprecated" ? " is-deprecated" : "";
        return (
          '<li data-idx="' +
          i +
          '" data-url="' +
          escapeHtml(item.url) +
          '" role="option" class="' +
          lifecycleClass +
          '">' +
          '<div class="search-row-main">' +
          '<span class="search-kind">' +
          escapeHtml(item.kind) +
          "</span>" +
          '<span class="search-name">' +
          highlightMatch(item.name, q) +
          "</span>" +
          (item.kind === "feature"
            ? '<span class="search-context">in ' +
              escapeHtml(item.group) +
              "</span>"
            : "") +
          '<span class="search-badges">' +
          badgeHtml(item) +
          "</span>" +
          "</div>" +
          (item.description
            ? '<div class="muted small search-row-desc">' +
              highlightMatch(truncate(item.description, 140), q) +
              "</div>"
            : "") +
          "</li>"
        );
      })
      .join("");
    activeIdx = -1;
  }

  function parseQuery(raw) {
    var q = raw.trim().toLowerCase();
    var typeFilter = null;
    var kindFilter = null;
    var match = q.match(/^type:(\S+)\s*/);
    if (match) {
      typeFilter = match[1];
      q = q.slice(match[0].length);
    }
    match = q.match(/^kind:(\S+)\s*/);
    if (match) {
      kindFilter = match[1];
      q = q.slice(match[0].length);
    }
    return { q: q, typeFilter: typeFilter, kindFilter: kindFilter };
  }

  function search(raw) {
    var parsed = parseQuery(raw);
    if (!parsed.q && !parsed.typeFilter && !parsed.kindFilter) {
      render([], "");
      return;
    }
    var scored = index
      .map(function (item) {
        if (parsed.typeFilter) {
          var ft = (item.feature_type || "").toLowerCase();
          if (ft !== parsed.typeFilter) return { item: item, s: 0 };
        }
        if (parsed.kindFilter) {
          if ((item.kind || "").toLowerCase() !== parsed.kindFilter)
            return { item: item, s: 0 };
        }
        if (!parsed.q) return { item: item, s: 1 };
        return { item: item, s: score(item, parsed.q) };
      })
      .filter(function (x) {
        return x.s > 0;
      })
      .sort(function (a, b) {
        return b.s - a.s;
      })
      .slice(0, 25)
      .map(function (x) {
        return x.item;
      });
    render(scored, parsed.q);
  }

  function navigateTo(li) {
    if (!li) return;
    var url = li.getAttribute("data-url");
    if (url) window.location.href = (window.SEARCH_BASE_URL || ".") + url;
  }

  function setActive(idx) {
    var lis = results.querySelectorAll("li[data-url]");
    lis.forEach(function (li) {
      li.classList.remove("active");
    });
    if (idx >= 0 && idx < lis.length) {
      lis[idx].classList.add("active");
      lis[idx].setAttribute("aria-selected", "true");
      lis[idx].scrollIntoView({ block: "nearest" });
      activeIdx = idx;
    } else {
      activeIdx = -1;
    }
  }

  function openDialog() {
    if (!dialog.hidden) return;
    lastFocused = document.activeElement;
    dialog.hidden = false;
    document.body.classList.add("search-open");
    input.value = "";
    render([], "");
    setTimeout(function () {
      input.focus();
    }, 0);
  }

  function closeDialog() {
    if (dialog.hidden) return;
    dialog.hidden = true;
    document.body.classList.remove("search-open");
    if (lastFocused && lastFocused.focus) lastFocused.focus();
  }

  input.addEventListener("input", function (e) {
    search(e.target.value);
  });

  input.addEventListener("keydown", function (e) {
    var lis = results.querySelectorAll("li[data-url]");
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
      e.preventDefault();
      closeDialog();
    }
  });

  results.addEventListener("click", function (e) {
    var li = e.target.closest("li[data-url]");
    if (li) navigateTo(li);
  });

  dialog.addEventListener("click", function (e) {
    if (e.target.closest("[data-close]")) closeDialog();
  });

  if (trigger) trigger.addEventListener("click", openDialog);

  // Global shortcuts: "/" and Cmd/Ctrl-K open the dialog. Esc closes.
  document.addEventListener("keydown", function (e) {
    var meta = e.metaKey || e.ctrlKey;
    if (meta && (e.key === "k" || e.key === "K")) {
      e.preventDefault();
      openDialog();
      return;
    }
    if (e.key !== "/") return;
    var tag = (e.target.tagName || "").toLowerCase();
    if (tag === "input" || tag === "textarea" || tag === "select") return;
    if (e.target.isContentEditable) return;
    e.preventDefault();
    openDialog();
  });

  fetch(window.SEARCH_INDEX_URL)
    .then(function (r) {
      return r.json();
    })
    .then(function (data) {
      index = data;
    })
    .catch(function () {
      // Fail silently — search is non-critical.
    });
})();
