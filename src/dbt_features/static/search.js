// Tiny client-side search. Substring matching over name/description/tags.
// Good enough for low thousands of features. Swap for lunr if it becomes
// painful.
(function () {
  const input = document.getElementById("search-input");
  const results = document.getElementById("search-results");
  if (!input || !results) return;

  let index = [];
  let activeIdx = -1;

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

  function render(items) {
    if (!items.length) {
      results.hidden = true;
      results.innerHTML = "";
      return;
    }
    results.hidden = false;
    results.innerHTML = items
      .map(
        (item, i) => `
        <li data-idx="${i}" data-url="${item.url}">
          <span class="search-kind">${item.kind}</span>
          <span class="search-name">${escapeHtml(item.name)}</span>
          ${item.kind === "feature" ? `<span class="search-context">in ${escapeHtml(item.group)}</span>` : ""}
          ${item.description ? `<div class="muted small">${escapeHtml(truncate(item.description, 120))}</div>` : ""}
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
    return s.length > n ? s.slice(0, n - 1) + "…" : s;
  }

  function search(q) {
    q = q.trim().toLowerCase();
    if (!q) {
      render([]);
      return;
    }
    const scored = index
      .map((item) => ({ item, s: score(item, q) }))
      .filter((x) => x.s > 0)
      .sort((a, b) => b.s - a.s)
      .slice(0, 20)
      .map((x) => x.item);
    render(scored);
  }

  function navigateTo(li) {
    if (!li) return;
    const url = li.getAttribute("data-url");
    if (url) window.location.href = url;
  }

  function setActive(idx) {
    const lis = results.querySelectorAll("li");
    lis.forEach((li) => li.classList.remove("active"));
    if (idx >= 0 && idx < lis.length) {
      lis[idx].classList.add("active");
      activeIdx = idx;
    } else {
      activeIdx = -1;
    }
  }

  input.addEventListener("input", (e) => search(e.target.value));
  input.addEventListener("keydown", (e) => {
    const lis = results.querySelectorAll("li");
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
      render([]);
    }
  });
  results.addEventListener("click", (e) => {
    const li = e.target.closest("li");
    if (li) navigateTo(li);
  });
  document.addEventListener("click", (e) => {
    if (!results.contains(e.target) && e.target !== input) {
      results.hidden = true;
    }
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
