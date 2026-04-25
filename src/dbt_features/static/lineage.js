// Lineage page: render the Mermaid graph using the page's font and
// theme, then layer interactivity on top — search to focus a node, dim
// everything outside its 1-hop neighborhood, click to navigate.
(function () {
  if (!window.mermaid) return;

  function bodyFont() {
    try {
      return getComputedStyle(document.body).fontFamily;
    } catch (e) {
      return '-apple-system, BlinkMacSystemFont, "Segoe UI", "Inter", Roboto, Helvetica, Arial, sans-serif';
    }
  }

  function themeVars() {
    var dark = document.documentElement.getAttribute("data-theme") !== "light";
    // Palette mirrors style.css. Keep these in sync if the CSS variables move.
    return dark
      ? {
          primaryColor: "#1f3653",
          primaryTextColor: "#e6edf3",
          primaryBorderColor: "#2f81f7",
          lineColor: "#8b949e",
          mainBkg: "#1f3653",
          background: "transparent",
          edgeLabelBackground: "#161b22",
          clusterBkg: "#161b22",
          clusterBorder: "#30363d",
          fontFamily: bodyFont(),
          fontSize: "14px",
        }
      : {
          primaryColor: "#ddf4ff",
          primaryTextColor: "#1f2328",
          primaryBorderColor: "#0969da",
          lineColor: "#6a737d",
          mainBkg: "#ddf4ff",
          background: "transparent",
          edgeLabelBackground: "#f7f8fa",
          clusterBkg: "#f7f8fa",
          clusterBorder: "#e1e4e8",
          fontFamily: bodyFont(),
          fontSize: "14px",
        };
  }

  function render() {
    var host = document.getElementById("mermaid-host");
    var tmpl = document.getElementById("mermaid-source");
    if (!host || !tmpl) return;
    host.innerHTML = "";
    host.appendChild(tmpl.content.cloneNode(true));
    window.mermaid.initialize({
      startOnLoad: false,
      securityLevel: "loose",
      theme: "base",
      themeVariables: themeVars(),
      flowchart: {
        curve: "basis",
        padding: 18,
        nodeSpacing: 40,
        rankSpacing: 60,
        htmlLabels: true,
      },
    });
    return window.mermaid
      .run({ nodes: host.querySelectorAll(".mermaid") })
      .then(annotateNodes);
  }

  function annotateNodes() {
    // Add a "clickable" cue to every node that has a click handler.
    document.querySelectorAll("#mermaid-host .node").forEach(function (n) {
      n.classList.add("clickable");
    });
  }

  function nodeMatchesQuery(node, q) {
    var label = (node.textContent || "").trim().toLowerCase();
    return label.indexOf(q) !== -1;
  }

  function focusNode(node) {
    var host = document.getElementById("mermaid-host");
    if (!host) return;
    var nodeId = node.id;
    host.classList.add("is-focused");
    var keep = new Set([nodeId]);
    // Walk edges to collect 1-hop neighbors.
    host.querySelectorAll(".edgePath").forEach(function (e) {
      var path = e.id;
      // Mermaid edge ids look like "L-<from>-<to>-N"
      var m = path.match(/^L-(.+?)-(.+?)(?:-\d+)?$/);
      if (!m) return;
      if (m[1] === nodeId) {
        keep.add(m[2]);
        e.classList.add("is-on-path");
      } else if (m[2] === nodeId) {
        keep.add(m[1]);
        e.classList.add("is-on-path");
      } else {
        e.classList.remove("is-on-path");
      }
    });
    host.querySelectorAll(".node").forEach(function (n) {
      n.classList.toggle("is-dim", !keep.has(n.id));
      n.classList.toggle("is-focused-node", n.id === nodeId);
    });
  }

  function clearFocus() {
    var host = document.getElementById("mermaid-host");
    if (!host) return;
    host.classList.remove("is-focused");
    host.querySelectorAll(".node").forEach(function (n) {
      n.classList.remove("is-dim");
      n.classList.remove("is-focused-node");
    });
    host.querySelectorAll(".edgePath").forEach(function (e) {
      e.classList.remove("is-on-path");
    });
  }

  function wireSearch() {
    var input = document.getElementById("lineage-search");
    var clear = document.getElementById("lineage-clear");
    if (!input) return;

    input.addEventListener("input", function () {
      var q = input.value.trim().toLowerCase();
      if (!q) {
        clearFocus();
        if (clear) clear.hidden = true;
        return;
      }
      var nodes = Array.from(
        document.querySelectorAll("#mermaid-host .node")
      );
      var match = nodes.find(function (n) {
        return nodeMatchesQuery(n, q);
      });
      if (match) {
        focusNode(match);
        if (clear) clear.hidden = false;
      }
    });

    if (clear) {
      clear.addEventListener("click", function () {
        input.value = "";
        clearFocus();
        clear.hidden = true;
        input.focus();
      });
    }
  }

  function init() {
    render().then(function () {
      wireSearch();
    });
  }

  init();

  // Re-render when the theme toggle flips data-theme on <html>.
  var observer = new MutationObserver(function (mutations) {
    for (var i = 0; i < mutations.length; i++) {
      if (mutations[i].attributeName === "data-theme") {
        render();
        break;
      }
    }
  });
  observer.observe(document.documentElement, {
    attributes: true,
    attributeFilter: ["data-theme"],
  });
})();
