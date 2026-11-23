// Makes the large tables of some pages sortable by clicking a column header:
// the tables of the benchmarks and the overview of the components. Values
// are compared as numbers where they are durations (ns, µs, ms, s), counts
// (k, M, G), percentages or plain numbers, as text otherwise; empty cells,
// "n/a" and "–" always sort last. Configuration tables elsewhere keep their
// order, which is deliberate.

(function () {
  // Pages whose tables can be sorted, by the end of their path.
  const PAGES = ["/operations/benchmarks/", "/components/"];
  // Tables with fewer rows stay as they are.
  const MIN_ROWS = 6;

  const UNITS = {
    ns: 1e-9, "µs": 1e-6, us: 1e-6, ms: 1e-3, s: 1,
    k: 1e3, M: 1e6, G: 1e9,
  };
  const NUMBER = /^[±~≈]?\s*(-?[\d,]*\.?\d+)\s*(ns|µs|us|ms|s|k|M|G|%)?\b/;

  // value returns a number for a cell, a string for text, or null for a cell
  // without a value.
  function value(cell) {
    const text = cell.textContent.replace(/⚠️/g, "").trim();
    if (text === "" || text === "n/a" || text === "–" || text === "-") {
      return null;
    }
    const match = NUMBER.exec(text);
    if (match) {
      const n = parseFloat(match[1].replace(/,/g, ""));
      const unit = match[2];
      return unit && unit !== "%" ? n * UNITS[unit] : n;
    }
    return text.toLowerCase();
  }

  function compare(a, b) {
    if (a === null || b === null) {
      return a === b ? 0 : a === null ? 1 : -1;
    }
    if (typeof a === "number" && typeof b === "number") {
      return a - b;
    }
    return String(a).localeCompare(String(b));
  }

  function sortBy(table, header, column) {
    const ascending = header.getAttribute("aria-sort") !== "ascending";
    for (const th of table.tHead.rows[0].cells) {
      th.removeAttribute("aria-sort");
    }
    header.setAttribute("aria-sort", ascending ? "ascending" : "descending");
    const body = table.tBodies[0];
    const rows = Array.from(body.rows).map((row, index) => ({
      row, index, key: value(row.cells[column]),
    }));
    rows.sort((x, y) => {
      // Cells without a value stay last in both directions.
      if (x.key === null || y.key === null) {
        return compare(x.key, y.key) || x.index - y.index;
      }
      const order = compare(x.key, y.key);
      return (ascending ? order : -order) || x.index - y.index;
    });
    for (const { row } of rows) {
      body.appendChild(row);
    }
  }

  function enable() {
    if (!PAGES.some((page) => location.pathname.endsWith(page))) {
      return;
    }
    for (const table of document.querySelectorAll(".md-typeset table")) {
      if (!table.tHead || !table.tBodies[0] || table.tBodies[0].rows.length < MIN_ROWS
        || table.dataset.sortable) {
        continue;
      }
      table.dataset.sortable = "true";
      Array.from(table.tHead.rows[0].cells).forEach((header, column) => {
        header.classList.add("tram-sortable");
        header.tabIndex = 0;
        header.title = "Sort by this column";
        const sort = () => sortBy(table, header, column);
        header.addEventListener("click", sort);
        header.addEventListener("keydown", (event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            sort();
          }
        });
      });
    }
  }

  // With instant navigation, pages are swapped without reloading scripts;
  // document$ tells when a new page is shown.
  if (typeof document$ !== "undefined") {
    document$.subscribe(enable);
  } else {
    document.addEventListener("DOMContentLoaded", enable);
  }
})();
