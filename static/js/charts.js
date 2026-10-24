// Small-multiple line charts for the dashboard. Each chart is one series
// (title names it, no legend), with a crosshair tooltip that snaps to the
// nearest sample, keyboard stepping, and a table view of the same values.
//
// Data: <script type="application/json" id="..."> {"Charts": [{"Key", "Title",
// "Unit", "Points": [[unixMillis, value], ...]}]}, rendered into the element
// whose data-source names that script.
(function () {
    "use strict";
    const SVG = "http://www.w3.org/2000/svg";
    const HEIGHT = 168; // plot + x-axis band, so axis labels never overflow
    const PAD = { top: 12, right: 56, bottom: 26, left: 48 };

    function el(name, attrs, parent) {
        const node = document.createElementNS(SVG, name);
        for (const [k, v] of Object.entries(attrs || {})) node.setAttribute(k, v);
        if (parent) parent.appendChild(node);
        return node;
    }

    function niceTicks(min, max, count) {
        if (min === max) {
            const pad = Math.abs(min) * 0.1 || 1;
            min -= pad;
            max += pad;
        }
        const raw = (max - min) / count;
        const mag = Math.pow(10, Math.floor(Math.log10(raw)));
        const step = [1, 2, 2.5, 5, 10].map((m) => m * mag).find((s) => s >= raw);
        const lo = Math.floor(min / step) * step;
        const hi = Math.ceil(max / step) * step;
        const ticks = [];
        for (let v = lo; v <= hi + step / 2; v += step) ticks.push(+v.toFixed(10));
        return ticks;
    }

    function formatValue(v) {
        const abs = Math.abs(v);
        if (abs >= 1000) return (v / 1000).toFixed(abs >= 10000 ? 0 : 1) + "K";
        if (abs >= 100 || Number.isInteger(v)) return v.toFixed(0);
        return v.toFixed(1);
    }

    function formatTime(ms, spanMs) {
        const d = new Date(ms);
        const time = d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
        if (spanMs <= 36 * 3600 * 1000) return time;
        return d.toLocaleDateString([], { day: "numeric", month: "short" });
    }

    function formatFull(ms) {
        return new Date(ms).toLocaleString([], { dateStyle: "medium", timeStyle: "short" });
    }

    function render(figure, chart) {
        const plotBox = figure.querySelector(".chart-plot");
        plotBox.textContent = "";
        const width = Math.max(plotBox.clientWidth, 240);
        const pts = chart.Points;
        const svg = el("svg", { width, height: HEIGHT, viewBox: `0 0 ${width} ${HEIGHT}`, role: "img",
            "aria-label": `${chart.Title} over time, ${pts.length} samples` }, plotBox);
        if (pts.length === 0) return;

        const x0 = pts[0][0], x1 = pts[pts.length - 1][0];
        const span = Math.max(x1 - x0, 1);
        const values = pts.map((p) => p[1]);
        const ticks = niceTicks(Math.min(...values), Math.max(...values), 3);
        const y0 = ticks[0], y1 = ticks[ticks.length - 1];
        const plotW = width - PAD.left - PAD.right;
        const plotH = HEIGHT - PAD.top - PAD.bottom;
        const sx = (t) => PAD.left + (pts.length === 1 ? plotW / 2 : ((t - x0) / span) * plotW);
        const sy = (v) => PAD.top + (1 - (v - y0) / (y1 - y0)) * plotH;

        for (const t of ticks) {
            el("line", { class: "chart-grid", x1: PAD.left, x2: PAD.left + plotW, y1: sy(t), y2: sy(t) }, svg);
            el("text", { class: "chart-tick", x: PAD.left - 8, y: sy(t), "text-anchor": "end", "dominant-baseline": "middle" }, svg)
                .textContent = formatValue(t);
        }
        const xTicks = Math.min(5, Math.max(2, Math.floor(plotW / 110)));
        for (let i = 0; i < xTicks; i++) {
            const t = x0 + (span * i) / (xTicks - 1);
            const anchor = i === 0 ? "start" : i === xTicks - 1 ? "end" : "middle";
            el("text", { class: "chart-tick", x: sx(t), y: HEIGHT - 8, "text-anchor": anchor }, svg).textContent = formatTime(t, span);
        }

        const line = pts.map((p, i) => `${i ? "L" : "M"}${sx(p[0]).toFixed(1)},${sy(p[1]).toFixed(1)}`).join("");
        if (pts.length > 1) {
            el("path", { class: "chart-area", d: `${line}L${sx(x1)},${sy(y0)}L${sx(x0)},${sy(y0)}Z` }, svg);
            el("path", { class: "chart-line", d: line }, svg);
        }
        const last = pts[pts.length - 1];
        el("circle", { class: "chart-dot", cx: sx(last[0]), cy: sy(last[1]), r: 4 }, svg);
        el("text", { class: "chart-end-label", x: sx(last[0]) + 9, y: sy(last[1]), "dominant-baseline": "middle" }, svg)
            .textContent = formatValue(last[1]);

        // Hover layer: crosshair snaps to the nearest sample.
        const cross = el("line", { class: "chart-crosshair", y1: PAD.top, y2: PAD.top + plotH, visibility: "hidden" }, svg);
        const marker = el("circle", { class: "chart-dot", r: 4, visibility: "hidden" }, svg);
        const tip = figure.querySelector(".chart-tooltip");
        let current = pts.length - 1;
        function show(i) {
            current = Math.max(0, Math.min(pts.length - 1, i));
            const [t, v] = pts[current];
            for (const n of [cross, marker]) n.setAttribute("visibility", "visible");
            cross.setAttribute("x1", sx(t));
            cross.setAttribute("x2", sx(t));
            marker.setAttribute("cx", sx(t));
            marker.setAttribute("cy", sy(v));
            tip.querySelector("strong").textContent = `${formatValue(v)} ${chart.Unit}`;
            tip.querySelector("span").textContent = formatFull(t);
            tip.hidden = false;
            const left = Math.min(Math.max(sx(t) - tip.offsetWidth / 2, 0), width - tip.offsetWidth);
            tip.style.left = `${left}px`;
        }
        function hide() {
            for (const n of [cross, marker]) n.setAttribute("visibility", "hidden");
            tip.hidden = true;
        }
        function nearest(clientX) {
            const x = clientX - svg.getBoundingClientRect().left;
            const t = x0 + ((x - PAD.left) / plotW) * span;
            let best = 0;
            for (let i = 1; i < pts.length; i++) {
                if (Math.abs(pts[i][0] - t) < Math.abs(pts[best][0] - t)) best = i;
            }
            return best;
        }
        svg.addEventListener("pointermove", (e) => show(nearest(e.clientX)));
        svg.addEventListener("pointerleave", hide);
        figure.onfocus = () => show(current);
        figure.onblur = hide;
        figure.onkeydown = (e) => {
            const step = { ArrowLeft: -1, ArrowRight: 1, Home: -pts.length, End: pts.length }[e.key];
            if (step === undefined) return;
            e.preventDefault();
            show(current + step);
        };
    }

    function tableView(figure, chart) {
        const details = document.createElement("details");
        details.className = "chart-table";
        const summary = document.createElement("summary");
        summary.textContent = "Show as table";
        details.appendChild(summary);
        const table = document.createElement("table");
        table.className = "data-table";
        const head = table.createTHead().insertRow();
        for (const label of ["Time", `${chart.Title} (${chart.Unit})`]) {
            const th = document.createElement("th");
            th.textContent = label;
            head.appendChild(th);
        }
        const body = table.createTBody();
        for (const [t, v] of [...chart.Points].reverse()) {
            const row = body.insertRow();
            row.insertCell().textContent = formatFull(t);
            row.insertCell().textContent = formatValue(v);
        }
        details.appendChild(table);
        figure.appendChild(details);
    }

    function mount(container) {
        const source = document.getElementById(container.dataset.source);
        if (!source) return;
        const { Charts } = JSON.parse(source.textContent);
        for (const chart of Charts) {
            const figure = document.createElement("figure");
            figure.className = "chart card";
            figure.tabIndex = 0;
            const caption = document.createElement("figcaption");
            const title = document.createElement("span");
            title.className = "chart-title";
            title.textContent = chart.Title;
            const unit = document.createElement("span");
            unit.className = "chart-unit";
            unit.textContent = chart.Unit;
            caption.append(title, unit);
            const plot = document.createElement("div");
            plot.className = "chart-plot";
            const tip = document.createElement("div");
            tip.className = "chart-tooltip";
            tip.hidden = true;
            tip.setAttribute("role", "status");
            tip.append(document.createElement("strong"), document.createElement("span"));
            figure.append(caption, plot, tip);
            container.appendChild(figure);
            if (chart.Points.length === 0) {
                plot.textContent = "No values in this range.";
                plot.classList.add("empty-state");
                continue;
            }
            render(figure, chart);
            tableView(figure, chart);
            new ResizeObserver(() => render(figure, chart)).observe(plot);
        }
    }

    document.querySelectorAll("[data-source]").forEach(mount);
})();
