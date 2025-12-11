/**
 * ELTO Dashboard Export - Graphiques
 * Pie charts et bar charts SVG
 */

(function() {
    'use strict';

    const PALETTES = ['#0ea5e9', '#8b5cf6', '#f97316', '#10b981', '#ec4899', '#facc15', '#3b82f6', '#22c55e'];

    // Tooltip global
    let tooltip = null;

    function getTooltip() {
        if (!tooltip) {
            tooltip = document.createElement('div');
            tooltip.className = 'chart-tooltip';
            document.body.appendChild(tooltip);
        }
        return tooltip;
    }

    function showTooltip(content, event) {
        const tip = getTooltip();
        tip.innerHTML = content;
        tip.style.display = 'block';
        tip.style.left = (event.pageX + 12) + 'px';
        tip.style.top = (event.pageY + 12) + 'px';
    }

    function hideTooltip() {
        const tip = getTooltip();
        tip.style.display = 'none';
    }

    /**
     * Extrait les données d'un tableau HTML
     */
    function extractDataFromTable(table) {
        const rows = Array.from(table.querySelectorAll('tbody tr'));
        return rows.map(function(row) {
            const cells = Array.from(row.children);
            const label = cells[0]?.textContent.trim() || '';

            // Trouve la première cellule numérique
            const numericCell = cells.slice(1).find(function(cell) {
                const raw = cell.dataset.sortValue || cell.textContent;
                const cleaned = raw.toString().replace('%', '').replace(',', '.').trim();
                return cleaned !== '' && !isNaN(cleaned);
            });

            const rawValue = numericCell ? (numericCell.dataset.sortValue || numericCell.textContent) : '0';
            const value = parseFloat(rawValue.toString().replace('%', '').replace(',', '.')) || 0;

            return { label: label, value: value };
        }).filter(function(item) {
            return item.label !== '' && item.label.toLowerCase() !== 'total';
        });
    }

    /**
     * Construit un pie chart SVG
     */
    function buildPie(container, data) {
        const total = data.reduce(function(sum, item) { return sum + item.value; }, 0);
        const pieEl = container.querySelector('.pie-chart');
        const legendEl = container.querySelector('.pie-legend');

        if (!pieEl || !legendEl || total <= 0) return;

        let cumulative = 0;
        const size = 180, center = 90, radius = 90;

        const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        svg.setAttribute('width', size);
        svg.setAttribute('height', size);
        svg.setAttribute('viewBox', '0 0 ' + size + ' ' + size);

        data.forEach(function(item, idx) {
            const startAngle = cumulative * 2 * Math.PI - Math.PI / 2;
            const slice = (item.value / total) * 2 * Math.PI;
            cumulative += item.value / total;
            const endAngle = startAngle + slice;

            const x1 = center + radius * Math.cos(startAngle);
            const y1 = center + radius * Math.sin(startAngle);
            const x2 = center + radius * Math.cos(endAngle);
            const y2 = center + radius * Math.sin(endAngle);
            const largeArc = slice > Math.PI ? 1 : 0;

            const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
            path.setAttribute('d', 'M ' + center + ' ' + center + ' L ' + x1 + ' ' + y1 + ' A ' + radius + ' ' + radius + ' 0 ' + largeArc + ' 1 ' + x2 + ' ' + y2 + ' Z');
            path.setAttribute('fill', PALETTES[idx % PALETTES.length]);
            path.style.cursor = 'pointer';

            const percent = ((item.value / total) * 100).toFixed(1);
            path.addEventListener('mouseenter', function(event) {
                path.style.opacity = '0.85';
                showTooltip('<strong>' + item.label + '</strong><br>' + item.value + ' (' + percent + '%)', event);
            });
            path.addEventListener('mousemove', function(event) {
                showTooltip('<strong>' + item.label + '</strong><br>' + item.value + ' (' + percent + '%)', event);
            });
            path.addEventListener('mouseleave', function() {
                path.style.opacity = '1';
                hideTooltip();
            });

            svg.appendChild(path);
        });

        pieEl.innerHTML = '';
        pieEl.appendChild(svg);

        legendEl.innerHTML = '';
        data.forEach(function(item, idx) {
            const percent = ((item.value / total) * 100).toFixed(1);
            const legendItem = document.createElement('div');
            legendItem.className = 'pie-legend-item';
            legendItem.innerHTML = '<span class="pie-legend-swatch" style="background:' + PALETTES[idx % PALETTES.length] + ';"></span><span>' + item.label + ' — ' + percent + '% (' + item.value + ')</span>';
            legendEl.appendChild(legendItem);
        });
    }

    /**
     * Construit un bar chart
     */
    function buildBars(container, data) {
        const chart = container.querySelector('.bar-chart');
        if (!chart || !data.length) return;

        const maxValue = Math.max.apply(Math, data.map(function(item) { return item.value; }));
        const total = data.reduce(function(sum, item) { return sum + item.value; }, 0) || 1;

        chart.innerHTML = '';

        data.forEach(function(item, idx) {
            const percentOfMax = maxValue ? (item.value / maxValue) * 100 : 0;
            const percentOfTotal = ((item.value / total) * 100).toFixed(1);

            const row = document.createElement('div');
            row.className = 'bar-row';

            const label = document.createElement('div');
            label.className = 'bar-label';
            label.textContent = item.label;

            const track = document.createElement('div');
            track.className = 'bar-track';

            const fill = document.createElement('div');
            fill.className = 'bar-fill';
            fill.style.width = percentOfMax + '%';
            fill.style.background = PALETTES[idx % PALETTES.length];
            fill.style.cursor = 'pointer';

            fill.addEventListener('mouseenter', function(event) {
                fill.style.opacity = '0.85';
                showTooltip('<strong>' + item.label + '</strong><br>' + item.value + ' (' + percentOfTotal + '%)', event);
            });
            fill.addEventListener('mousemove', function(event) {
                showTooltip('<strong>' + item.label + '</strong><br>' + item.value + ' (' + percentOfTotal + '%)', event);
            });
            fill.addEventListener('mouseleave', function() {
                fill.style.opacity = '1';
                hideTooltip();
            });

            track.appendChild(fill);

            const value = document.createElement('div');
            value.className = 'bar-value';
            value.textContent = item.value;

            row.appendChild(label);
            row.appendChild(track);
            row.appendChild(value);
            chart.appendChild(row);
        });
    }

    /**
     * Initialise tous les graphiques de la page
     */
    function initCharts() {
        // Pie charts
        document.querySelectorAll('[data-pie-source]').forEach(function(container) {
            const tableId = container.dataset.pieSource;
            const table = document.getElementById(tableId);
            if (!table) return;

            const data = extractDataFromTable(table);
            buildPie(container, data);
        });

        // Bar charts
        document.querySelectorAll('[data-bar-source]').forEach(function(container) {
            const tableId = container.dataset.barSource;
            const table = document.getElementById(tableId);
            if (!table) return;

            const data = extractDataFromTable(table);
            buildBars(container, data);
        });
    }

    // Expose la fonction
    window.initCharts = initCharts;

    // Initialise au chargement
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initCharts);
    } else {
        initCharts();
    }
})();
