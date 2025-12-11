/**
 * ELTO Dashboard Export - Tri des tables
 * Fonctions de tri pour les tableaux HTML
 */

(function() {
    'use strict';

    /**
     * Parse une valeur pour le tri
     */
    function parseValue(text, type) {
        const cleaned = (text || '').toString().replace(/\s+/g, '').replace('%', '');
        if (type === 'number') {
            return parseFloat(cleaned) || 0;
        }
        return cleaned.toLowerCase();
    }

    /**
     * Initialise le tri sur une table
     */
    function initTableSorting(tableId) {
        const table = document.getElementById(tableId);
        if (!table) return;

        const headers = table.querySelectorAll('th.sortable');
        const tbody = table.querySelector('tbody');
        if (!tbody) return;

        let currentSort = { col: null, dir: 'asc' };

        function sortTable(colIndex, type) {
            const rows = Array.from(tbody.querySelectorAll('tr'));
            const dir = currentSort.col === colIndex && currentSort.dir === 'asc' ? 'desc' : 'asc';
            const mult = dir === 'asc' ? 1 : -1;

            // Vérifie si c'est une table groupée (site + PDC)
            const hasGroups = rows.some(function(row) {
                return row.classList.contains('site-row') || row.classList.contains('pdc-row');
            });

            if (hasGroups) {
                // Tri groupé
                const groupedRows = rows.reduce(function(map, row) {
                    const key = row.dataset.siteKey || 'unknown';
                    if (!map[key]) {
                        map[key] = { site: null, pdcs: [] };
                    }
                    if (row.classList.contains('site-row')) {
                        map[key].site = row;
                    } else {
                        map[key].pdcs.push(row);
                    }
                    return map;
                }, {});

                const siteRows = rows.filter(function(row) {
                    return row.classList.contains('site-row');
                });

                siteRows.sort(function(a, b) {
                    const aVal = parseValue(
                        a.children[colIndex].dataset.sortValue || a.children[colIndex].textContent.trim(),
                        type
                    );
                    const bVal = parseValue(
                        b.children[colIndex].dataset.sortValue || b.children[colIndex].textContent.trim(),
                        type
                    );
                    return aVal < bVal ? -mult : aVal > bVal ? mult : 0;
                });

                siteRows.forEach(function(siteRow) {
                    const siteKey = siteRow.dataset.siteKey || 'unknown';
                    const group = groupedRows[siteKey] || { pdcs: [] };
                    tbody.appendChild(siteRow);
                    group.pdcs.forEach(function(pdcRow) {
                        tbody.appendChild(pdcRow);
                    });
                });
            } else {
                // Tri simple
                rows.sort(function(a, b) {
                    const aVal = parseValue(
                        a.children[colIndex].dataset.sortValue || a.children[colIndex].textContent.trim(),
                        type
                    );
                    const bVal = parseValue(
                        b.children[colIndex].dataset.sortValue || b.children[colIndex].textContent.trim(),
                        type
                    );
                    return aVal < bVal ? -mult : aVal > bVal ? mult : 0;
                });

                rows.forEach(function(row) {
                    tbody.appendChild(row);
                });
            }

            // Met à jour les classes de tri
            headers.forEach(function(h) {
                h.classList.remove('sorted-asc', 'sorted-desc');
            });
            headers[colIndex].classList.add('sorted-' + dir);

            currentSort = { col: colIndex, dir: dir };
        }

        headers.forEach(function(th, index) {
            th.addEventListener('click', function() {
                sortTable(index, th.dataset.type || 'text');
            });
        });
    }

    /**
     * Initialise toutes les tables de la page
     */
    function initTables() {
        // Trouve toutes les tables avec des en-têtes sortables
        document.querySelectorAll('table').forEach(function(table) {
            if (table.id && table.querySelector('th.sortable')) {
                initTableSorting(table.id);
            }
        });
    }

    // Expose la fonction
    window.initTables = initTables;

    // Initialise au chargement
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initTables);
    } else {
        initTables();
    }
})();
