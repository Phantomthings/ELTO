/**
 * ELTO Dashboard - JavaScript Application
 * Module: Filtres + Generale + Details Site + Analyse Erreur (Top 3)
 */

// ==========================================================================
// State Management
// ==========================================================================
const state = {
    tab: 'general',
    dateMode: 'focus_mois',
    dateDebut: null,
    dateFin: null,
    allSites: [],
    sites: [],
    errorTypes: [],
    selectedErrorTypes: [],
    moments: [],
    selectedMoments: [],
    siteFocus: '',
    siteDetailsPdcs: [],
};

const MOMENT_GROUPS = {
    avant: ['Init', 'Lock Connector', 'CableCheck'],
    charge: ['Charge'],
    fin: ['Fin de charge'],
};

const endpoints = {
    'general': '/api/sessions/general',
    'details-site': '/api/sessions/site-details',
    'analyse-erreur': '/api/sessions/error-analysis',
};

// ==========================================================================
// Month Configuration
// ==========================================================================
const months = ['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Juin', 'Juil', 'Août', 'Sept', 'Oct', 'Nov', 'Déc'];

// ==========================================================================
// Initialization
// ==========================================================================
function initMonthRadios() {
    const container = document.getElementById('month-radios');
    if (!container) return;

    const currentMonth = new Date().getMonth() + 1;
    container.innerHTML = months.map((m, i) =>
        `<label class="month-radio"><input type="radio" name="month" value="${i + 1}" ${i + 1 === currentMonth ? 'checked' : ''} onchange="updateDates()"> ${m}</label>`
    ).join('');
}

// ==========================================================================
// Sites Management
// ==========================================================================
async function loadSites() {
    try {
        const res = await fetch('/api/filters/sites');
        const data = await res.json();
        state.allSites = data.sites || [];
        state.sites = [...state.allSites];
        if (!state.siteFocus && state.allSites.length) {
            state.siteFocus = state.allSites[0];
        }
        renderSites();
    } catch (error) {
        console.error('Error loading sites:', error);
    }
}

function renderSites() {
    const container = document.getElementById('sites-tags');
    if (!container) return;

    container.innerHTML = state.sites.map(s =>
        `<span class="tag">${s} <button class="tag-remove" onclick="removeSite('${s}')">×</button></span>`
    ).join('');

    const btnAllSites = document.getElementById('btn-all-sites');
    if (btnAllSites) {
        btnAllSites.textContent = state.sites.length === state.allSites.length ? '☑ Tous les sites' : '☐ Tous les sites';
    }

    const sumSites = document.getElementById('sum-sites');
    if (sumSites) {
        sumSites.textContent = state.sites.length;
    }
}

function toggleAllSites() {
    state.sites = state.sites.length === state.allSites.length ? [] : [...state.allSites];
    renderSites();
    onFiltersChange();
}

function removeSite(site) {
    state.sites = state.sites.filter(s => s !== site);
    renderSites();
    onFiltersChange();
}

// ==========================================================================
// Filter Options
// ==========================================================================
async function loadFilterOptions() {
    const params = new URLSearchParams();
    if (state.sites.length < state.allSites.length) params.set('sites', state.sites.join(','));
    if (state.dateDebut) params.set('date_debut', state.dateDebut);
    if (state.dateFin) params.set('date_fin', state.dateFin);

    try {
        const res = await fetch(`/api/filters/options?${params}`);
        const data = await res.json();

        state.errorTypes = data.error_types || [];
        state.moments = data.moments || [];

        if (!state.selectedErrorTypes.length) state.selectedErrorTypes = [...state.errorTypes];
        else state.selectedErrorTypes = state.selectedErrorTypes.filter(t => state.errorTypes.includes(t));

        if (!state.selectedMoments.length) state.selectedMoments = [...state.moments];
        else state.selectedMoments = state.selectedMoments.filter(m => state.moments.includes(m));

        renderErrorTypes();
        renderMoments();
        updateToggles();
    } catch (error) {
        console.error('Error loading filter options:', error);
    }
}

function renderErrorTypes() {
    const container = document.getElementById('error-type-tags');
    if (!container) return;

    container.innerHTML = state.errorTypes.map(t => {
        const isSelected = state.selectedErrorTypes.includes(t);
        const label = t.length > 18 ? t.slice(0, 18) + '...' : t;
        return `<span class="tag ${isSelected ? '' : 'inactive'}" onclick="toggleErrorType('${t}')">${label} <button class="tag-remove" onclick="event.stopPropagation(); toggleErrorType('${t}')">${isSelected ? '×' : '+'}</button></span>`;
    }).join('');
}

function toggleErrorType(type) {
    if (state.selectedErrorTypes.includes(type)) {
        state.selectedErrorTypes = state.selectedErrorTypes.filter(t => t !== type);
    } else {
        state.selectedErrorTypes = [...state.selectedErrorTypes, type];
    }
    renderErrorTypes();
    refreshTab();
}

function renderMoments() {
    const container = document.getElementById('moment-tags');
    if (!container) return;

    container.innerHTML = state.moments.map(m => {
        const isSelected = state.selectedMoments.includes(m);
        return `<span class="tag ${isSelected ? '' : 'inactive'}" onclick="toggleMoment('${m}')">${m} <button class="tag-remove" onclick="event.stopPropagation(); toggleMoment('${m}')">${isSelected ? '×' : '+'}</button></span>`;
    }).join('');
}

function toggleMoment(moment) {
    if (state.selectedMoments.includes(moment)) {
        state.selectedMoments = state.selectedMoments.filter(m => m !== moment);
    } else {
        state.selectedMoments = [...state.selectedMoments, moment];
    }
    renderMoments();
    updateToggles();
    refreshTab();
}

function onToggle() {
    let newMoments = [];
    if (document.getElementById('toggle-avant')?.checked)
        newMoments.push(...MOMENT_GROUPS.avant.filter(m => state.moments.includes(m)));
    if (document.getElementById('toggle-charge')?.checked)
        newMoments.push(...MOMENT_GROUPS.charge.filter(m => state.moments.includes(m)));
    if (document.getElementById('toggle-fin')?.checked)
        newMoments.push(...MOMENT_GROUPS.fin.filter(m => state.moments.includes(m)));
    if (state.moments.includes('Unknown')) newMoments.push('Unknown');

    state.selectedMoments = newMoments;
    renderMoments();
    refreshTab();
}

function updateToggles() {
    const hasAvant = MOMENT_GROUPS.avant.every(m => !state.moments.includes(m) || state.selectedMoments.includes(m));
    const hasCharge = !state.moments.includes('Charge') || state.selectedMoments.includes('Charge');
    const hasFin = !state.moments.includes('Fin de charge') || state.selectedMoments.includes('Fin de charge');

    const toggleAvant = document.getElementById('toggle-avant');
    const toggleCharge = document.getElementById('toggle-charge');
    const toggleFin = document.getElementById('toggle-fin');

    if (toggleAvant) toggleAvant.checked = hasAvant;
    if (toggleCharge) toggleCharge.checked = hasCharge;
    if (toggleFin) toggleFin.checked = hasFin;
}

// ==========================================================================
// Date Management
// ==========================================================================
function setDateMode(mode) {
    state.dateMode = mode;
    document.querySelectorAll('.period-btns .btn').forEach(b =>
        b.classList.toggle('active', b.dataset.mode === mode));

    const monthSelector = document.getElementById('month-selector');
    const daySelector = document.getElementById('day-selector');

    if (monthSelector) monthSelector.style.display = mode === 'focus_mois' ? 'flex' : 'none';
    if (daySelector) daySelector.style.display = mode === 'focus_jour' ? 'block' : 'none';

    updateDates();
}

function updateDates() {
    const today = new Date();
    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);

    switch (state.dateMode) {
        case 'focus_jour':
            const day = document.getElementById('day-input')?.value || fmt(today);
            state.dateDebut = state.dateFin = day;
            break;
        case 'focus_mois':
            const year = document.getElementById('year-select')?.value || today.getFullYear();
            const month = document.querySelector('input[name="month"]:checked')?.value || today.getMonth() + 1;
            const first = new Date(year, month - 1, 1);
            const last = new Date(year, month, 0);
            state.dateDebut = fmt(first);
            state.dateFin = fmt(last);
            break;
        case 'j_minus_1':
            state.dateDebut = state.dateFin = fmt(yesterday);
            break;
        case 'semaine_minus_1':
            const week = new Date(yesterday);
            week.setDate(week.getDate() - 6);
            state.dateDebut = fmt(week);
            state.dateFin = fmt(yesterday);
            break;
        case 'toute_periode':
            state.dateDebut = window.dateMin || '2020-01-01';
            state.dateFin = window.dateMax || fmt(today);
            break;
    }
    updateSummary();
    onFiltersChange();
}

function fmt(d) {
    const year = d.getFullYear();
    const month = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
}

function updateSummary() {
    const monthNames = [
        'Janvier', 'Février', 'Mars', 'Avril', 'Mai', 'Juin',
        'Juillet', 'Août', 'Septembre', 'Octobre', 'Novembre', 'Décembre'
    ];
    const year = document.getElementById('year-select')?.value || new Date().getFullYear();
    const month = document.querySelector('input[name="month"]:checked')?.value || new Date().getMonth() + 1;

    let text = '';
    switch (state.dateMode) {
        case 'focus_jour': text = `📅 Focus Jour : ${state.dateDebut}`; break;
        case 'focus_mois': text = `📅 Mois complet : ${monthNames[month - 1]} ${year}`; break;
        case 'j_minus_1': text = `📅 J-1 (hier) : ${state.dateDebut}`; break;
        case 'semaine_minus_1': text = `📅 Semaine -1 : ${state.dateDebut} → ${state.dateFin}`; break;
        case 'toute_periode': text = `📅 Toute la période : ${state.dateDebut} → ${state.dateFin}`; break;
    }

    const periodSummary = document.getElementById('period-summary');
    const sumDates = document.getElementById('sum-dates');

    if (periodSummary) periodSummary.textContent = text;
    if (sumDates) sumDates.textContent = `${state.dateDebut} → ${state.dateFin}`;
}

// ==========================================================================
// Filters Change Handler
// ==========================================================================
async function onFiltersChange() {
    await loadFilterOptions();
    refreshTab();
}

// ==========================================================================
// Tabs Management
// ==========================================================================
function buildUrl(base) {
    const p = new URLSearchParams();
    if (state.sites.length && state.sites.length < state.allSites.length) p.set('sites', state.sites.join(','));
    if (state.tab === 'details-site' && state.siteFocus) {
        p.set('site_focus', state.siteFocus);
        if (state.siteDetailsPdcs.length) {
            p.set('pdc', state.siteDetailsPdcs.join(','));
        }
    }
    if (state.dateDebut) p.set('date_debut', state.dateDebut);
    if (state.dateFin) p.set('date_fin', state.dateFin);
    if (state.selectedErrorTypes.length < state.errorTypes.length) p.set('error_types', state.selectedErrorTypes.join(','));
    if (state.selectedMoments.length < state.moments.length) p.set('moments', state.selectedMoments.join(','));
    return p.toString() ? `${base}?${p}` : base;
}

function captureTabState() {
    if (state.tab === 'details-site') {
        const pdcCheckboxes = Array.from(document.querySelectorAll('input[name="pdc-option"]:checked'));
        state.siteDetailsPdcs = pdcCheckboxes.map(el => el.value);
        const siteSelect = document.getElementById('site-focus');
        if (siteSelect && siteSelect.value) {
            state.siteFocus = siteSelect.value;
        }
    }
}

function restoreTabState() {
    if (state.tab === 'details-site' && state.siteDetailsPdcs.length) {
        const pdcCheckboxes = Array.from(document.querySelectorAll('input[name="pdc-option"]'));
        if (pdcCheckboxes.length) {
            const pdcSet = new Set(state.siteDetailsPdcs);
            pdcCheckboxes.forEach(cb => { cb.checked = pdcSet.has(cb.value); });
            const toggle = document.getElementById('pdc-toggle');
            if (toggle) {
                const allChecked = pdcCheckboxes.every(cb => cb.checked);
                toggle.textContent = allChecked ? '☑ Tous' : '☐ Tous';
            }
        }
    }
}

function refreshTab() {
    captureTabState();
    const tabContent = document.getElementById('tab-content');
    if (tabContent) {
        tabContent.innerHTML = `<div class="loading"><span class="spinner"></span> Chargement...</div>`;
    }
    const url = buildUrl(endpoints[state.tab]);
    htmx.ajax('GET', url, { target: '#tab-content', swap: 'innerHTML' });
}

function initTabs() {
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            state.tab = btn.dataset.tab;
            refreshTab();
        });
    });
}

// ==========================================================================
// Event Listeners
// ==========================================================================
function initEventListeners() {
    document.addEventListener('click', (event) => {
        const navTarget = event.target.closest('[data-nav-tab]');
        if (navTarget) {
            const tab = navTarget.dataset.navTab;
            if (navTarget.dataset.siteFocus) {
                state.siteFocus = navTarget.dataset.siteFocus;
            }
            const tabBtn = document.querySelector(`.tab-btn[data-tab="${tab}"]`);
            if (tabBtn) {
                document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
                tabBtn.classList.add('active');
                state.tab = tab;
                refreshTab();
            }
        }
    });

    document.addEventListener('change', (event) => {
        if (event.target.id === 'site-focus') {
            state.siteFocus = event.target.value;
        }
        if (event.target.name === 'pdc-option') {
            const checked = Array.from(document.querySelectorAll('input[name="pdc-option"]:checked')).map(el => el.value);
            state.siteDetailsPdcs = checked;
        }
    });

    document.body.addEventListener('htmx:afterSwap', (event) => {
        if (event.target && event.target.id === 'tab-content') {
            restoreTabState();
        }
    });
}

// ==========================================================================
// Time Update
// ==========================================================================
function updateTime() {
    const timeEl = document.getElementById('update-time');
    if (timeEl) {
        timeEl.textContent = new Date().toLocaleTimeString('fr-FR', { hour: '2-digit', minute: '2-digit' });
    }
}

// ==========================================================================
// Table Sorting Utility
// ==========================================================================
function initTableSorting(tableId) {
    const table = document.getElementById(tableId);
    if (!table) return;

    const headers = table.querySelectorAll('th.sortable');
    const tbody = table.querySelector('tbody');
    if (!tbody) return;

    let currentSort = { col: null, dir: 'asc' };

    function parseValue(text, type) {
        const cleaned = (text || '').toString().replace(/\s+/g, '').replace('%', '');
        if (type === 'number') return parseFloat(cleaned) || 0;
        return cleaned.toLowerCase();
    }

    function sortTable(colIndex, type) {
        const rows = Array.from(tbody.querySelectorAll('tr'));
        const dir = currentSort.col === colIndex && currentSort.dir === 'asc' ? 'desc' : 'asc';
        const mult = dir === 'asc' ? 1 : -1;

        rows.sort((a, b) => {
            const aVal = parseValue(a.children[colIndex]?.dataset.sortValue || a.children[colIndex]?.textContent.trim(), type);
            const bVal = parseValue(b.children[colIndex]?.dataset.sortValue || b.children[colIndex]?.textContent.trim(), type);
            return aVal < bVal ? -mult : aVal > bVal ? mult : 0;
        });

        rows.forEach(row => tbody.appendChild(row));
        headers.forEach(h => h.classList.remove('sorted-asc', 'sorted-desc'));
        headers[colIndex].classList.add('sorted-' + dir);
        currentSort = { col: colIndex, dir: dir };
    }

    headers.forEach((th, index) => {
        th.addEventListener('click', () => {
            sortTable(index, th.dataset.type || 'text');
        });
    });
}

// ==========================================================================
// Pie Chart Builder
// ==========================================================================
const palettes = ['#0ea5e9', '#8b5cf6', '#f97316', '#10b981', '#ec4899', '#facc15', '#3b82f6', '#22c55e'];

function buildPie(container, data) {
    const total = data.reduce((sum, item) => sum + item.value, 0);
    const pieEl = container.querySelector('.pie-chart');
    const legendEl = container.querySelector('.pie-legend');
    if (!pieEl || !legendEl || total <= 0) return;

    let cumulative = 0;
    const size = 180, center = 90, radius = 90;
    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('width', size);
    svg.setAttribute('height', size);
    svg.setAttribute('viewBox', `0 0 ${size} ${size}`);

    data.forEach((item, idx) => {
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
        path.setAttribute('d', `M ${center} ${center} L ${x1} ${y1} A ${radius} ${radius} 0 ${largeArc} 1 ${x2} ${y2} Z`);
        path.setAttribute('fill', palettes[idx % palettes.length]);
        path.style.cursor = 'pointer';

        svg.appendChild(path);
    });

    pieEl.innerHTML = '';
    pieEl.appendChild(svg);

    legendEl.innerHTML = '';
    data.forEach((item, idx) => {
        const percent = ((item.value / total) * 100).toFixed(1);
        const legendItem = document.createElement('div');
        legendItem.className = 'pie-legend-item';
        legendItem.innerHTML = `<span class="pie-legend-swatch" style="background:${palettes[idx % palettes.length]};"></span><span>${item.label} — ${percent}% (${item.value})</span>`;
        legendEl.appendChild(legendItem);
    });
}

// ==========================================================================
// Bar Chart Builder
// ==========================================================================
function buildBars(container, data) {
    const chart = container.querySelector('.bar-chart');
    if (!chart || !data.length) return;

    const maxValue = Math.max(...data.map(item => item.value));
    const total = data.reduce((sum, item) => sum + item.value, 0) || 1;
    chart.innerHTML = '';

    data.forEach((item, idx) => {
        const percentOfMax = maxValue ? (item.value / maxValue) * 100 : 0;
        const row = document.createElement('div');
        row.className = 'bar-row';
        row.style.cssText = 'display: grid; grid-template-columns: 140px 1fr 70px; align-items: center; gap: 0.5rem; font-size: 0.9rem;';

        const label = document.createElement('div');
        label.className = 'bar-label';
        label.textContent = item.label;

        const track = document.createElement('div');
        track.className = 'bar-track';

        const fill = document.createElement('div');
        fill.className = 'bar-fill';
        fill.style.width = percentOfMax + '%';
        fill.style.background = palettes[idx % palettes.length];

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

// ==========================================================================
// Extract Data from Table
// ==========================================================================
function extractDataFromTable(table) {
    const rows = Array.from(table.querySelectorAll('tbody tr'));
    return rows.map(row => {
        const cells = Array.from(row.children);
        const label = cells[0]?.textContent.trim() || '';
        const numericCell = cells.slice(1).find(cell => {
            const raw = cell.dataset.sortValue || cell.textContent;
            const cleaned = raw.toString().replace('%', '').replace(',', '.').trim();
            return cleaned !== '' && !isNaN(cleaned);
        });
        const rawValue = numericCell ? (numericCell.dataset.sortValue || numericCell.textContent) : '0';
        const value = parseFloat(rawValue.toString().replace('%', '').replace(',', '.')) || 0;
        return { label, value };
    }).filter(item => item.label !== '' && item.label.toLowerCase() !== 'total');
}

// ==========================================================================
// Main Initialization
// ==========================================================================
async function init() {
    // Set default year
    const yearSelect = document.getElementById('year-select');
    if (yearSelect) yearSelect.value = new Date().getFullYear();

    // Set default day
    const dayInput = document.getElementById('day-input');
    if (dayInput) dayInput.value = fmt(new Date());

    // Initialize components
    initMonthRadios();
    initTabs();
    initEventListeners();

    // Update dates and load data
    updateDates();
    await loadSites();
    await loadFilterOptions();
    refreshTab();

    // Start time update
    updateTime();
    setInterval(updateTime, 60000);
}

// Start the application when DOM is ready
document.addEventListener('DOMContentLoaded', init);

// Expose functions globally for inline handlers
window.toggleAllSites = toggleAllSites;
window.removeSite = removeSite;
window.setDateMode = setDateMode;
window.updateDates = updateDates;
window.toggleErrorType = toggleErrorType;
window.toggleMoment = toggleMoment;
window.onToggle = onToggle;
window.initTableSorting = initTableSorting;
window.buildPie = buildPie;
window.buildBars = buildBars;
window.extractDataFromTable = extractDataFromTable;
