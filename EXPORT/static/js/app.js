/**
 * ELTO Dashboard Export - Application principale
 * Gère la navigation par onglets et le chargement des contenus
 */

(function() {
    'use strict';

    // État de l'application
    window.ELTO = window.ELTO || {
        currentTab: 'sessions_general',
        filters: {
            sites: [],
            dateDebut: null,
            dateFin: null,
            errorTypes: [],
            moments: []
        }
    };

    /**
     * Construit l'URL avec les paramètres de filtre
     */
    function buildUrl(base) {
        const params = new URLSearchParams();

        if (ELTO.filters.sites.length > 0) {
            params.set('sites', ELTO.filters.sites.join(','));
        }
        if (ELTO.filters.dateDebut) {
            params.set('date_debut', ELTO.filters.dateDebut);
        }
        if (ELTO.filters.dateFin) {
            params.set('date_fin', ELTO.filters.dateFin);
        }
        if (ELTO.filters.errorTypes.length > 0) {
            params.set('error_types', ELTO.filters.errorTypes.join(','));
        }
        if (ELTO.filters.moments.length > 0) {
            params.set('moments', ELTO.filters.moments.join(','));
        }

        const queryString = params.toString();
        return queryString ? `${base}?${queryString}` : base;
    }

    /**
     * Charge le contenu d'un onglet
     */
    function loadTab(tabName) {
        ELTO.currentTab = tabName;

        // Met à jour les boutons d'onglet
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.tab === tabName);
        });

        // Endpoints par onglet
        const endpoints = {
            'sessions_general': '/api/sessions/general',
            'sessions_site_details': '/api/sessions/site-details',
            'error_analysis': '/api/sessions/error-analysis'
        };

        const endpoint = endpoints[tabName];
        if (!endpoint) return;

        const url = buildUrl(endpoint);
        htmx.ajax('GET', url, {
            target: '#tab-content',
            swap: 'innerHTML'
        });
    }

    /**
     * Rafraîchit l'onglet actuel
     */
    function refreshTab() {
        loadTab(ELTO.currentTab);
    }

    /**
     * Appelé quand les filtres changent
     */
    function onFiltersChange() {
        // Met à jour les filtres depuis l'interface
        updateFiltersFromUI();

        // Recharge les options de filtres (types d'erreur et moments)
        loadFilterOptions();

        // Rafraîchit l'onglet actuel
        refreshTab();
    }

    /**
     * Met à jour les filtres depuis l'interface utilisateur
     */
    function updateFiltersFromUI() {
        // Sites
        const sitesTags = document.querySelectorAll('#sites-tags .tag');
        ELTO.filters.sites = Array.from(sitesTags).map(tag => tag.dataset.value);

        // Dates
        const dateDebut = document.getElementById('date-debut');
        const dateFin = document.getElementById('date-fin');
        ELTO.filters.dateDebut = dateDebut ? dateDebut.value : null;
        ELTO.filters.dateFin = dateFin ? dateFin.value : null;

        // Types d'erreur
        const errorTags = document.querySelectorAll('#error-types-tags .tag');
        ELTO.filters.errorTypes = Array.from(errorTags).map(tag => tag.dataset.value);

        // Moments
        const momentTags = document.querySelectorAll('#moments-tags .tag');
        ELTO.filters.moments = Array.from(momentTags).map(tag => tag.dataset.value);
    }

    /**
     * Charge les options de filtres dynamiques
     */
    async function loadFilterOptions() {
        const params = new URLSearchParams();

        if (ELTO.filters.sites.length > 0) {
            params.set('sites', ELTO.filters.sites.join(','));
        }
        if (ELTO.filters.dateDebut) {
            params.set('date_debut', ELTO.filters.dateDebut);
        }
        if (ELTO.filters.dateFin) {
            params.set('date_fin', ELTO.filters.dateFin);
        }

        try {
            const response = await fetch(`/api/filters/options?${params.toString()}`);
            const data = await response.json();

            // Met à jour les dropdowns de type d'erreur et moment
            updateDropdownOptions('error-types-dropdown', data.error_types, ELTO.filters.errorTypes);
            updateDropdownOptions('moments-dropdown', data.moments, ELTO.filters.moments);
        } catch (error) {
            console.error('Erreur lors du chargement des options de filtres:', error);
        }
    }

    /**
     * Met à jour les options d'un dropdown
     */
    function updateDropdownOptions(dropdownId, options, selectedValues) {
        const dropdown = document.getElementById(dropdownId);
        if (!dropdown) return;

        const optionsContainer = dropdown.querySelector('.multiselect-options');
        if (!optionsContainer) return;

        optionsContainer.innerHTML = '';

        options.forEach(option => {
            const isSelected = selectedValues.includes(option);
            const optionDiv = document.createElement('div');
            optionDiv.className = `multiselect-option ${isSelected ? 'selected' : ''}`;
            optionDiv.dataset.value = option;
            optionDiv.innerHTML = `
                <span class="option-checkbox">${isSelected ? '☑' : '☐'}</span>
                <span>${option}</span>
            `;
            optionsContainer.appendChild(optionDiv);
        });
    }

    /**
     * Charge la liste des sites
     */
    async function loadSites() {
        try {
            const response = await fetch('/api/filters/sites');
            const data = await response.json();

            updateDropdownOptions('sites-dropdown', data.sites, ELTO.filters.sites);
        } catch (error) {
            console.error('Erreur lors du chargement des sites:', error);
        }
    }

    /**
     * Initialise les écouteurs d'événements
     */
    function initEventListeners() {
        // Onglets
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                loadTab(btn.dataset.tab);
            });
        });

        // Dates
        const dateDebut = document.getElementById('date-debut');
        const dateFin = document.getElementById('date-fin');

        if (dateDebut) {
            dateDebut.addEventListener('change', onFiltersChange);
        }
        if (dateFin) {
            dateFin.addEventListener('change', onFiltersChange);
        }

        // Après chaque swap HTMX, réinitialise les graphiques
        document.body.addEventListener('htmx:afterSwap', function(event) {
            if (event.target.id === 'tab-content') {
                // Réinitialise les graphiques et le tri des tables
                if (typeof initCharts === 'function') {
                    initCharts();
                }
                if (typeof initTables === 'function') {
                    initTables();
                }
            }
        });

        // Navigation depuis les lignes de tableau vers détails site
        document.body.addEventListener('click', function(event) {
            const row = event.target.closest('[data-nav-tab]');
            if (row) {
                const tab = row.dataset.navTab;
                const siteFocus = row.dataset.siteFocus;

                if (tab === 'details-site' && siteFocus) {
                    ELTO.currentTab = 'sessions_site_details';
                    loadTabWithSiteFocus('sessions_site_details', siteFocus);
                }
            }
        });
    }

    /**
     * Charge un onglet avec un site focus spécifique
     */
    function loadTabWithSiteFocus(tabName, siteFocus) {
        ELTO.currentTab = tabName;

        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.tab === tabName);
        });

        const endpoint = '/api/sessions/site-details';
        let url = buildUrl(endpoint);

        // Ajoute le site_focus à l'URL
        const separator = url.includes('?') ? '&' : '?';
        url += `${separator}site_focus=${encodeURIComponent(siteFocus)}`;

        htmx.ajax('GET', url, {
            target: '#tab-content',
            swap: 'innerHTML'
        });
    }

    /**
     * Initialisation au chargement de la page
     */
    function init() {
        // Charge les sites
        loadSites();

        // Initialise les dates depuis les valeurs par défaut
        const dateDebut = document.getElementById('date-debut');
        const dateFin = document.getElementById('date-fin');

        if (dateDebut && dateDebut.value) {
            ELTO.filters.dateDebut = dateDebut.value;
        }
        if (dateFin && dateFin.value) {
            ELTO.filters.dateFin = dateFin.value;
        }

        // Charge les options de filtres
        loadFilterOptions();

        // Initialise les écouteurs
        initEventListeners();

        // Charge l'onglet par défaut
        loadTab(ELTO.currentTab);
    }

    // Expose les fonctions nécessaires
    window.ELTO.loadTab = loadTab;
    window.ELTO.refreshTab = refreshTab;
    window.ELTO.onFiltersChange = onFiltersChange;
    window.ELTO.buildUrl = buildUrl;

    // Initialise quand le DOM est prêt
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
