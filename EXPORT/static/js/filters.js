/**
 * ELTO Dashboard Export - Gestion des filtres
 * Multiselect pour Sites, Types d'erreur, Moments
 */

(function() {
    'use strict';

    /**
     * Initialise un composant multiselect
     */
    function initMultiselect(containerId, tagsContainerId, tagClass, onChange) {
        const container = document.getElementById(containerId);
        if (!container) return;

        const tagsEl = document.getElementById(tagsContainerId);
        const dropdown = container.querySelector('.multiselect-dropdown');
        const searchInput = container.querySelector('.multiselect-search');

        // Toggle dropdown
        tagsEl.addEventListener('click', function(e) {
            if (e.target.classList.contains('tag-remove')) return;
            dropdown.classList.toggle('open');
            if (dropdown.classList.contains('open') && searchInput) {
                searchInput.focus();
            }
        });

        // Ferme le dropdown quand on clique ailleurs
        document.addEventListener('click', function(e) {
            if (!container.contains(e.target)) {
                dropdown.classList.remove('open');
            }
        });

        // Recherche
        if (searchInput) {
            searchInput.addEventListener('input', function() {
                const search = this.value.toLowerCase();
                container.querySelectorAll('.multiselect-option').forEach(opt => {
                    const text = opt.textContent.toLowerCase();
                    opt.style.display = text.includes(search) ? '' : 'none';
                });
            });
        }

        // Sélection d'option
        container.addEventListener('click', function(e) {
            const option = e.target.closest('.multiselect-option');
            if (!option) return;

            option.classList.toggle('selected');
            const checkbox = option.querySelector('.option-checkbox');
            checkbox.textContent = option.classList.contains('selected') ? '☑' : '☐';

            updateTags(container, tagsEl, tagClass);
            if (onChange) onChange();
        });

        // Suppression de tag
        tagsEl.addEventListener('click', function(e) {
            if (e.target.classList.contains('tag-remove')) {
                const tag = e.target.closest('.tag');
                const value = tag.dataset.value;

                // Désélectionne l'option correspondante
                const option = container.querySelector(`.multiselect-option[data-value="${value}"]`);
                if (option) {
                    option.classList.remove('selected');
                    const checkbox = option.querySelector('.option-checkbox');
                    checkbox.textContent = '☐';
                }

                tag.remove();
                if (onChange) onChange();
            }
        });
    }

    /**
     * Met à jour les tags affichés
     */
    function updateTags(container, tagsEl, tagClass) {
        const selected = container.querySelectorAll('.multiselect-option.selected');
        tagsEl.innerHTML = '';

        selected.forEach(opt => {
            const value = opt.dataset.value;
            const tag = document.createElement('span');
            tag.className = `tag ${tagClass}`;
            tag.dataset.value = value;
            tag.innerHTML = `${value} <button class="tag-remove" type="button">&times;</button>`;
            tagsEl.appendChild(tag);
        });
    }

    /**
     * Bouton "Tous les sites"
     */
    function initAllSitesButton() {
        const btn = document.getElementById('btn-all-sites');
        if (!btn) return;

        btn.addEventListener('click', function() {
            const container = document.getElementById('sites-container');
            const tagsEl = document.getElementById('sites-tags');
            const options = container.querySelectorAll('.multiselect-option');
            const allSelected = Array.from(options).every(opt => opt.classList.contains('selected'));

            options.forEach(opt => {
                if (allSelected) {
                    opt.classList.remove('selected');
                    opt.querySelector('.option-checkbox').textContent = '☐';
                } else {
                    opt.classList.add('selected');
                    opt.querySelector('.option-checkbox').textContent = '☑';
                }
            });

            updateTags(container, tagsEl, 'tag-site');

            // Met à jour l'icône du bouton
            const icon = btn.querySelector('.checkbox-icon');
            if (icon) {
                icon.textContent = allSelected ? '☐' : '☑';
            }

            if (window.ELTO && window.ELTO.onFiltersChange) {
                window.ELTO.onFiltersChange();
            }
        });
    }

    /**
     * Synchronise les tags avec les options sélectionnées
     */
    function syncTagsWithOptions(containerId, tagsContainerId, tagClass) {
        const container = document.getElementById(containerId);
        const tagsEl = document.getElementById(tagsContainerId);
        if (!container || !tagsEl) return;

        const currentTags = Array.from(tagsEl.querySelectorAll('.tag')).map(t => t.dataset.value);

        container.querySelectorAll('.multiselect-option').forEach(opt => {
            const value = opt.dataset.value;
            if (currentTags.includes(value)) {
                opt.classList.add('selected');
                opt.querySelector('.option-checkbox').textContent = '☑';
            }
        });
    }

    /**
     * Initialisation
     */
    function init() {
        // Sites
        initMultiselect(
            'sites-container',
            'sites-tags',
            'tag-site',
            function() {
                if (window.ELTO && window.ELTO.onFiltersChange) {
                    window.ELTO.onFiltersChange();
                }
            }
        );

        // Types d'erreur
        initMultiselect(
            'error-types-container',
            'error-types-tags',
            'tag-error',
            function() {
                if (window.ELTO && window.ELTO.onFiltersChange) {
                    window.ELTO.onFiltersChange();
                }
            }
        );

        // Moments
        initMultiselect(
            'moments-container',
            'moments-tags',
            'tag-moment',
            function() {
                if (window.ELTO && window.ELTO.onFiltersChange) {
                    window.ELTO.onFiltersChange();
                }
            }
        );

        // Bouton tous les sites
        initAllSitesButton();
    }

    // Initialise quand le DOM est prêt
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
