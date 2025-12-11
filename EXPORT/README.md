# ELTO Dashboard - Module Export

Module autonome extrait de l'application ELTO Dashboard.

## Fonctionnalités incluses

- **Filtres** : Barre globale + filtres site/date/type d'erreur/moment
- **Onglet Générale** : Vue d'ensemble des charges par site
- **Onglet Détails Site** : Analyse détaillée par PDC
- **Onglet Analyse Erreur** : Top 3 erreurs (EVI + Downstream) avec détail par site

## Structure du projet

```
EXPORT/
├── main.py                 # Point d'entrée FastAPI
├── db.py                   # Connexion MySQL
├── requirements.txt        # Dépendances Python
├── static/
│   ├── styles.css         # Styles CSS séparés
│   └── app.js             # JavaScript séparé
├── templates/
│   ├── index.html         # Page principale
│   └── partials/
│       ├── sessions_general.html
│       ├── sessions_site_details.html
│       └── error_analysis.html
├── routers/
│   ├── __init__.py
│   ├── filters.py         # API filtres
│   └── sessions.py        # API sessions
└── assets/
    ├── elto.png
    └── nidec.png
```

## Installation

```bash
# Créer un environnement virtuel
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate     # Windows

# Installer les dépendances
pip install -r requirements.txt
```

## Configuration

Modifier les variables d'environnement ou directement dans `db.py`:

```python
DB_HOST=141.94.31.144
DB_PORT=3306
DB_USER=AdminNidec
DB_PASSWORD=u6Ehe987XBSXxa4
DB_NAME=indicator
```

## Lancement

```bash
# Lancement en développement
python main.py

# ou avec uvicorn directement
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

L'application sera accessible sur : http://localhost:8000

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /` | Page principale du dashboard |
| `GET /api/filters/sites` | Liste des sites |
| `GET /api/filters/options` | Options de filtrage |
| `GET /api/sessions/general` | Données onglet Générale |
| `GET /api/sessions/site-details` | Données onglet Détails Site |
| `GET /api/sessions/error-analysis` | Données onglet Analyse Erreur |
| `GET /health` | Vérification de santé |

## Intégration Laravel

Pour intégrer ce module dans une application Laravel PHP:

1. **Déployer l'API** : Lancer le backend Python sur un serveur séparé
2. **Intégrer le frontend** : Inclure les fichiers `styles.css` et `app.js`
3. **Adapter les appels API** : Modifier les URLs dans `app.js` pour pointer vers votre backend

### Exemple d'intégration côté Laravel

```html
<!-- Dans votre blade template -->
<link rel="stylesheet" href="{{ asset('css/elto-styles.css') }}">

<div id="elto-dashboard">
    <!-- Le contenu sera chargé dynamiquement -->
</div>

<script>
    // Configurer l'URL de base de l'API
    window.ELTO_API_BASE = 'https://votre-api-elto.com';
</script>
<script src="{{ asset('js/elto-app.js') }}"></script>
```

## Notes

- **Sans authentification** : Ce module n'inclut pas de système de login
- **Frontend séparé** : HTML, CSS et JS sont dans des fichiers distincts
- **API JSON** : Tous les endpoints retournent des fragments HTML (HTMX)
